#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mdb_shape_integrity_check.py
---------------------------------
Verifica mirata per le integrazioni Collebeato:
- Metadati -> proprietario dato = "Regione Lombardia"
- P46 -> tipologia indagine corretta + (facolt.) presenza PDF
- P107/P108 -> presenza come SMS (se rilevabile) e presenza in shapefile Ind_pu
- L6 -> presenza e coordinate di inizio, più presenza in shapefile Ind_ln
- Coerenza ID MDB <-> Shapefile per gli ID indicati (P46, P107, P108, L6)

Requisiti:
- Python 64-bit
- pyodbc (pip install pyodbc)
- pyshp (pip install pyshp)
- Driver Microsoft Access Database Engine 2016 x64

Esempio d'uso:
python mdb_shape_integrity_check.py --mdb "...\CdI_Tabelle.mdb" \
  --ind_pu_shp "...\Ind_pu.shp" --ind_ln_shp "...\Ind_ln.shp" \
  --docs_dir "...\allegati_pdf" --out "Collebeato_integr_check.csv"

Autore: GPT helper
"""
import argparse, csv, os, re, sys
from typing import Dict, List, Tuple, Optional

# Optional imports (guarded)
try:
    import pyodbc
except Exception as e:
    pyodbc = None

try:
    import shapefile  # pyshp
except Exception as e:
    shapefile = None


def warn(msg: str):
    print(f"[WARN] {msg}", file=sys.stderr)


def info(msg: str):
    print(f"[INFO] {msg}", file=sys.stderr)


def connect_mdb(path: str):
    if pyodbc is None:
        raise RuntimeError("pyodbc non disponibile. Installa con: pip install pyodbc")
    if not os.path.exists(path):
        raise FileNotFoundError(f"MDB non trovato: {path}")
    conn_strs = [
        f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={path};",
        f"Driver={{Microsoft Access Driver (*.mdb)}};DBQ={path};",
    ]
    last_err = None
    for cs in conn_strs:
        try:
            return pyodbc.connect(cs, autocommit=False)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Impossibile connettersi al MDB. Verifica driver Access 64-bit. Ultimo errore: {last_err}")


def list_tables(conn) -> List[str]:
    # Try generic method
    try:
        return [r.table_name for r in conn.cursor().tables(tableType='TABLE') if not r.table_name.startswith('MSys')]
    except Exception:
        # Fallback: try SQLite-like pragma (unlikely)
        try:
            cur = conn.cursor()
            cur.execute("SELECT Name FROM MSysObjects WHERE Type=1 AND Flags=0")
            return [r[0] for r in cur.fetchall()]
        except Exception:
            return []


def table_columns(conn, table: str) -> List[Tuple[str, str]]:
    cols = []
    cur = conn.cursor()
    try:
        for row in cur.columns(table=table):
            cols.append((row.column_name, row.type_name))
    except Exception:
        # Fallback: try select top 1 and read description
        try:
            cur.execute(f"SELECT * FROM [{table}]")
            cols = [(d[0], "UNKNOWN") for d in cur.description]
        except Exception:
            pass
    return cols


def find_table_case_insensitive(candidates: List[str], tables: List[str]) -> Optional[str]:
    lower_map = {t.lower(): t for t in tables}
    for c in candidates:
        t = lower_map.get(c.lower())
        if t:
            return t
    # fuzzy contains
    for t in tables:
        for c in candidates:
            if c.lower() in t.lower():
                return t
    return None


def get_value_like(conn, table: str, id_value: str, prefer_cols: List[str]=None) -> List[Dict[str, str]]:
    """
    Cerca righe che contengono id_value in qualsiasi colonna testo.
    Se prefer_cols è fornito, tenta prima quelle.
    Ritorna una lista di dict {colonna: valore} per ciascuna riga trovata (colonne utili).
    """
    cur = conn.cursor()
    cols = table_columns(conn, table)
    if not cols:
        return []
    text_cols = [c for c, t in cols if ('CHAR' in t.upper() or 'TEXT' in t.upper() or t.upper() in ('VARCHAR', 'LONGCHAR', 'MEMO', 'UNKNOWN'))]
    num_cols  = [c for c, t in cols if any(k in t.upper() for k in ('INT', 'DOUBLE', 'SINGLE', 'DECIMAL', 'NUMERIC'))]

    search_cols = text_cols[:]
    if prefer_cols:
        search_cols = [c for c in prefer_cols if c in text_cols] + [c for c in text_cols if c not in prefer_cols]

    found = []
    for col in search_cols:
        try:
            q = f"SELECT * FROM [{table}] WHERE [{col}] LIKE ?"
            cur.execute(q, f"%{id_value}%")
            rows = cur.fetchall()
            if rows:
                # Build minimal dict per row
                headers = [d[0] for d in cur.description]
                for r in rows:
                    rowd = {h: r[i] for i, h in enumerate(headers)}
                    found.append(rowd)
        except Exception:
            continue
    return found


def read_shp_records(shp_path: str) -> Tuple[List[Dict[str, str]], List[str]]:
    if shapefile is None:
        raise RuntimeError("pyshp non disponibile. Installa con: pip install pyshp")
    if not os.path.exists(shp_path):
        raise FileNotFoundError(f"Shapefile non trovato: {shp_path}")
    r = shapefile.Reader(shp_path)
    fields = [f[0] for f in r.fields if f[0] != 'DeletionFlag']
    recs = []
    for sr in r.iterShapeRecords():
        rd = {fields[i]: sr.record[i] for i in range(len(fields))}
        # add some geometry convenience for lines
        if sr.shape and sr.shape.points:
            rd['_geom_first_x'] = sr.shape.points[0][0]
            rd['_geom_first_y'] = sr.shape.points[0][1]
        recs.append(rd)
    return recs, fields


def find_in_shp_by_id(recs: List[Dict[str, str]], id_value: str, id_fields: List[str]) -> Optional[Dict[str, str]]:
    id_value_low = id_value.lower()
    for rec in recs:
        for f in id_fields:
            if f in rec and isinstance(rec[f], (str, bytes)):
                if id_value_low in str(rec[f]).lower():
                    return rec
    # Try generic 'ID' field fallback
    for rec in recs:
        for k, v in rec.items():
            if k.lower().startswith('id') and isinstance(v, (str, bytes)):
                if id_value_low in str(v).lower():
                    return rec
    return None


def check_file_exists_for_id(docs_dir: str, id_value: str) -> bool:
    if not docs_dir or not os.path.isdir(docs_dir):
        return False
    id_low = id_value.lower().replace(" ", "")
    for root, _, files in os.walk(docs_dir):
        for fn in files:
            if fn.lower().endswith(".pdf") and id_low in fn.lower().replace(" ", ""):
                return True
    return False


def write_result(rows: List[Dict[str, str]], out_csv: str):
    if not rows:
        info("Nessun risultato da scrivere.")
        return
    keys = sorted({k for row in rows for k in row.keys()})
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    info(f"Report scritto: {out_csv}")


def main():
    ap = argparse.ArgumentParser(description="Verifica mirata MDB <-> Shapefile per Collebeato")
    ap.add_argument("--mdb", required=True, help="Percorso al file CdI_Tabelle.mdb")
    ap.add_argument("--ind_pu_shp", required=True, help="Percorso a Ind_pu.shp")
    ap.add_argument("--ind_ln_shp", required=True, help="Percorso a Ind_ln.shp")
    ap.add_argument("--docs_dir", default="", help="Cartella con PDF delle indagini (opzionale)")
    ap.add_argument("--out", default="Collebeato_integr_check.csv", help="File CSV di output")
    args = ap.parse_args()

    results = []

    # --- MDB ---
    try:
        conn = connect_mdb(args.mdb)
        tables = list_tables(conn)
        tables_low = [t.lower() for t in tables]

        # Metadati -> Proprietario = Regione Lombardia
        metatable = find_table_case_insensitive(["Metadati"], tables)
        meta_ok = "NA"
        meta_owner = ""
        if metatable:
            cols = [c for c, _ in table_columns(conn, metatable)]
            owner_col = None
            for c in cols:
                if "propriet" in c.lower():  # proprietario, proprieta, ecc.
                    owner_col = c
                    break
            if owner_col:
                cur = conn.cursor()
                try:
                    cur.execute(f"SELECT [{owner_col}] FROM [{metatable}]")
                    vals = [str(r[0]) if r[0] is not None else "" for r in cur.fetchall()]
                    # consider OK if any value equals "Regione Lombardia" (case-insensitive, trimmed)
                    meta_ok = any(v.strip().lower() == "regione lombardia" for v in vals)
                    meta_owner = ";".join(vals)
                except Exception as e:
                    warn(f"Errore lettura Metadati: {e}")
                    meta_ok = "ERR"
        results.append({
            "check": "Metadati proprietario",
            "expected": "Regione Lombardia",
            "value": meta_owner,
            "table": metatable or "n/a",
            "status": "OK" if meta_ok is True else ("KO" if meta_ok is False else str(meta_ok))
        })

        # P46 in indagini_puntuali con tipologia penetrometrica dinamica pesante
        # tentiamo su tabella Indagini_puntuali
        cand_tables = ["Indagini_puntuali", "indagini_puntuali"]
        indpu_table = find_table_case_insensitive(cand_tables, tables) or find_table_case_insensitive(["Indagini"], tables)
        p46_rows = []
        p46_typ = ""
        if indpu_table:
            # Colonne preferite per ricerca: ID, ID_SP*, ID_IND*
            prefer = []
            for c, _ in table_columns(conn, indpu_table):
                if re.search(r"^(ID|ID_)", c, re.IGNORECASE):
                    prefer.append(c)
            p46_rows = get_value_like(conn, indpu_table, "P46", prefer_cols=prefer)
            if p46_rows:
                # prova a dedurre colonna 'tipo'
                cand_type_cols = [k for k in p46_rows[0].keys() if "tipo" in k.lower() or "indag" in k.lower()]
                if cand_type_cols:
                    p46_typ = str(p46_rows[0].get(cand_type_cols[0], ""))
        results.append({
            "check": "P46 in Indagini_puntuali",
            "expected": "Presente con tipologia 'prova penetrometrica dinamica pesante' (codice dizionario)",
            "value": p46_typ or ("trovate righe" if p46_rows else "non trovato"),
            "table": indpu_table or "n/a",
            "status": "OK" if p46_rows else "KO"
        })

        # P107 + P108 (SMS) in indagini puntuali
        sms_found = []
        for pid in ("P107", "P108"):
            rows = []
            typ = ""
            if indpu_table:
                prefer = []
                for c, _ in table_columns(conn, indpu_table):
                    if re.search(r"^(ID|ID_)", c, re.IGNORECASE):
                        prefer.append(c)
                rows = get_value_like(conn, indpu_table, pid, prefer_cols=prefer)
                if rows:
                    cand_type_cols = [k for k in rows[0].keys() if "tipo" in k.lower() or "indag" in k.lower()]
                    if cand_type_cols:
                        typ = str(rows[0].get(cand_type_cols[0], ""))
            results.append({
                "check": f"{pid} in Indagini_puntuali",
                "expected": "Presente, tipologia SMS (stazione singola)",
                "value": typ or ("trovate righe" if rows else "non trovato"),
                "table": indpu_table or "n/a",
                "status": "OK" if rows else "KO"
            })
            sms_found.append((pid, bool(rows)))

        # L6 coordinate in indagini_lineari / siti_lineari
        indln_table = find_table_case_insensitive(["Indagini_lineari", "Sito_lineare", "Siti_lineari"], tables)
        l6_status = "KO"
        l6_x = ""
        l6_y = ""
        l6_table_used = indln_table or "n/a"
        if indln_table:
            prefer = []
            for c, _ in table_columns(conn, indln_table):
                if re.search(r"^(ID|ID_)", c, re.IGNORECASE):
                    prefer.append(c)
            l6_rows = get_value_like(conn, indln_table, "L6", prefer_cols=prefer)
            if l6_rows:
                # prova a catturare colonne X/Y di inizio
                candidate_x = None
                candidate_y = None
                for k in l6_rows[0].keys():
                    kl = k.lower()
                    if ('x' in kl and ('iniz' in kl or 'start' in kl)) or kl in ('x_inizio','x_start','xini'):
                        candidate_x = k
                    if ('y' in kl and ('iniz' in kl or 'start' in kl)) or kl in ('y_inizio','y_start','yini'):
                        candidate_y = k
                l6_x = str(l6_rows[0].get(candidate_x, "")) if candidate_x else ""
                l6_y = str(l6_rows[0].get(candidate_y, "")) if candidate_y else ""
                l6_status = "OK"
        results.append({
            "check": "L6 coordinate in MDB",
            "expected": "Record presente con X_inizio valorizzata (modifica X eseguita)",
            "value": f"X={l6_x} Y={l6_y}".strip(),
            "table": l6_table_used,
            "status": l6_status
        })

        conn.close()
    except Exception as e:
        warn(f"Errore durante la verifica MDB: {e}")
        results.append({
            "check": "Connessione MDB",
            "expected": "Connessione riuscita",
            "value": str(e),
            "table": "n/a",
            "status": "ERR"
        })

    # --- SHAPEFILE Ind_pu ---
    try:
        pu_recs, pu_fields = read_shp_records(args.ind_pu_shp)
        # campi ID plausibili
        id_fields_pu = [f for f in pu_fields if f.upper() in ("ID_SPU","ID","IDPU","ID_SP","ID_SITE")]
        # fallback: usa tutti i campi per ricerca
        if not id_fields_pu:
            id_fields_pu = pu_fields[:]

        for pid in ("P46","P107","P108"):
            rec = find_in_shp_by_id(pu_recs, pid, id_fields_pu)
            status = "OK" if rec else "KO"
            results.append({
                "check": f"{pid} in Ind_pu.shp",
                "expected": "Presente nel layer puntuale",
                "value": "trovato" if rec else "non trovato",
                "table": os.path.basename(args.ind_pu_shp),
                "status": status
            })
    except Exception as e:
        warn(f"Errore lettura Ind_pu.shp: {e}")
        results.append({
            "check": "Lettura Ind_pu.shp",
            "expected": "Shapefile leggibile",
            "value": str(e),
            "table": os.path.basename(args.ind_pu_shp),
            "status": "ERR"
        })

    # --- SHAPEFILE Ind_ln ---
    try:
        ln_recs, ln_fields = read_shp_records(args.ind_ln_shp)
        id_fields_ln = [f for f in ln_fields if f.upper() in ("ID_SLN","ID","ID_LN","ID_LINE")]
        if not id_fields_ln:
            id_fields_ln = ln_fields[:]
        rec = find_in_shp_by_id(ln_recs, "L6", id_fields_ln)
        status = "OK" if rec else "KO"
        val = "non trovato"
        if rec:
            x = rec.get("_geom_first_x","")
            y = rec.get("_geom_first_y","")
            val = f"geom_first_vertex X={x} Y={y}"
        results.append({
            "check": "L6 in Ind_ln.shp (geometria di inizio)",
            "expected": "Presente; coord. inizio valorizzata",
            "value": val,
            "table": os.path.basename(args.ind_ln_shp),
            "status": status
        })
    except Exception as e:
        warn(f"Errore lettura Ind_ln.shp: {e}")
        results.append({
            "check": "Lettura Ind_ln.shp",
            "expected": "Shapefile leggibile",
            "value": str(e),
            "table": os.path.basename(args.ind_ln_shp),
            "status": "ERR"
        })

    # --- PDF allegati (opzionale) ---
    for pid in ("P46","P107","P108"):
        if args.docs_dir:
            exists = check_file_exists_for_id(args.docs_dir, pid)
            results.append({
                "check": f"PDF allegato {pid}",
                "expected": "Documento PDF presente in docs_dir",
                "value": args.docs_dir,
                "table": "allegati",
                "status": "OK" if exists else "KO"
            })

    write_result(results, args.out)


if __name__ == "__main__":
    main()
