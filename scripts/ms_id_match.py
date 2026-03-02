# scripts/ms_id_match.py  (versione senza GDAL/pyogrio)
import sys, os, csv
from pathlib import Path

import yaml
import pyodbc
import pandas as pd
import shapefile  # pyshp

def norm_series(s: pd.Series, cfg: dict | None) -> pd.Series:
    if s is None:
        return s
    out = s.astype(str)
    if not cfg:
        return out.str.strip()
    if cfg.get("strip", True):
        out = out.str.strip()
    if cfg.get("collapse_spaces", True):
        out = out.str.replace(r"\s+", " ", regex=True)
    if cfg.get("upper", True):
        out = out.str.upper()
    if cfg.get("istat_pad6", False):
        def _pad_istat(x):
            try:
                parts = x.split("-", 1)
                if parts and parts[0].isdigit():
                    parts[0] = parts[0].zfill(6)
                    return "-".join(parts)
            except Exception:
                pass
            return x
        out = out.map(_pad_istat)
    return out

def read_shp_attrs(shp_path: str, wanted_fields: list[str]) -> pd.DataFrame:
    """Legge solo gli attributi richiesti (DBF) con pyshp, senza geometrie."""
    r = shapefile.Reader(shp_path)
    # pyshp: r.fields[0] è ('DeletionFlag', ...). I veri nomi iniziano da index 1
    field_names = [f[0] for f in r.fields[1:]]
    # mappa case-insensitive -> nome reale
    name_map = {n.lower(): n for n in field_names}
    # verifica campi richiesti
    missing = [f for f in wanted_fields if f.lower() not in name_map]
    if missing:
        raise ValueError(f"Campi mancanti nello shapefile {os.path.basename(shp_path)}: {missing}")
    idx = [field_names.index(name_map[f.lower()]) for f in wanted_fields]

    rows = []
    for rec in r.iterRecords():
        row = {wanted_fields[i]: rec[idx[i]] for i in range(len(idx))}
        rows.append(row)
    return pd.DataFrame(rows, columns=wanted_fields)

def read_mdb_table(cnx, table: str, id_field: str, extra_fields: list[str]) -> pd.DataFrame:
    cols = [id_field] + (extra_fields or [])
    col_sql = ", ".join(f"[{c}]" for c in cols)
    return pd.read_sql(f"SELECT {col_sql} FROM [{table}]", cnx)

def main(dataset_root, mdb_path, linkmap_yaml, out_csv):
    assert os.path.exists(dataset_root), f"dataset_root non trovato: {dataset_root}"
    assert os.path.exists(mdb_path), f"mdb non trovato: {mdb_path}"
    assert os.path.exists(linkmap_yaml), f"yaml non trovato: {linkmap_yaml}"

    cfg = yaml.safe_load(open(linkmap_yaml, "r", encoding="utf-8"))
    blocks = cfg.get("ms_linkmap", [])
    if not blocks:
        print("Nessun blocco ms_linkmap nel YAML.")
        sys.exit(2)

    cnx = pyodbc.connect(f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb_path};", autocommit=True)
    rows: list[dict] = []

    def emit(**kw):
        rows.append(kw)

    for block in blocks:
        name = block["name"]
        shp = os.path.join(dataset_root, block["shp"])
        shp_id = block["shp_id_field"]
        tab = block["mdb_table"]
        mdb_id = block["mdb_id_field"]
        norm_cfg = block.get("normalize", {"strip": True, "collapse_spaces": True, "upper": True})

        # --- check file shp
        if not os.path.exists(shp):
            emit(check=name, kind="READ_SHP_ERROR", detail=f"Shapefile non trovato: {shp}")
            continue

        # --- read SHP attributes
        extra_fk = [fk["shp_field"] for fk in block.get("also_check_fk", [])] if block.get("also_check_fk") else []
        shp_fields = [shp_id] + extra_fk
        try:
            gdf = read_shp_attrs(shp, shp_fields)
        except Exception as e:
            emit(check=name, kind="READ_SHP_ERROR", detail=str(e))
            continue
        if shp_id not in gdf.columns:
            emit(check=name, kind="SHP_FIELD_MISSING", field=shp_id, detail=f"Campo non presente in {shp}")
            continue

        # --- read MDB table
        extra_mdb = [fk["mdb_field"] for fk in block.get("also_check_fk", [])] if block.get("also_check_fk") else []
        try:
            mdb_df = read_mdb_table(cnx, tab, mdb_id, extra_mdb)
        except Exception as e:
            emit(check=name, kind="READ_MDB_ERROR", detail=f"{tab}: {e}")
            continue
        if mdb_id not in mdb_df.columns:
            emit(check=name, kind="MDB_FIELD_MISSING", table=tab, field=mdb_id, detail=f"Campo non presente in tabella {tab}")
            continue

        # --- normalize IDs
        A = norm_series(gdf[shp_id].dropna(), norm_cfg)
        B = norm_series(mdb_df[mdb_id].dropna(), norm_cfg)

        # --- duplicates
        dupA = A[A.duplicated(keep=False)]
        for v in sorted(set(dupA)):
            emit(check=name, kind="DUP_SHP_ID", id=v, count=int((A == v).sum()))
        dupB = B[B.duplicated(keep=False)]
        for v in sorted(set(dupB)):
            emit(check=name, kind="DUP_MDB_ID", id=v, count=int((B == v).sum()))

        # --- set diff
        setA, setB = set(A), set(B)
        only_shp = sorted(setA - setB)
        only_mdb = sorted(setB - setA)
        for v in only_shp:
            emit(check=name, kind="ID_ONLY_IN_SHP", id=v)
        for v in only_mdb:
            emit(check=name, kind="ID_ONLY_IN_MDB", id=v)

        # --- optional FKs
        for fk in block.get("also_check_fk", []) or []:
            sfield = fk["shp_field"]
            t = fk["mdb_table"]
            f = fk["mdb_field"]
            if sfield not in gdf.columns:
                emit(check=name, kind="SHP_FIELD_MISSING", field=sfield, detail=f"Campo FK assente nello shapefile")
                continue
            try:
                ref = pd.read_sql(f"SELECT [{f}] AS k FROM [{t}]", cnx)
                REF = norm_series(ref["k"].dropna(), norm_cfg)
                REF = set(REF)
            except Exception as e:
                emit(check=name, kind="READ_MDB_ERROR", detail=f"{t}: {e}")
                continue
            miss = sorted(set(norm_series(gdf[sfield].dropna(), norm_cfg)) - REF)
            for v in miss:
                emit(check=name, kind="FK_MISSING", field=sfield, value=v, ref_table=t, ref_field=f)

        # --- summary
        emit(check=name, kind="SUMMARY",
             shp_count=int(len(A)), mdb_count=int(len(B)),
             only_shp=len(only_shp), only_mdb=len(only_mdb),
             dup_shp=int(len(set(dupA))), dup_mdb=int(len(set(dupB))))

    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    all_keys = set()
    for r in rows:
        all_keys |= set(r.keys())
    order = [k for k in ("check","kind","id","field","value","ref_table","ref_field","count","detail","shp_count","mdb_count","only_shp","only_mdb","dup_shp","dup_mdb") if k in all_keys]
    order += [k for k in sorted(all_keys) if k not in order]

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=order, delimiter=";")
        w.writeheader(); w.writerows(rows)

    print(f"OK: report -> {out_csv}")

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("usage: python ms_id_match.py <dataset_root> <mdb_path> <linkmap_yaml> <out_csv>")
        sys.exit(3)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])



