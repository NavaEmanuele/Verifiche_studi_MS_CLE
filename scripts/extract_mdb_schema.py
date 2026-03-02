# scripts/extract_mdb_schema.py

import os
import pyodbc
import pandas as pd

# ───── CONFIGURAZIONE ─────
DATA_DIR   = "data"
REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

records = []

for comune in os.listdir(DATA_DIR):
    cle_dir = os.path.join(DATA_DIR, comune, "CLE")
    mdb_path = os.path.join(cle_dir, "CLE_db.mdb")
    if not os.path.isfile(mdb_path):
        # Se non esiste il file .mdb, salto questo comune
        continue

    # Costruisco la stringa di connessione ODBC per Access
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={os.path.abspath(mdb_path)};"
    )
    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
    except Exception as e:
        print(f"❌ Impossibile connettersi a {mdb_path}: {e}")
        continue

    cur = conn.cursor()

    # Recupero l’elenco delle tabelle utente
    try:
        tables = [row.table_name for row in cur.tables(tableType="TABLE")]
    except Exception:
        tables = []

    for table in tables:
        # Prima prova a usare cursor.columns(table=...), se però fallisce,
        # uso il fallback con SELECT * ... WHERE 1=0
        columns = []
        types   = []
        sizes   = []
        nulls   = []

        try:
            # Tenta di leggere metadati della tabella tramite catalogo ODBC
            for col in cur.columns(table=table):
                columns.append(col.column_name)
                types.append(col.type_name)
                sizes.append(getattr(col, "column_size", None))
                nulls.append(col.nullable)
        except Exception:
            # Fallback: eseguo una query che non restituisce righe,
            # ricavo i nomi dai cursor.description
            try:
                cur.execute(f'SELECT * FROM "{table}" WHERE 1=0')
                desc = cur.description  # lista di tuple
                for d in desc:
                    columns.append(d[0])      # nome colonna
                    types.append("")          # tipo non disponibile via fallback
                    sizes.append(None)        # size non disponibile
                    nulls.append(None)        # nullable non disponibile
                cur.commit()
            except Exception:
                # Se pure questo fallisce, salto la tabella
                continue

        # Per ogni colonna estratta, calcolo il conteggio di valori distinti
        for idx, colname in enumerate(columns):
            try:
                cnt = cur.execute(
                    f'SELECT COUNT(DISTINCT "{colname}") FROM "{table}"'
                ).fetchval()
            except Exception:
                cnt = None

            records.append({
                "comune":   comune,
                "mdb":      os.path.basename(mdb_path),
                "table":    table,
                "column":   colname,
                "type":     types[idx] if idx < len(types) else "",
                "size":     sizes[idx] if idx < len(sizes) else None,
                "nullable": nulls[idx] if idx < len(nulls) else None,
                "n_unique": cnt
            })

    conn.close()

# ───── SALVATAGGIO DEL REPORT ─────
df = pd.DataFrame(records)
csv_out  = os.path.join(REPORT_DIR, "cle_mdb_schema.csv")
xlsx_out = os.path.join(REPORT_DIR, "cle_mdb_schema.xlsx")

df.to_csv(csv_out, index=False)
try:
    df.to_excel(xlsx_out, index=False)
    print(f"✅ Schema MDB esportato in: {xlsx_out}")
except ImportError:
    print(f"✅ Schema MDB esportato in: {csv_out}")

