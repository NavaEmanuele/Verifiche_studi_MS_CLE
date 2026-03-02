# extract_cle_fields.py

import os
import glob
import shapefile  # pip install pyshp
import pandas as pd

# ───── CONFIG ─────
# Cartelle radice
DATA_DIR   = os.path.join(os.getcwd(), "data")
REPORT_DIR = os.path.join(os.getcwd(), "reports")
os.makedirs(REPORT_DIR, exist_ok=True)

# ───── RACCOLTA RISULTATI ─────
records = []

# Scorro tutte le directory “Comune” sotto data/
for comune in os.listdir(DATA_DIR):
    cle_dir = os.path.join(DATA_DIR, comune, "CLE")
    if not os.path.isdir(cle_dir):
        continue

    # Trovo tutti gli shapefile (.shp) nella cartella CLE
    for shp_path in glob.glob(os.path.join(cle_dir, "*.shp")):
        shp_name = os.path.basename(shp_path)
        try:
            # Leggo con pyshp: .shp + .dbf
            sf = shapefile.Reader(shp_path)
            # sf.fields: lista di tuple; skippo il campo di cancellazione (indice 0)
            fields = [fld[0] for fld in sf.fields[1:]]
            status = "OK"
        except Exception as e:
            fields = []
            status = f"ERROR_LOADING: {e}"

        # Accumulo il risultato
        records.append({
            "comune": comune,
            "shapefile": shp_name,
            "fields": ";".join(fields) if fields else "",
            "status": status
        })

# ───── CREAZIONE DEL REPORT ─────
df = pd.DataFrame(records)

# Percorsi di output
csv_out   = os.path.join(REPORT_DIR, "cle_shapefile_fields.csv")
xlsx_out  = os.path.join(REPORT_DIR, "cle_shapefile_fields.xlsx")

# Salvo CSV
df.to_csv(csv_out, index=False)

# Salvo Excel (se openpyxl installato)
try:
    df.to_excel(xlsx_out, index=False)
    print(f"✅ Report Excel salvato in: {xlsx_out}")
except ImportError:
    print(f"✅ Report CSV salvato in: {csv_out}")

print("🎉 Elaborazione CLE shapefile fields completata.")
