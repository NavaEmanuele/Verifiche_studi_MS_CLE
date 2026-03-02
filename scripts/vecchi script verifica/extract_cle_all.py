# ---------------------------------------------
# scripts/extract_cle_all.py
# ---------------------------------------------
import os
import glob
import geopandas as gpd
import pandas as pd

# Configurazione cartelle
DATA_DIR   = os.path.join(os.getcwd(), "data")
REPORT_DIR = os.path.join(os.getcwd(), "reports")
os.makedirs(REPORT_DIR, exist_ok=True)

records = []
for comune in sorted(os.listdir(DATA_DIR)):
    base_cle = os.path.join(DATA_DIR, comune, "CLE")
    if not os.path.isdir(base_cle):
        continue

    # Trova tutti i file .shp in data/<Comune>/CLE/
    for shp_path in glob.glob(os.path.join(base_cle, "*.shp")):
        shp_name = os.path.basename(shp_path)
        try:
            gdf = gpd.read_file(shp_path)
            fields = list(gdf.columns)
            status = "OK"
        except Exception as e:
            fields = []
            status = "ERROR_LOADING: " + str(e)

        fields_str = ";".join(fields) if fields else ""
        records.append({
            "comune": comune,
            "shapefile": shp_name,
            "fields": fields_str,
            "status": status
        })

# Salva il CSV
df = pd.DataFrame(records, columns=["comune", "shapefile", "fields", "status"])
csv_out = os.path.join(REPORT_DIR, "cle_shapefile_fields.csv")
df.to_csv(csv_out, index=False, encoding="utf-8")
print("Estrazione campi CLE completata. File:", csv_out)


