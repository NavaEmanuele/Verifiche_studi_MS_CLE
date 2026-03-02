# extract_cle_all.py

import os
import geopandas as gpd
import pandas as pd

DATA_DIR   = "data"
REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

results = []

# per ogni comune
for comune in os.listdir(DATA_DIR):
    comune_path = os.path.join(DATA_DIR, comune, "CLE")
    if not os.path.isdir(comune_path):
        continue

    # per ciascuno dei cinque shapefile CLE_*.shp
    for shp_name in ("CL_ES.shp", "CL_AE.shp", "CL_AC.shp", "CL_AS.shp", "CL_US.shp"):
        shp_path = os.path.join(comune_path, shp_name)
        if not os.path.isfile(shp_path):
            results.append({
                "comune": comune,
                "shapefile": shp_name,
                "field": None,
                "dtype": None,
                "n_unique": None,
                "status": "MISSING"
            })
            continue

        # carica
        try:
            gdf = gpd.read_file(shp_path)
        except Exception as e:
            results.append({
                "comune": comune,
                "shapefile": shp_name,
                "field": None,
                "dtype": None,
                "n_unique": None,
                "status": f"ERROR: {e}"
            })
            continue

        # per ogni colonna
        for fld in gdf.columns:
            # skip geometry
            if fld == gdf.geometry.name:
                continue
            dtype = str(gdf[fld].dtype)
            nunq  = int(gdf[fld].nunique(dropna=False))
            results.append({
                "comune": comune,
                "shapefile": shp_name,
                "field": fld,
                "dtype": dtype,
                "n_unique": nunq,
                "status": "OK"
            })

# salva report
df = pd.DataFrame(results)
out_csv = os.path.join(REPORT_DIR, "cle_fields_report.csv")
df.to_csv(out_csv, index=False)
print(f"✅ Report CLE salvato in: {out_csv}")
