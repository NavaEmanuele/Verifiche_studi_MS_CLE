import os, glob
import geopandas as gpd
import pandas as pd
import yaml

ROOT      = os.getcwd()
DATA_DIR  = os.path.join(ROOT, "data")
CONFIG    = os.path.join(ROOT, "config", "ms1_schema.yaml")
REPORT    = os.path.join(ROOT, "reports")
os.makedirs(REPORT, exist_ok=True)

# Carico le categorie dal file YAML
with open(CONFIG, encoding="utf-8") as f:
    schema = yaml.safe_load(f)
categories = list(schema.get("shapefiles", {}).keys())

records = []
for comune in sorted(os.listdir(DATA_DIR)):
    base = os.path.join(DATA_DIR, comune)
    if not os.path.isdir(base):
        continue

    for cat in categories:
        folder = os.path.join(base, cat)
        if not os.path.isdir(folder):
            continue

        for shp_path in glob.glob(os.path.join(folder, "*.shp")):
            shp_name = os.path.basename(shp_path)
            try:
                gdf = gpd.read_file(shp_path)
                fields = list(gdf.columns)
                status = "OK"
            except Exception as e:
                fields = []
                status = f"ERROR: {e}"

            records.append({
                "comune": comune,
                "categoria": cat,
                "shapefile": shp_name,
                "fields": ";".join(fields),
                "status": status
            })

df = pd.DataFrame(records, columns=["comune","categoria","shapefile","fields","status"])
out = os.path.join(REPORT, "ms1_shapefile_fields.csv")
df.to_csv(out, index=False, encoding="utf-8")
print("1) Extract MS1 fields →", out)
