import os, yaml
import pandas as pd

ROOT     = os.getcwd()
CONFIG   = os.path.join(ROOT, "config", "ms1_schema.yaml")
CSV_IN   = os.path.join(ROOT, "reports", "ms1_shapefile_fields.csv")
CSV_OUT  = os.path.join(ROOT, "reports", "ms1_field_differences.csv")

schema = yaml.safe_load(open(CONFIG, encoding="utf-8"))
df = pd.read_csv(CSV_IN, dtype=str).fillna("")

records = []
for _, r in df.iterrows():
    shp     = r["shapefile"].replace(".shp","")
    comune  = r["comune"]
    cat     = r["categoria"]
    found   = set(r["fields"].split(";")) if r["fields"] else set()

    expected = {f["name"] for f in schema["shapefiles"].get(cat, {}).get(shp, {}).get("fields", [])}

    missing = sorted(expected - found)
    extra   = sorted(found - expected)

    records.append({
        "comune": comune,
        "categoria": cat,
        "shapefile": shp,
        "missing":   ",".join(missing),
        "extra":     ",".join(extra)
    })

pd.DataFrame(records).to_csv(CSV_OUT, index=False, encoding="utf-8")
print("2) Compare MS1 fields →", CSV_OUT)
