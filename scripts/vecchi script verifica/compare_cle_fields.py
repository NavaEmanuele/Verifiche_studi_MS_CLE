# ---------------------------------------------
# scripts/compare_cle_fields.py
# ---------------------------------------------
import os
import yaml
import pandas as pd

# Percorsi file di configurazione e report estratto
CONFIG_YAML = os.path.join(os.getcwd(), "config", "cle_schema.yaml")
CSV_FIELDS  = os.path.join(os.getcwd(), "reports", "cle_shapefile_fields.csv")
OUT_CSV     = os.path.join(os.getcwd(), "reports", "cle_field_differences.csv")

# Carica lo schema YAML delle regole CLE
def load_schema(path):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)

schema = load_schema(CONFIG_YAML)

# Carica il CSV con i campi estratti dagli shapefile
df = pd.read_csv(CSV_FIELDS)

records = []
for _, row in df.iterrows():
    shp = row['shapefile'].replace('.shp', '')
    comune = row['comune']
    # Parso i campi trovati
    fields_str = row.get('fields', "")
    found = set(fields_str.split(';')) if isinstance(fields_str, str) and fields_str else set()

    # Campi attesi dal YAML per questo shapefile
    expected = set(
        field_def["name"]
        for field_def in schema.get("shapefiles", {}).get(shp, {}).get("fields", [])
    )

    missing = sorted(expected - found)
    extra   = sorted(found - expected)

    records.append({
        "comune": comune,
        "shapefile": shp,
        "expected_fields": ",".join(sorted(expected)),
        "found_fields":    ",".join(sorted(found)),
        "missing":         ",".join(missing),
        "extra":           ",".join(extra)
    })

# Esporta il report delle differenze
df_out = pd.DataFrame(records, columns=[
    "comune", "shapefile", "expected_fields", "found_fields", "missing", "extra"
])
df_out.to_csv(OUT_CSV, index=False, encoding="utf-8")
print("Differenze campi CLE salvate in:", OUT_CSV)



