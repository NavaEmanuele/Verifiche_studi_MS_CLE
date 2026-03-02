import os, re, yaml
import geopandas as gpd
import pandas as pd

ROOT     = os.getcwd()
CONFIG   = os.path.join(ROOT, "config", "ms1_schema.yaml")
DATA_DIR = os.path.join(ROOT, "data")
REPORT   = os.path.join(ROOT, "reports")
os.makedirs(REPORT, exist_ok=True)

schema = yaml.safe_load(open(CONFIG, encoding="utf-8"))
errors = []

def validate(val, fdef):
    msgs = []
    name = fdef["name"]
    # required
    if fdef.get("required") and (val is None or (isinstance(val,str) and not val.strip())):
        msgs.append(f"{name} required but missing")
        return msgs
    # regex
    rx = fdef.get("regex")
    if rx and val not in (None, ""):
        if not re.fullmatch(rx, str(val)):
            msgs.append(f"{name}='{val}' does not match {rx}")
    # length
    ln = fdef.get("length")
    if ln and isinstance(val,str) and len(val.strip())!=ln:
        msgs.append(f"{name} length {len(val.strip())} != expected {ln}")
    # allowed_values
    av = fdef.get("allowed_values")
    if av and str(val) not in map(str,av):
        msgs.append(f"{name}='{val}' not in {av}")
    return msgs

for comune in sorted(os.listdir(DATA_DIR)):
    base = os.path.join(DATA_DIR, comune)
    for cat, layers in schema.get("shapefiles", {}).items():
        folder = os.path.join(base, cat)
        if not os.path.isdir(folder):
            continue

        for shp in os.listdir(folder):
            if not shp.lower().endswith(".shp"):
                continue
            layer = shp.replace(".shp","")
            fdefs = layers.get(layer, {}).get("fields", [])
            path = os.path.join(folder, shp)
            try:
                gdf = gpd.read_file(path)
            except Exception as e:
                errors.append({"comune":comune,"categoria":cat,"layer":layer,
                               "fid":"","field":"","message":f"open error: {e}"})
                continue

            for idx, row in gdf.iterrows():
                for fdef in fdefs:
                    name = fdef["name"]
                    if name not in gdf.columns:
                        continue
                    val = row[name]
                    for m in validate(val, fdef):
                        errors.append({
                            "comune": comune,
                            "categoria": cat,
                            "layer": layer,
                            "fid": idx,
                            "field": name,
                            "message": m
                        })

df = pd.DataFrame(errors,
    columns=["comune","categoria","layer","fid","field","message"])
out = os.path.join(REPORT, "ms1_value_errors.csv")
df.to_csv(out, index=False, encoding="utf-8")
print("3) Validate MS1 values →", out)
