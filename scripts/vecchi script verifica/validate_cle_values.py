# ---------------------------------------------
# scripts/validate_cle_values.py
# ---------------------------------------------
import os
import re
import yaml
import geopandas as gpd
import pandas as pd

# Percorsi configurazione e output
CONFIG_YAML = os.path.join(os.getcwd(), "config", "cle_schema.yaml")
DATA_DIR    = os.path.join(os.getcwd(), "data")
REPORT_DIR  = os.path.join(os.getcwd(), "reports")
os.makedirs(REPORT_DIR, exist_ok=True)

# Carica lo schema YAML delle regole CLE
with open(CONFIG_YAML, encoding="utf-8") as f:
    schema = yaml.safe_load(f)

errors = []

# Funzione di validazione di un singolo valore
def validate_value(val, field_def):
    """
    field_def: dict con chiavi 
       'name','type','length','regex','allowed_values','required',…
    val: valore letto dal GeoDataFrame (può essere stringa, numero, None…)
    Ritorna lista di messaggi di errore (vuota se OK).
    """
    msgs = []
    name = field_def["name"]
    length = field_def.get("length")
    regex = field_def.get("regex")
    allowed = field_def.get("allowed_values")

    # 1) Se required=True e val è None/NaN/"" → errore
    if field_def.get("required") and (val is None or (isinstance(val, str) and val.strip() == "")):
        msgs.append(f"Campo '{name}' obbligatorio ma vuoto o mancante")
        return msgs

    # 2) Controllo regex (solo se val non è None/NaN e regex definito)
    if regex and (val is not None and not (isinstance(val, float) and pd.isna(val))):
        text = str(val).strip()
        if not re.fullmatch(regex, text):
            msgs.append(f"'{name}'='{val}' non rispetta pattern {regex}")

    # 3) Controllo lunghezza stringa (se length definito e val è stringa)
    if length and isinstance(val, str):
        if len(val.strip()) != length:
            msgs.append(f"'{name}'='{val}' lunghezza errata (attesi {length} caratteri)")

    # 4) Controllo valori ammessi (allowed_values)
    if allowed and (val is not None and not (isinstance(val, float) and pd.isna(val))):
        if val not in allowed:
            msgs.append(f"'{name}'='{val}' non in valori ammessi {allowed}")

    return msgs

# Scorri tutti i comuni e i relativi shapefile CLE
for comune in sorted(os.listdir(DATA_DIR)):
    base_cle = os.path.join(DATA_DIR, comune, "CLE")
    if not os.path.isdir(base_cle):
        continue

    for shp_file in os.listdir(base_cle):
        if not shp_file.lower().endswith(".shp"):
            continue
        shp_name = shp_file.replace(".shp", "")
        shp_path = os.path.join(base_cle, shp_file)

        # Trovo le definizioni di campo nel YAML
        field_defs = schema.get("shapefiles", {}).get(shp_name, {}).get("fields", [])
        if not field_defs:
            # Se non esiste la chiave, salto (verrà segnalato da compare_cle_fields)
            continue

        try:
            gdf = gpd.read_file(shp_path)
        except Exception as e:
            errors.append({
                "comune": comune,
                "shapefile": shp_name,
                "fid": "",
                "field": "",
                "value": "",
                "message": f"Errore apertura Shapefile: {e}"
            })
            continue

        # Per ogni feature (riga) del GeoDataFrame
        for idx, row in gdf.iterrows():
            for fld_def in field_defs:
                fname = fld_def["name"]
                if fname not in gdf.columns:
                    # se il campo non esiste, lo segnala compare_cle_fields
                    continue
                val = row[fname]
                msgs = validate_value(val, fld_def)
                for m in msgs:
                    errors.append({
                        "comune": comune,
                        "shapefile": shp_name,
                        "fid": idx,
                        "field": fname,
                        "value": val,
                        "message": m
                    })

# Salvo il CSV con tutti gli errori di valore
df_err = pd.DataFrame(errors, columns=["comune", "shapefile", "fid", "field", "value", "message"])
csv_out = os.path.join(REPORT_DIR, "cle_value_errors.csv")
df_err.to_csv(csv_out, index=False, encoding="utf-8")
print("Validazione valori CLE completata. File:", csv_out)
