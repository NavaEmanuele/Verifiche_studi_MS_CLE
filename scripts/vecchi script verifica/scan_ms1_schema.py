# scripts/scan_ms1_schema.py

import os
import fiona
import yaml

ROOT      = os.getcwd()
DATA_DIR  = os.path.join(ROOT, "data")
OUT_YAML  = os.path.join(ROOT, "config", "ms_schema.yaml")

# Tutte le cartelle che vedo in data/<Comune> (copia esattamente dalla tua struttura)
CATEGORIES = [
    "Elineari",
    "Epuntuali",
    "Forme",
    "Geoidr",
    "Geotec",
    "Ind_In",
    "Ind_pu",
    "Instab",
    "Isosub",
    "Stab",
    "Indagini_Lineari",
    "Indagini_Puntuali",
    "Sito_Lineare",
    "Sito_Puntuale",
    "Parametri_Lineari",
    "Parametri_Puntuali",
]

schema = {}
for cat in CATEGORIES:
    schema[f"shapefiles_MS1_{cat}"] = {}

for comune in sorted(os.listdir(DATA_DIR)):
    path_comune = os.path.join(DATA_DIR, comune)
    if not os.path.isdir(path_comune):
        continue

    for cat in CATEGORIES:
        folder = os.path.join(path_comune, cat)
        if not os.path.isdir(folder):
            continue

        for fname in sorted(os.listdir(folder)):
            if not fname.lower().endswith(".shp"):
                continue

            layer = os.path.splitext(fname)[0]
            shp_path = os.path.join(folder, fname)

            try:
                with fiona.open(shp_path, 'r') as src:
                    geom  = src.schema['geometry']
                    props = src.schema['properties']
            except Exception as e:
                print(f"ERROR opening {shp_path}: {e}")
                continue

            fields = []
            for nm, tp in props.items():
                if tp.startswith("str"):
                    # 'str:80'
                    parts = tp.split(":")
                    length = int(parts[1]) if len(parts) > 1 else None
                    fields.append({"name": nm, "type": "string", "length": length})
                elif tp.startswith("int"):
                    fields.append({"name": nm, "type": "integer"})
                elif tp.startswith("float"):
                    fields.append({"name": nm, "type": "float"})
                else:
                    fields.append({"name": nm, "type": tp})

            schema_key = f"shapefiles_MS1_{cat}"
            schema[schema_key][layer] = {
                "description": "",
                "required": True,
                "geometry": geom,
                "fields": fields
            }

with open(OUT_YAML, 'w', encoding='utf-8') as f:
    yaml.dump(schema, f, sort_keys=False, allow_unicode=True)

print("Bozza di YAML generata in:", OUT_YAML)

