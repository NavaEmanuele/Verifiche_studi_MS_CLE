import os
import geopandas as gpd
import yaml

VALID_FOLDERS = ['CLE']
RULES_PATH = "config/regole_shape.yaml"

with open(RULES_PATH, 'r', encoding='utf-8') as f:
    RULES = yaml.safe_load(f)

def check_composition(row, rule, field):
    try:
        parts = rule['composition'].split('+')
        expected = ''.join(str(row[p.strip()]) for p in parts)
        return str(row.get(field)) == expected
    except:
        return False

def check_shapefile_attributes_advanced(base_path):
    results = []

    for root, _, files in os.walk(base_path):
        if not any(folder in root for folder in VALID_FOLDERS):
            continue

        for file in files:
            if not file.endswith(".shp"):
                continue

            shp_path = os.path.join(root, file)
            layer = os.path.splitext(file)[0]

            if layer not in RULES:
                continue

            try:
                gdf = gpd.read_file(shp_path)
            except Exception as e:
                results.append({
                    'shapefile': layer,
                    'controllo': 'lettura',
                    'esito': 'ERRORE',
                    'dettagli': str(e)
                })
                continue

            for field, rule in RULES[layer].items():
                if field not in gdf.columns:
                    results.append({
                        'shapefile': layer,
                        'controllo': f"campo '{field}'",
                        'esito': 'MANCANTE',
                        'dettagli': 'Campo non presente'
                    })
                    continue

                for idx, val in gdf[field].items():
                    value = str(val).strip() if val is not None else ''
                    row_info = {
                        'shapefile': layer,
                        'controllo': f"campo '{field}'",
                        'esito': 'OK',
                        'dettagli': f"Riga {idx}"
                    }

                    if rule.get('required') and value == '':
                        row_info['esito'] = 'VALORE NULLO'
                    elif 'length' in rule and len(value) != rule['length']:
                        row_info['esito'] = 'LUNGHEZZA ERRATA'
                        row_info['dettagli'] += f" (atteso: {rule['length']})"
                    elif 'domain' in rule and value not in rule['domain']:
                        row_info['esito'] = 'FUORI DOMINIO'
                        row_info['dettagli'] += f" (valore: {value})"

                    if row_info['esito'] != 'OK':
                        results.append(row_info)

            for field, rule in RULES[layer].items():
                if 'composition' in rule and field in gdf.columns:
                    for idx, row in gdf.iterrows():
                        if not check_composition(row, rule, field):
                            results.append({
                                'shapefile': layer,
                                'controllo': f"composizione campo '{field}'",
                                'esito': 'ERRORE COMPOSIZIONE',
                                'dettagli': f"Riga {idx}"
                            })

    return results
