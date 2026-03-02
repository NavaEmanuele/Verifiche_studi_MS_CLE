import os
import geopandas as gpd

# Campi obbligatori per ciascun tipo di shapefile (esempio base CLE)
REQUIRED_FIELDS = {
    'CL_AC': ['id_scheda', 'tipo_infra', 'zona_MS'],
    'CL_AE': ['id_scheda', 'uso_att', 'zona_MS'],
    'CL_AS': ['id_scheda', 'zona_MS', 'settore'],
    'CL_US': ['id_scheda', 'zona_MS'],
    'CL_ES': ['id_scheda', 'zona_MS']
}

def check_shapefile_attributes(base_path):
    results = []

    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith(".shp"):
                path = os.path.join(root, file)
                layer_name = os.path.splitext(file)[0]
                try:
                    gdf = gpd.read_file(path)
                except Exception as e:
                    results.append({
                        'shapefile': layer_name,
                        'controllo': 'lettura',
                        'esito': 'ERRORE',
                        'dettagli': str(e)
                    })
                    continue

                required = REQUIRED_FIELDS.get(layer_name)
                if not required:
                    continue  # non è uno shapefile da controllare

                for field in required:
                    if field not in gdf.columns:
                        results.append({
                            'shapefile': layer_name,
                            'controllo': f'campo {field}',
                            'esito': 'MANCANTE',
                            'dettagli': 'Campo non presente'
                        })
                    else:
                        nulls = gdf[field].isnull().sum()
                        results.append({
                            'shapefile': layer_name,
                            'controllo': f'campo {field}',
                            'esito': 'OK' if nulls == 0 else f'{nulls} VALORI NULLI',
                            'dettagli': '-' if nulls == 0 else f'{nulls} righe da completare'
                        })

                # Verifica duplicati su id_scheda
                if 'id_scheda' in gdf.columns:
                    duplicates = gdf['id_scheda'].duplicated().sum()
                    results.append({
                        'shapefile': layer_name,
                        'controllo': 'duplicati id_scheda',
                        'esito': 'OK' if duplicates == 0 else f'{duplicates} DUPLICATI',
                        'dettagli': '-' if duplicates == 0 else 'Verificare ID univoci'
                    })

    return results
