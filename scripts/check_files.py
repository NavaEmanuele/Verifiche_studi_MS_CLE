import os

def check_files_structure(base_path):
    expected_folders = ['CLE', 'MS1', 'GeoTec', 'Indagini', 'Elaborati_finali']
    expected_extensions = ['.shp', '.dbf', '.mdb', '.pdf']
    results = []
    for folder in expected_folders:
        folder_path = os.path.join(base_path, folder)
        exists = os.path.isdir(folder_path)
        results.append({
            'Controllo': f'Cartella {folder}',
            'Risultato': 'OK' if exists else 'MANCANTE',
            'Dettagli': folder_path if exists else 'Non trovata'
        })
    for root, _, files in os.walk(base_path):
        for ext in expected_extensions:
            found = any(file.lower().endswith(ext) for file in files)
            results.append({
                'Controllo': f'File con estensione {ext}',
                'Risultato': 'OK' if found else 'MANCANTE',
                'Dettagli': root if found else f'Nessun file {ext} in {root}'
            })
    return results
