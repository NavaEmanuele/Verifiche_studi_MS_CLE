import os
import pyodbc

REQUIRED_TABLES = {
    'scheda_AC': ['id_scheda', 'zona_MS', 'tipo_infra'],
    'scheda_AE': ['id_scheda', 'zona_MS', 'uso_att'],
    'scheda_AS': ['id_scheda', 'zona_MS', 'settore'],
    'scheda_US': ['id_scheda', 'zona_MS'],
    'scheda_ES': ['id_scheda', 'zona_MS']
}

def check_mdb_fields(base_path):
    results = []

    for root, _, files in os.walk(base_path):
        for file in files:
            if file.lower().endswith(".mdb"):
                file_path = os.path.join(root, file)
                conn_str = (
                    r'DRIVER={Microsoft Access Driver (*.mdb)};'
                    f'DBQ={file_path};'
                )
                try:
                    conn = pyodbc.connect(conn_str, autocommit=True)
                    cursor = conn.cursor()
                    tables = [t.table_name for t in cursor.tables(tableType='TABLE')]

                    for table, fields in REQUIRED_TABLES.items():
                        if table not in tables:
                            results.append({
                                'mdb': file,
                                'tabella': table,
                                'controllo': 'presenza',
                                'esito': 'MANCANTE',
                                'dettagli': 'Tabella assente'
                            })
                            continue

                        # Controlla i campi
                        cursor.execute(f"SELECT * FROM [{table}]")
                        col_names = [column[0] for column in cursor.description]

                        for field in fields:
                            if field not in col_names:
                                results.append({
                                    'mdb': file,
                                    'tabella': table,
                                    'controllo': f'campo {field}',
                                    'esito': 'MANCANTE',
                                    'dettagli': 'Campo non presente'
                                })
                            else:
                                # Conta valori nulli
                                cursor.execute(f"SELECT COUNT(*) FROM [{table}] WHERE [{field}] IS NULL OR [{field}] = ''")
                                nulls = cursor.fetchone()[0]
                                results.append({
                                    'mdb': file,
                                    'tabella': table,
                                    'controllo': f'campo {field}',
                                    'esito': 'OK' if nulls == 0 else f'{nulls} NULLI',
                                    'dettagli': '-' if nulls == 0 else f'{nulls} righe vuote'
                                })

                    conn.close()

                except Exception as e:
                    results.append({
                        'mdb': file,
                        'tabella': 'N/D',
                        'controllo': 'accesso MDB',
                        'esito': 'ERRORE',
                        'dettagli': str(e)
                    })

    return results
