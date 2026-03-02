# fix_mdb_fields.py

import os
import yaml
import pyodbc

# Percorso al tuo YAML (contiene mdb_tables)
CONFIG_YAML = os.path.join(os.getcwd(), 'config', 'cle_schema.yaml')

# Leggi il file di configurazione
with open(CONFIG_YAML, encoding='utf-8') as f:
    schema = yaml.safe_load(f)

def fix_table(mdb_path, table, field_def):
    """
    Modifica la colonna [field_def['name']] nella tabella [table]
    secondo i parametri in field_def (tipo, lunghezza, ecc.).
    ATTENZIONE: Access ha sintassi ALTER TABLE limitata. Testa prima su un backup.
    """
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={os.path.abspath(mdb_path)};"
    )
    conn = pyodbc.connect(conn_str, autocommit=True)
    cur  = conn.cursor()

    name   = field_def['name']
    ftype  = field_def['type']
    length = field_def.get('length')

    # Esempio di ALTER TABLE per modificare tipo/size (adatta a seconda del tuo caso)
    sql = f"ALTER TABLE [{table}] ALTER COLUMN [{name}] {ftype}"
    if length:
        sql += f"({length})"

    try:
        cur.execute(sql)
        print(f"✔️ Column {name} in {table} modificata con {ftype}({length})")
    except Exception as e:
        print(f"❌ Errore fixing {table}.{name}: {e}")
    conn.close()

def main():
    data_dir = 'data'
    for comune in os.listdir(data_dir):
        mdb_path = os.path.join(data_dir, comune, 'CLE', 'CLE_db.mdb')
        if not os.path.isfile(mdb_path):
            continue

        # Per ogni tabella definita nel tuo YAML
        for table, tbl_def in schema.get('mdb_tables', {}).items():
            for field_def in tbl_def.get('fields', []):
                fix_table(mdb_path, table, field_def)

if __name__ == '__main__':
    main()
    print("✅ fix_mdb_fields.py eseguito")
