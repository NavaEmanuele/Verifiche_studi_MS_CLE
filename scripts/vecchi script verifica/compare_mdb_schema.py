# compare_mdb_schema.py

import os
import yaml
import pandas as pd

# Percorsi
CONFIG_YAML = os.path.join(os.getcwd(), 'config', 'cle_schema.yaml')
CSV_MDB     = os.path.join(os.getcwd(), 'reports', 'cle_mdb_schema.csv')
OUT_CSV     = os.path.join(os.getcwd(), 'reports', 'cle_mdb_differences.csv')

# Carica lo schema CLE (sezione mdb_tables)
with open(CONFIG_YAML, encoding='utf-8') as f:
    schema = yaml.safe_load(f)

# Carica il report MDB estratto
df = pd.read_csv(CSV_MDB)

records = []
for _, row in df.iterrows():
    comune = row['comune']
    table  = row['table']
    column = row['column']
    found  = column

    # I campi attesi secondo il tuo YAML
    expected = {
        f['name']
        for f in schema.get('mdb_tables', {}).get(table, {}).get('fields', [])
    }

    missing = sorted(expected - {found}) if found not in expected else []
    extra   = [found] if found not in expected else []

    records.append({
        'comune': comune,
        'table':  table,
        'column': column,
        'missing': ','.join(missing),
        'extra':   ','.join(extra)
    })

pd.DataFrame(records).to_csv(OUT_CSV, index=False)
print(f"✅ Differenze MDB salvate in: {OUT_CSV}")
