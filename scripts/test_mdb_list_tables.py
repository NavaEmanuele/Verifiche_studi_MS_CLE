import sys, pyodbc
mdb = sys.argv[1]  # percorso file .mdb o .accdb
conn = pyodbc.connect(
    f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb};",
    autocommit=True
)
cur = conn.cursor()
tables = [r.table_name for r in cur.tables(tableType='TABLE') if not r.table_name.startswith('MSys')]
print("Tabelle:", tables)
