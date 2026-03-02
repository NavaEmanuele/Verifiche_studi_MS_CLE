
import os
from loguru import logger

def _connect_mdb(abs_path):
    try:
        import pyodbc
        conn = pyodbc.connect(f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={abs_path};")
        return conn
    except Exception as e:
        logger.error(f"Connessione MDB fallita: {e}")
        return None

def _list_tables(conn):
    tabs = []
    cr = conn.cursor()
    for row in cr.tables(tableType='TABLE'):
        tabs.append(row.table_name)
    cr.close()
    return tabs

def _table_columns(conn, table):
    cr = conn.cursor()
    cr.execute(f"SELECT * FROM [{table}] WHERE 1=0")
    cols = [d[0] for d in cr.description]
    cr.close()
    return cols

def check_mdb(root, relpath, mdb_schema):
    out = []
    path = os.path.join(root, relpath)
    if not os.path.exists(path):
        out.append(dict(rule_id="MDB-NOT-FOUND", level="error", target=relpath, message=f"MDB mancante: {path}"))
        return out

    conn = _connect_mdb(path)
    if conn is None:
        out.append(dict(rule_id="MDB-CONNECT", level="error", target=relpath, message="Connessione MDB fallita (driver mancante?)"))
        return out

    try:
        tabs = set(_list_tables(conn))
        for t in mdb_schema.get("tables", []):
            name = t["name"]
            if name not in tabs:
                out.append(dict(rule_id="TABLE-MISSING", level="error", target=name, message="Tabella mancante nel MDB"))
                continue
            cols = set(_table_columns(conn, name))
            for f in t.get("fields", []):
                fname = f["name"]
                if f.get("required", False) and fname not in cols:
                    out.append(dict(rule_id="FIELD-MISSING", level="error", target=name, message=f"Campo MDB mancante: {fname}"))
    except Exception as e:
        out.append(dict(rule_id="MDB-ERROR", level="error", target=relpath, message=str(e)))
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return out
