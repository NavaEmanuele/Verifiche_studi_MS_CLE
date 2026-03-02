# scripts/mdb_validate.py
import sys, re, pyodbc, pandas as pd, yaml
from pathlib import Path

def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def connect_mdb(mdb_path):
    return pyodbc.connect(
        f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb_path};",
        autocommit=True
    )

def list_tables(cnx):
    cur = cnx.cursor()
    return [r.table_name for r in cur.tables(tableType="TABLE") if not r.table_name.startswith("MSys")]

def fetch_table(cnx, table):
    return pd.read_sql(f"SELECT * FROM [{table}]", cnx)

def check_types(series, ftype):
    s = series.dropna()
    if ftype=="string":  return s.map(lambda v: isinstance(v, str)).all()
    if ftype=="integer": return s.map(lambda v: isinstance(v,int) or (isinstance(v,float) and float(v).is_integer())).all()
    if ftype=="float":   return s.map(lambda v: isinstance(v,(int,float))).all()
    return True

def main(mdb_path, schema_yaml, codes_yaml, out_csv):
    schema = load_yaml(schema_yaml)
    codes  = load_yaml(codes_yaml) if codes_yaml and codes_yaml not in ("-","") else {}
    res = []

    def add(table, rule, level, detail, row_id=""):
        res.append({"table":table, "row":row_id, "rule":rule, "level":level, "detail":detail})

    cnx = connect_mdb(mdb_path)
    tables = list_tables(cnx)

    spec_tables = schema.get("tables", {})
    required = {t for t,s in spec_tables.items() if s.get("required", False)}
    missing = required - set(tables)
    for t in sorted(missing):
        add(t, "TABLE-MISSING", "error", "Tabella richiesta assente")

    for t in tables:
        if t not in spec_tables:
            add(t, "TABLE-UNSPEC", "warning", "Tabella non prevista (ok se extra)")
            continue

        spec = spec_tables[t]
        try:
            df = fetch_table(cnx, t)
        except Exception as e:
            add(t, "READ", "error", f"Errore lettura: {e}")
            continue

        cols = list(df.columns)
        for f in spec.get("fields", []):
            name = f["name"]
            if f.get("required", False) and name not in cols:
                add(t, f"FIELD({name})", "error", "Campo obbligatorio assente")
                continue
            if name not in cols:
                continue

            ser = df[name]
            if "type" in f:
                ok = check_types(ser, f["type"])
                add(t, f"FIELD-TYPE({name})", "ok" if ok else "error", f"atteso={f['type']}")
            if f.get("not_null", False):
                nn = ser.isna().sum()
                add(t, f"NOT-NULL({name})", "ok" if nn==0 else "error", f"nulls={nn}")
            if f.get("unique", False):
                dups = ser.duplicated().sum()
                add(t, f"UNIQUE({name})", "ok" if dups==0 else "error", f"duplicati={dups}")
            if f.get("type")=="string":
                if "max_length" in f:
                    bad = ser.dropna().astype(str).map(len) > int(f["max_length"])
                    add(t, f"MAX-LEN({name})", "ok" if (~bad).all() else "error", f"max={f['max_length']} violazioni={int(bad.sum())}")
                if "min_length" in f:
                    bad = ser.dropna().astype(str).map(len) < int(f["min_length"])
                    add(t, f"MIN-LEN({name})", "ok" if (~bad).all() else "error", f"min={f['min_length']} violazioni={int(bad.sum())}")
            allowed = None
            if "enum_from" in f:
                ref = f["enum_from"].split(".")[-1]
                group = codes.get(ref, {})
                allowed = set(group.keys()) if isinstance(group, dict) else set(group if isinstance(group, list) else [])
            elif "enum" in f:
                allowed = set(f["enum"])
            if allowed is not None:
                vals = ser.dropna()
                bad = vals[~vals.isin(allowed)]
                add(t, f"ENUM({name})", "ok" if bad.empty else "error", f"non_ammessi={sorted(set(map(str,bad)))[:12]}")
            if "regex" in f:
                import re
                pat = re.compile(f["regex"])
                vals = ser.dropna().astype(str)
                bad = vals[~vals.map(lambda x: bool(pat.fullmatch(x)))]
                add(t, f"REGEX({name})", "ok" if bad.empty else "error", f"non_conformi={bad.head(10).tolist()}")

    pd.DataFrame(res).to_csv(out_csv, index=False, encoding="utf-8")

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Uso: python mdb_validate.py <file.mdb> <schema_yaml> <codes_yaml|-|> <out_csv>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
