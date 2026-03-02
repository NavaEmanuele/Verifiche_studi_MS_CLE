# link_shapes_mdb.py
import sys, pyodbc, pandas as pd, geopandas as gpd
from pathlib import Path

def read_any(path):
    try: return gpd.read_file(path)
    except Exception: return gpd.read_file(path, engine="pyogrio")

def get_ids_shp(root, name, id_field):
    shp = None
    p = Path(root) / "CLE" / f"{name}.shp"
    if p.exists():
        shp = p
    else:
        for s in Path(root).rglob(f"{name}.shp"):
            shp = s; break
    if not shp: return set(), f"{name}.shp non trovato"
    try:
        gdf = read_any(shp)
    except Exception as e:
        return set(), f"errore lettura {shp.name}: {e}"
    if id_field not in gdf.columns:
        return set(), f"campo {id_field} assente in {shp.name}"
    return set(gdf[id_field].dropna().astype(str).tolist()), str(shp)

def get_ids_mdb(mdb_path, table, id_field):
    cnx = pyodbc.connect(f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb_path};", autocommit=True)
    try:
        df = pd.read_sql(f"SELECT [{id_field}] FROM [{table}]", cnx)
    except Exception as e:
        return set(), f"errore lettura {table}: {e}"
    if id_field not in df.columns:
        return set(), f"campo {id_field} assente in {table}"
    return set(df[id_field].dropna().astype(str).tolist()), table

def compare(set_a, set_b):
    only_a = sorted(set_a - set_b)
    only_b = sorted(set_b - set_a)
    return only_a, only_b

def main(dataset_root, mdb_path, out_csv):
    root = Path(dataset_root)
    pairs = [
        ("CL_US", "ID_US", "scheda_US", "ID_US"),
        ("CL_AS", "ID_AS", "scheda_AS", "ID_AS"),
        ("CL_AE", "ID_AE", "scheda_AE", "ID_AE"),
        ("CL_AC", "ID_AC", "scheda_AC", "ID_AC"),
        ("CL_ES", "ID_ES", "scheda_ES", "ID_ES"),
    ]
    rows = []
    for shp_name, shp_id, tab, tab_id in pairs:
        shp_ids, shp_info = get_ids_shp(root, shp_name, shp_id)
        tab_ids, tab_info = get_ids_mdb(mdb_path, tab, tab_id)
        if isinstance(shp_info, str) and "errore" in shp_info.lower(): 
            rows.append({"pair": shp_name, "rule": "READ-SHP", "level": "error", "detail": shp_info}); continue
        if isinstance(tab_info, str) and "errore" in tab_info.lower():
            rows.append({"pair": shp_name, "rule": "READ-MDB", "level": "error", "detail": tab_info}); continue
        only_shp, only_tab = compare(shp_ids, tab_ids)
        level = "ok" if not only_shp and not only_tab else "error"
        detail = f"ok (count_shp={len(shp_ids)}, count_mdb={len(tab_ids)})" if level=="ok" else f"solo_shp={only_shp[:10]} | solo_mdb={only_tab[:10]}"
        rows.append({"pair": shp_name, "rule": "ID-MATCH", "level": level, "detail": detail})
    pd.DataFrame(rows).to_csv(out_csv, index=False, encoding="utf-8")
    print(out_csv)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python link_shapes_mdb.py <dataset_root> <file.mdb> <out_csv>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3])
