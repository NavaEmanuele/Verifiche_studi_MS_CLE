# scripts/topology_validation.py
import sys
from pathlib import Path
import geopandas as gpd
import pandas as pd

def read_any(path):
    try:
        return gpd.read_file(path)
    except Exception:
        return gpd.read_file(path, engine="pyogrio")

def ensure_same_crs(a, b):
    if a.crs is None or b.crs is None:
        return a, b
    if a.crs == b.crs:
        return a, b
    return a.to_crs(b.crs), b

def sindex_within(children, parents, child_id=None, parent_id=None):
    problems = []
    ch = children.copy()
    pa = parents.copy()
    try: ch["geometry"] = ch.buffer(0)
    except Exception: pass
    try: pa["geometry"] = pa.buffer(0)
    except Exception: pass
    sidx = pa.sindex
    for i, row in ch.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            problems.append((str(row.get(child_id, i)), "GEOM-EMPTY")); continue
        cand = list(sidx.intersection(geom.bounds))
        inside = False
        for j in cand:
            if geom.within(pa.geometry.iloc[j]) or geom.covered_by(pa.geometry.iloc[j]):
                inside = True; break
        if not inside:
            problems.append((str(row.get(child_id, i)), "NOT-WITHIN"))
    return problems

def no_overlaps_within(gdf, id_field=None):
    overlaps = []
    gdf = gdf.reset_index(drop=False).rename(columns={"index":"_idx"})
    try: gdf["geometry"] = gdf.buffer(0)
    except Exception: pass
    sindex = gdf.sindex
    for i, geom in enumerate(gdf.geometry):
        if geom is None or geom.is_empty: continue
        cand_idx = list(sindex.intersection(geom.bounds))
        for j in cand_idx:
            if j <= i: continue
            g2 = gdf.geometry.iloc[j]
            if g2 is None or g2.is_empty: continue
            if geom.intersects(g2):
                inter = geom.intersection(g2)
                if not inter.is_empty and inter.area > 0:
                    a = gdf.iloc[i]; b = gdf.iloc[j]
                    a_id = a.get(id_field) if id_field and id_field in a else a.get("_idx")
                    b_id = b.get(id_field) if id_field and id_field in b else a.get("_idx")
                    overlaps.append((a_id, b_id, float(inter.area)))
    return overlaps

def find(root, name):
    p = Path(root) / name
    if p.exists(): return p
    for shp in Path(root).rglob("*.shp"):
        if shp.name.lower() == name.lower():
            return shp
    return None

def main(root_dir, out_csv):
    root = Path(root_dir)
    out = []

    def add(rule, level, layer, file, detail):
        out.append({"rule": rule, "level": level, "layer": layer, "file": file, "detail": detail})

    # --- MS ---
    stab = find(root, "Stab.shp")
    if stab:
        try:
            gdf = read_any(stab)
            ovs = no_overlaps_within(gdf, id_field="ID_z" if "ID_z" in gdf.columns else None)
            if ovs:
                for a,b,area in ovs:
                    add("MS:STAB-NO-OVERLAP", "error", "Stab", str(stab.relative_to(root)), f"{a} vs {b} (area={area:.2f})")
            else:
                add("MS:STAB-NO-OVERLAP", "ok", "Stab", str(stab.relative_to(root)), "Nessuna sovrapposizione interna")
        except Exception as e:
            add("MS:STAB-NO-OVERLAP", "error", "Stab", str(stab.relative_to(root)), f"Errore: {e}")

    instab = find(root, "Instab.shp")
    if instab and stab:
        try:
            gi = read_any(instab); gs = read_any(stab)
            gi, gs = ensure_same_crs(gi, gs)
            probs = sindex_within(gi, gs, "ID_i" if "ID_i" in gi.columns else None, "ID_z" if "ID_z" in gs.columns else None)
            if probs:
                for pid, code in probs:
                    add("MS:INSTAB-IN-STAB", "error", "Instab", str(instab.relative_to(root)), f"ID_i={pid} -> {code}")
            else:
                add("MS:INSTAB-IN-STAB", "ok", "Instab", str(instab.relative_to(root)), "Tutte interne a Stab")
        except Exception as e:
            add("MS:INSTAB-IN-STAB", "error", "Instab", str(instab.relative_to(root)), f"Errore: {e}")

    # --- CLE ---
    us = find(root, "CL_US.shp")
    aggr = find(root, "CL_AS.shp")
    if us and aggr:
        try:
            gus = read_any(us); gas = read_any(aggr)
            gus, gas = ensure_same_crs(gus, gas)
            probs = sindex_within(gus, gas, "ID_US" if "ID_US" in gus.columns else None, "ID_AS" if "ID_AS" in gas.columns else None)
            if probs:
                for pid, code in probs:
                    add("CLE:US-IN-AS", "error", "CLE", f"US={str(us.relative_to(root))} | AS={str(aggr.relative_to(root))}", f"ID_US={pid} -> {code}")
            else:
                add("CLE:US-IN-AS", "ok", "CLE", f"US={str(us.relative_to(root))} | AS={str(aggr.relative_to(root))}", "Tutte le US sono interne agli AS")
        except Exception as e:
            add("CLE:US-IN-AS", "error", "CLE", f"US={str(us.relative_to(root))} | AS={str(aggr.relative_to(root))}", f"Errore: {e}")

    pd.DataFrame(out).to_csv(out_csv, index=False)
    print(out_csv)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python topology_validation.py <root_dir_dataset> <out_csv>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
