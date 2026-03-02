# scripts/strict_validate.py
import sys, re, yaml, pandas as pd, geopandas as gpd
from shapely.validation import explain_validity
from pathlib import Path

def read_any(path):
    try:
        return gpd.read_file(path)
    except Exception:
        return gpd.read_file(path, engine="pyogrio")

def ensure_metric_crs(gdf):
    try:
        if gdf.crs is None: return gdf
        if hasattr(gdf.crs, "axis_info"):
            units = [ai.unit_name for ai in gdf.crs.axis_info]
            if any(u and "degree" in u.lower() for u in units):
                return gdf.to_crs(epsg=3857)
        return gdf
    except Exception:
        return gdf

def guess_layer(name, schema):
    lname = name.lower()
    for layer in schema.get("shapefiles", {}):
        if layer.lower() in lname:
            return layer
    return None

def check_types(series, ftype):
    s = series.dropna()
    if ftype == "string":
        return s.map(lambda v: isinstance(v, str)).all()
    if ftype == "integer":
        return s.map(lambda v: isinstance(v, int) or (isinstance(v,float) and float(v).is_integer())).all()
    if ftype == "float":
        return s.map(lambda v: isinstance(v,(int,float))).all()
    return True

def main(dataset_root, schema_yaml, codes_yaml, out_csv):
    with open(schema_yaml, "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)
    with open(codes_yaml, "r", encoding="utf-8") as f:
        codes = yaml.safe_load(f)

    results = []
    root = Path(dataset_root)
    shp_defs = schema.get("shapefiles", {})

    def add(file_rel, layer, rule, level, detail):
        results.append({"file": file_rel, "layer": layer or "", "rule": rule, "level": level, "detail": detail})

    required_layers = {k for k,v in shp_defs.items() if v.get("required", False)}
    found_layers = set()

    for shp in sorted(root.rglob("*.shp")):
        file_rel = str(shp.relative_to(root))
        try:
            gdf = read_any(shp)
            geom_types = sorted(gdf.geom_type.dropna().unique().tolist())
        except Exception as e:
            add(file_rel, "", "READ", "error", f"Errore lettura: {str(e)[:180]}")
            continue

        layer = guess_layer(shp.name, schema)
        if not layer or layer not in shp_defs:
            add(file_rel, "(non MS)", "LAYER-GUESS", "warning", "Non previsto dallo schema MS strict")
            continue

        found_layers.add(layer)
        spec = shp_defs[layer]

        # GEOM TYPE (strict)
        allowed = set(spec.get("geometry", []))
        if allowed:
            ok = set(geom_types).issubset(allowed)
            add(file_rel, layer, "GEOM-TYPE", "ok" if ok else "error", f"{geom_types} vs {sorted(allowed)}")

        # FIELDS strict (case-sensitive; no alias)
        cols = list(gdf.columns)
        for f in spec.get("fields", []):
            name = f["name"]
            if f.get("required", False) and name not in cols:
                add(file_rel, layer, f"FIELD({name})", "error", "Campo obbligatorio assente (strict)")
                continue
            if name in cols:
                if "type" in f:
                    ok_t = check_types(gdf[name], f["type"])
                    add(file_rel, layer, f"FIELD-TYPE({name})", "ok" if ok_t else "error", f"atteso={f['type']}")
                if f.get("unique", False):
                    dups = gdf[name].duplicated().sum()
                    add(file_rel, layer, f"UNIQUE({name})", "ok" if dups==0 else "error", f"duplicati={dups}")
                if "enum_from" in f:
                    group = f["enum_from"].split(".")[-1]
                    enum_def = codes.get(group, {})
                    allowed_vals = set(enum_def.keys()) if isinstance(enum_def, dict) else set(enum_def if isinstance(enum_def, list) else [])
                    ser = gdf[name].dropna()
                    bad = ser[~ser.isin(allowed_vals)]
                    add(file_rel, layer, f"ENUM({name})", "ok" if bad.empty else "error",
                        f"non_ammessi={sorted(set(map(str,bad)))[:12]}")
                if "enum" in f:
                    allowed_vals = set(f["enum"])
                    ser = gdf[name].dropna()
                    bad = ser[~ser.isin(allowed_vals)]
                    add(file_rel, layer, f"ENUM({name})", "ok" if bad.empty else "error",
                        f"non_ammessi={sorted(set(map(str,bad)))[:12]}")
                if f.get("type")=="string" and "max_length" in f:
                    bad = gdf[name].dropna().astype(str).map(len) > int(f["max_length"])
                    add(file_rel, layer, f"MAX-LEN({name})", "ok" if (~bad).all() else "error",
                        f"max={f['max_length']} violazioni={int(bad.sum())}")
                if "regex" in f:
                    pat = re.compile(f["regex"])
                    ser = gdf[name].dropna().astype(str)
                    bad = ser[~ser.map(lambda x: bool(pat.fullmatch(x)))]
                    add(file_rel, layer, f"REGEX({name})", "ok" if bad.empty else "error",
                        f"non_conformi={bad.head(10).tolist()}")

        # VALID-GEOM
        try:
            invalid = ~gdf.is_valid
            add(file_rel, layer, "VALID-GEOM", "ok" if not invalid.any() else "error",
                "Tutte valide" if not invalid.any() else f"{int(invalid.sum())} non valide")
        except Exception as e:
            add(file_rel, layer, "VALID-GEOM", "warning", f"Validità non valutabile: {e}")

    # Required layers missing
    missing = required_layers - found_layers
    for m in sorted(missing):
        add("-", m, "LAYER-MISSING", "error", "Layer richiesto non presente")

    pd.DataFrame(results).to_csv(out_csv, index=False)

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Uso: python strict_validate.py <dataset_root> <schema_yaml> <codes_yaml> <out_csv>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
