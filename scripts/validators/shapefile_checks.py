
import os
import geopandas as gpd
import pandas as pd

def _load_layer(root, relpath):
    path = os.path.join(root, relpath)
    if not os.path.exists(path):
        return None, [dict(rule_id="FILE-NOT-FOUND", level="error", target=relpath, message=f"File mancante: {path}")]
    try:
        gdf = gpd.read_file(path)
        return gdf, []
    except Exception as e:
        return None, [dict(rule_id="READ-ERROR", level="error", target=relpath, message=str(e))]

def _geometry_ok(gdf, allowed):
    return gdf.geom_type.isin(allowed).all()

def check_layers(root, shp_cfg, schema):
    out = []
    layers_schema = schema.get("shapefiles", {})
    for layer_name, defn in layers_schema.items():
        relpath = shp_cfg.get(layer_name)
        required = defn.get("required", False)
        if not relpath:
            if required:
                out.append(dict(rule_id="LAYER-MISSING", level="error", target=layer_name, message="Layer richiesto non configurato"))
            continue
        gdf, errs = _load_layer(root, relpath)
        out.extend(errs)
        if gdf is None:
            continue

        # Geometry type
        allowed_geom = defn.get("geometry", [])
        if allowed_geom and not _geometry_ok(gdf, allowed_geom):
            out.append(dict(rule_id="GEOM-TYPE", level="error", target=layer_name, message=f"Geometrie non conformi: consentite {allowed_geom}"))

        # Fields
        fields = defn.get("fields", [])
        for f in fields:
            name, ftype = f["name"], f.get("type")
            if f.get("required", False) and name not in gdf.columns:
                out.append(dict(rule_id="FIELD-MISSING", level="error", target=layer_name, message=f"Campo mancante: {name}"))
                continue
            if name in gdf.columns:
                ser = gdf[name]
                if ftype == "integer" and not ser.dropna().map(lambda v: isinstance(v, (int,)) or (isinstance(v, float) and v.is_integer())).all():
                    out.append(dict(rule_id="FIELD-TYPE", level="error", target=layer_name, message=f"Tipo non intero per campo: {name}"))
                if ftype == "string" and not ser.dropna().map(lambda v: isinstance(v, str)).all():
                    out.append(dict(rule_id="FIELD-TYPE", level="error", target=layer_name, message=f"Tipo non stringa per campo: {name}"))
                if f.get("unique", False) and ser.duplicated().any():
                    out.append(dict(rule_id="FIELD-UNIQUE", level="error", target=layer_name, message=f"Valori duplicati nel campo: {name}"))
                if "enum" in f and name in gdf.columns:
                    allowed = set(f["enum"]) if isinstance(f["enum"], list) else set(f["enum"].keys())
                    bad = ser.dropna()[~ser.astype(str).isin(allowed)]
                    if len(bad) > 0:
                        out.append(dict(rule_id="FIELD-ENUM", level="error", target=layer_name, message=f"Valori fuori enum in {name}: {sorted(set(bad))[:10]}..."))
                if "regex" in f and name in gdf.columns:
                    import re
                    pat = re.compile(f["regex"])
                    bad = ser.dropna()[~ser.astype(str).map(lambda x: bool(pat.match(str(x))))]
                    if len(bad) > 0:
                        out.append(dict(rule_id="FIELD-REGEX", level="error", target=layer_name, message=f"Valori non conformi a regex in {name} (esempi: {list(map(str,bad.head(5)))})"))
                if "max_length" in f and name in gdf.columns:
                    bad = ser.dropna().map(lambda x: len(str(x)) > f["max_length"])
                    if bad.any():
                        out.append(dict(rule_id="FIELD-LEN", level="warning", target=layer_name, message=f"Stringhe oltre {f['max_length']} caratteri in {name}"))
    return out
