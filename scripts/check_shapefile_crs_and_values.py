import os
import geopandas as gpd
import pandas as pd

# ───── CONFIGURAZIONE ─────
DATA_DIR   = "data"
REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

# Regole per i layer MS (modifica eventuali nomi o campi)
MS_CONTROLS = {
    "Instab": {
        "Livello": {"1", "2"}  # in MS1, ad esempio, i livelli ammessi
    },
    "Stab": {
        "CAT": None  # basta che esista e non sia vuoto per nessuna feature
    },
    # se hai altri shapefile MS con controlli, aggiungili qui
}

errors = []

print("\n=== Scansione shapefile CLE e MS con log di debug ===\n")

for comune in os.listdir(DATA_DIR):
    base_cle = os.path.join(DATA_DIR, comune, "CLE")
    base_ms  = os.path.join(DATA_DIR, comune, "MS")

    # 1) scandisco CLE
    if os.path.isdir(base_cle):
        for fname in os.listdir(base_cle):
            if fname.lower().endswith(".shp"):
                shp_path = os.path.join(base_cle, fname)
                shp_name = os.path.splitext(fname)[0]
                print(f"[CLE] Comune='{comune}' → rilevato shapefile: {fname}")
    else:
        print(f"[CLE] Comune='{comune}' → cartella CLE NON trovata")

    # 2) scandisco MS
    if os.path.isdir(base_ms):
        for fname in os.listdir(base_ms):
            if fname.lower().endswith(".shp"):
                shp_path = os.path.join(base_ms, fname)
                shp_name = os.path.splitext(fname)[0]
                print(f"[MS] Comune='{comune}' → rilevato shapefile: {fname}")
    else:
        print(f"[MS] Comune='{comune}' → cartella MS NON trovata")

print("\n=== Fine scansione shapefile (debug) ===\n")

# ─── Ora procedo con i controlli veri e propri ───

for comune in os.listdir(DATA_DIR):
    base_cle = os.path.join(DATA_DIR, comune, "CLE")
    base_ms  = os.path.join(DATA_DIR, comune, "MS")

    # ─── 1) Controllo CRS di ogni shapefile CLE e MS ───
    for folder, tag in [(base_cle, "CLE"), (base_ms, "MS")]:
        if not os.path.isdir(folder):
            continue

        for fname in os.listdir(folder):
            if not fname.lower().endswith(".shp"):
                continue
            shp_path = os.path.join(folder, fname)
            shp_name = os.path.splitext(fname)[0]

            try:
                gdf = gpd.read_file(shp_path)
            except Exception as e:
                errors.append({
                    "comune": comune,
                    "layer":   shp_name,
                    "check":  "CRS",
                    "message": f"Errore apertura shapefile: {e}",
                    "status": "Errore"
                })
                continue

            crs = gdf.crs
            try:
                epsg = crs.to_epsg() if crs else None
            except:
                epsg = None

            if epsg != 32633:
                errors.append({
                    "comune": comune,
                    "layer":   shp_name,
                    "check":  "CRS",
                    "message": f"SRID errato o mancante (valore rilevato: {epsg}), deve essere EPSG:32633",
                    "status": "Errore"
                })

            # ─── 2) Se layer è in MS_CONTROLS, controllo i valori dei campi ───
            if tag == "MS" and shp_name in MS_CONTROLS:
                rules = MS_CONTROLS[shp_name]
                for field, allowed in rules.items():
                    if field not in gdf.columns:
                        errors.append({
                            "comune": comune,
                            "layer":   shp_name,
                            "check":  f"Attr-{field}",
                            "message": f"Campo '{field}' mancante",
                            "status": "Errore"
                        })
                        continue

                    if allowed is None:
                        # basta che almeno una feature abbia un valore non vuoto
                        non_null = gdf[field].dropna()
                        non_empty = [v for v in non_null if str(v).strip() != ""]
                        if len(non_empty) == 0:
                            errors.append({
                                "comune": comune,
                                "layer":   shp_name,
                                "check":  f"Attr-{field}",
                                "message": f"Campo '{field}' presente ma vuoto in tutte le feature",
                                "status": "Errore"
                            })
                    else:
                        uniques = set(map(str, gdf[field].dropna().astype(str).unique()))
                        invalid = uniques - set(map(str, allowed))
                        if invalid:
                            errors.append({
                                "comune": comune,
                                "layer":   shp_name,
                                "check":  f"Valori-{field}",
                                "message": f"Valori non ammessi in '{field}': {', '.join(sorted(invalid))}",
                                "status": "Errore"
                            })

# ───── SALVATAGGIO DEL REPORT ─────
df = pd.DataFrame(errors, columns=["comune", "layer", "check", "message", "status"])
out_csv = os.path.join(REPORT_DIR, "shapefile_crs_and_values_errors.csv")
df.to_csv(out_csv, index=False)
print(f"✅ Report CRS/Value Errors salvato in: {out_csv}")
