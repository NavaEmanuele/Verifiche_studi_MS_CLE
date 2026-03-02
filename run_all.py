# run_all.py - launcher unico per verifiche MS/CLE (Python 3)
import argparse
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE / "scripts"
SCHEMAS = HERE / "schemas"

MS_SCHEMA = SCHEMAS / "ms_schema_4_2_strict.yaml"
MS_CODES  = SCHEMAS / "ms_codes_4_2.yaml"
MS_MDB_SCHEMA = SCHEMAS / "ms_mdb_schema_4_2.yaml"
MS_LINKMAP = SCHEMAS / "ms_linkmap_4_2.yaml"

CLE_SCHEMA = SCHEMAS / "cle_schema_3_1_strict.yaml"
CLE_CODES  = SCHEMAS / "cle_codes_3_1.yaml"
CLE_MDB_SCHEMA = SCHEMAS / "cle_mdb_schema_3_1.yaml"

def run(cmd, cwd=None):
    print(">>", " ".join(str(c) for c in cmd))
    p = subprocess.run(cmd, cwd=cwd)
    if p.returncode != 0:
        raise SystemExit(p.returncode)

def find_first(root: Path, patterns):
    for pat in patterns:
        found = list(root.rglob(pat))
        if found:
            return found[0]
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=str(HERE / "data"), help="Cartella che contiene le consegne (una sottocartella per comune)")
    ap.add_argument("--reports-dir", default=str(HERE / "reports"), help="Cartella output report")
    ap.add_argument("--only", choices=["all","ms","cle"], default="all")
    ap.add_argument("--no-topology", action="store_true")
    ap.add_argument("--no-idmatch", action="store_true")
    args = ap.parse_args()

    data_dir = Path(args.data_dir).resolve()
    reports_dir = Path(args.reports_dir).resolve()
    reports_dir.mkdir(parents=True, exist_ok=True)

    if not data_dir.exists():
        raise SystemExit(f"data-dir non trovato: {data_dir}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    datasets = [p for p in data_dir.iterdir() if p.is_dir()]
    if not datasets:
        print(f"Nessuna consegna trovata in {data_dir}")
        return 0

    for ds in datasets:
        out = reports_dir / ds.name / ts
        out.mkdir(parents=True, exist_ok=True)

        # --- MS shapefiles strict
        if args.only in ("all", "ms"):
            run([sys.executable, str(SCRIPTS / "strict_validate.py"),
                 str(ds), str(MS_SCHEMA), str(MS_CODES), str(out / "ms_strict.csv")])

        # --- CLE shapefiles strict
        if args.only in ("all", "cle"):
            run([sys.executable, str(SCRIPTS / "strict_validate.py"),
                 str(ds), str(CLE_SCHEMA), str(CLE_CODES), str(out / "cle_strict.csv")])

        # --- MDB validate (CLE)
        if args.only in ("all","cle"):
            cle_mdb = find_first(ds, ["CLE/*.mdb", "CLE_db*.mdb", "*CLE*db*.mdb"])
            if cle_mdb:
                run([sys.executable, str(SCRIPTS / "mdb_validate.py"),
                     str(cle_mdb), str(CLE_MDB_SCHEMA), str(CLE_CODES), str(out / "cle_mdb.csv")])
            else:
                (out / "cle_mdb.MISSING.txt").write_text("MDB CLE non trovato\n", encoding="utf-8")

        # --- MDB validate (MS)
        if args.only in ("all","ms"):
            ms_mdb = find_first(ds, ["Indagini/*.mdb", "*MS*db*.mdb", "*CdI*Tabelle*.mdb"])
            if ms_mdb:
                run([sys.executable, str(SCRIPTS / "mdb_validate.py"),
                     str(ms_mdb), str(MS_MDB_SCHEMA), str(MS_CODES), str(out / "ms_mdb.csv")])
            else:
                (out / "ms_mdb.MISSING.txt").write_text("MDB MS/Indagini non trovato\n", encoding="utf-8")

        # --- ID match (MS shapefile vs MDB)
        if (args.only in ("all","ms")) and (not args.no_idmatch):
            ms_mdb = find_first(ds, ["Indagini/*.mdb", "*MS*db*.mdb", "*CdI*Tabelle*.mdb"])
            if ms_mdb:
                run([sys.executable, str(SCRIPTS / "ms_id_match.py"),
                     str(ds), str(ms_mdb), str(MS_LINKMAP), str(out / "ms_id_match.csv")])
            else:
                (out / "ms_id_match.MISSING.txt").write_text("MDB non trovato: impossibile fare ID match\n", encoding="utf-8")

        # --- Topology checks (MS + CLE)
        if not args.no_topology:
            run([sys.executable, str(SCRIPTS / "topology_validation.py"),
                 str(ds), str(out / "topology.csv")])

        print(f"OK: {ds.name} -> {out}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
