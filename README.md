# Verifiche_studi_MS_CLE

Strumenti Python per verificare consegne **MS/CLE** (shapefile + MDB) e generare report CSV.

## Requisiti

- Python 3.10+
- Dipendenze Python in `requirements.txt`

Installazione rapida:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Struttura attesa dei dati

Metti ogni consegna in una sottocartella dentro `data/`:

```text
data/
  ComuneA/
    ...file shapefile...
    CLE/...mdb
    Indagini/...mdb
  ComuneB/
    ...
```

## Verifica completa

Esegui tutte le verifiche su tutte le consegne presenti in `data/`:

```bash
python3 run_all.py
```

Output report in `reports/<nome_consegna>/<timestamp>/`.

## Comandi utili

Solo MS:

```bash
python3 run_all.py --only ms
```

Solo CLE:

```bash
python3 run_all.py --only cle
```

Disattiva topologia:

```bash
python3 run_all.py --no-topology
```

Disattiva ID match:

```bash
python3 run_all.py --no-idmatch
```

Cartelle personalizzate:

```bash
python3 run_all.py --data-dir /percorso/consegne --reports-dir /percorso/report
```

## Nota

Se `data/` è vuota, lo script termina con il messaggio:

`Nessuna consegna trovata in ...`
