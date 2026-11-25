# Citi Bike NYC — Live Operations BI

Python pipeline → GitHub Actions → Power BI. Full documentation (architecture, data
dictionary, DAX measures) is completed in the final phase.

## Branches
| Branch | Contents |
|---|---|
| `main` | code: `pipeline/`, `tests/`, `.github/workflows/`, `powerbi/` |
| `data` | generated outputs only (`data/clean/*.parquet`, maps). Force-pushed by every run, no history |

Power BI reads: `https://raw.githubusercontent.com/<owner>/citibike-bi/data/data/clean/<table>.parquet`

## Run locally
```powershell
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python pipeline\run.py            # full: live snapshot + latest trip month + model
.venv\Scripts\python pipeline\run.py --mode live
.venv\Scripts\pytest
```

## Schedule (GitHub Actions, UTC)
- every 30 min: live GBFS snapshot (`--mode live`)
- 5th of each month 09:00: trip refresh (`--mode trips`)
- manual: Actions → citibike-pipeline → Run workflow
