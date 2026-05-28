# DSA Ads Intelligence

Multi-platform Share of Voice pipeline for competitive intelligence on the Czech advertising market, built on top of advertising archives mandated by Article 39 of the EU **Digital Services Act** (Regulation 2022/2065).

This repository accompanies a master's thesis at the University of Economics in Prague (VŠE FIS, 2026).

## What it does

- Continuously collects ads from **Meta**, **TikTok** and **LinkedIn** DSA archives via their public APIs
- Normalises heterogeneous platform data into a unified analytical model
- Produces an interactive Share of Voice / competitor monitoring dashboard

The pipeline runs on **Azure Functions** with a **medallion architecture** (bronze → silver → gold) on Azure Blob Storage. Operating cost is in single-digit EUR per month thanks to the Flex Consumption plan and pay-per-execution pricing.

## Architecture

| Layer | Format | Purpose |
|---|---|---|
| **Bronze** | JSON | Raw API responses, single source of truth |
| **Silver** | CSV | Per-platform flattened, normalised |
| **Gold** | Parquet | Cross-platform unified, analytics-ready |

Six Azure Functions handle the flow:

| Function | Trigger | Role |
|---|---|---|
| `bp_tiktok_discover` | Timer 2h | TikTok ad ID discovery |
| `bp_tiktok_enrich` | Timer 2h | TikTok detail enrichment |
| `bp_meta_collect` | Timer 4h | Meta Graph API collection |
| `bp_linkedin_collect` | Timer 4h | LinkedIn Ad Library collection |
| `bp_bronze_dispatch` | EventGrid blob | Bronze → Silver transformation |
| `bp_reporter` | Timer daily | Silver → Gold cross-platform export |

## Repository layout

```
azure_functions/
  blueprints/        # Six function entry points (bp_*.py)
  shared/            # API clients, transform utilities, blob helpers
  dashboard/         # Dash Plotly app (Share of Voice + 5 other tabs)
  config/            # Advertiser CSVs (sector × platform × business ID)
  function_app.py    # Blueprint registration
  host.json
  requirements.txt
analysis/
  trends/            # Empirical experiment from chapter 7.5
                     # — DSA reach × Google Trends / Wikipedia pageviews correlation
.env.example         # Required environment variables
```

See [`analysis/trends/README.md`](analysis/trends/README.md) for details on reproducing the empirical experiment.

## Local setup

```bash
# 1. Clone and install
git clone https://github.com/fogm02/dsa-ads-intelligence.git
cd dsa-ads-intelligence
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r azure_functions/requirements.txt

# 2. Configure secrets (see .env.example for required variables)
cp azure_functions/local.settings.json.example azure_functions/local.settings.json
# Edit local.settings.json with your own tokens:
#   TIKTOK_CLIENT_KEY, TIKTOK_CLIENT_SECRET
#   META_ACCESS_TOKEN
#   LINKEDIN_ACCESS_TOKEN
#   BLOB_CONNECTION_STRING

# 3. Run locally with Azure Functions Core Tools
cd azure_functions
func start

# 4. Dashboard (separate process)
cd azure_functions/dashboard
python app.py  # → http://localhost:8050
```

## Required API access

- **Meta:** Ad Library API access via Meta for Developers (User Access Token)
- **TikTok:** Commercial Content API via TikTok for Developers (OAuth 2.0)
- **LinkedIn:** Ad Library API access (OAuth 2.0, requires approved app)
