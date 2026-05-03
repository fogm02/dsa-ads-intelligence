# Empirical experiment: DSA reach × brand search interest

Reproduces the analysis from chapter 7.5 of the thesis — Pearson correlation between monthly DSA reach (from the pipeline's gold layer) and two independent brand interest proxies: **Google Trends** and **Wikipedia pageviews**.

Tested on 19 brands across 4 sectors (banking, FMCG retail, e-commerce, telecom) over the period April 2025 – March 2026.

## Folder structure

```
trends/
├── scripts/
│   ├── analyze_multisector.py          # Main analysis (correlations + per-sector charts)
│   ├── fetch_wiki_pageviews.py         # Fetches Wikipedia pageviews via Wikimedia REST API
│   └── generate_ecommerce_weekly_panels.py   # Weekly panels for Příloha B
├── data/
│   ├── google_trends/                  # Manual Google Trends exports (Topic-level, per sector)
│   └── wikipedia/                      # Fetched Wikipedia pageviews (daily, weekly, monthly)
└── output/
    ├── correlations/                   # CSV results (per-brand and per-sector Pearson r, p-values)
    └── charts/                         # Output PNG charts referenced in chapter 7.5
```

## How to reproduce

```bash
# 1. Install dependencies
pip install pandas matplotlib scipy requests pyarrow

# 2. (Optional) Re-fetch Wikipedia data — takes ~1 min
#    (Pre-fetched data is already in data/wikipedia/)
python scripts/fetch_wiki_pageviews.py

# 3. Run analysis — needs the DSA gold parquet from the main pipeline
#    (set GOLD_DAILY_PATH env var or place at azure_functions/gold/cross_platform_daily.parquet)
python scripts/analyze_multisector.py
```

The Google Trends data in `data/google_trends/` was manually exported from [trends.google.com](https://trends.google.com) (Topic-level data per sector) and cannot be programmatically re-fetched without an unofficial scraper.

Without the DSA gold parquet from the pipeline, the correlation results in `output/correlations/` show what was used in the thesis.
