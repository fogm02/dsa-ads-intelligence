# Empirical experiment: DSA reach × brand search interest

Reproduces the analysis from chapter 7.5 of the thesis — Pearson correlation between monthly DSA reach (from the pipeline's gold layer) and two independent brand interest proxies: **Google Trends** and **Wikipedia pageviews**.

Tested on 19 brands across 4 sectors (banking, FMCG retail, e-commerce, telecom) over the period April 2025 – March 2026.

## Files

| File | Purpose |
|---|---|
| `analyze_multisector.py` | Main analysis — computes Pearson correlations per brand and sector, generates panel charts |
| `fetch_wiki_pageviews.py` | Fetches Wikipedia pageviews via Wikimedia REST API |
| `generate_ecommerce_weekly_panels.py` | Generates weekly granularity panels for Příloha B |
| `manual/*.csv` | Google Trends exports (manual download from trends.google.com — Topic-level data per sector) |
| `wiki_pageviews_*.csv` | Fetched Wikipedia pageview data (monthly / weekly / daily) |
| `correlations_monthly.csv`, `correlations_weekly.csv` | Computed correlation results per brand |
| `sector_*.png` | Output panel charts referenced in chapter 7.5 |

## How to reproduce

```bash
# 1. Install dependencies
pip install pandas matplotlib scipy requests

# 2. Fetch Wikipedia data (Google Trends data is in manual/ — too small to script reliably)
python fetch_wiki_pageviews.py

# 3. Run analysis (reads manual/, wiki_pageviews_*.csv, plus DSA gold parquet from pipeline output)
python analyze_multisector.py
```

The script expects the DSA gold parquet output from the main pipeline. Without it, you can still inspect the pre-computed `correlations_*.csv` files which contain the final results presented in the thesis.
