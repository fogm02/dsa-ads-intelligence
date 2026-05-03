"""
Stažení Wikipedia pageviews přes oficiální Wikimedia Analytics API.

API dokumentace
---------------
Reference: https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html
Examples:  https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/examples/page-metrics.html

Endpoint
--------
GET https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/
        {project}/{access}/{agent}/{article}/{granularity}/{start}/{end}

Parametry:
  project     — např. cs.wikipedia, en.wikipedia
  access      — desktop | mobile-app | mobile-web | all-access
  agent       — user | spider | automated | all-agents
  article     — URL-encoded nadpis článku (mezery -> "_")
  granularity — daily | monthly
  start, end  — YYYYMMDD (případně YYYYMMDDHH)

Limity
------
- Bez API klíče, ale dodržujte rozumné rate limits (~1 req/s).
- Při HTTP 429 čekat ~60s a pokračovat.
- API podporuje jen daily a monthly. Pro weekly analýzu daily fetch a agregovat.

Použití
-------
python fetch_wiki_pageviews.py
    Stáhne pro definovaný seznam značek monthly i daily pageviews
    a uloží CSV do /local_data/trends/.
"""

import time
from pathlib import Path

import pandas as pd
import requests

OUT_DIR = Path(__file__).resolve().parent.parent / 'data' / 'wikipedia'
OUT_DIR.mkdir(parents=True, exist_ok=True)
WIKI_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
HEADERS = {'User-Agent': 'thesis-research/1.0 (Wikipedia pageviews analysis)'}

DATE_FROM = '20250401'
DATE_TO = '20260320'

# Mapping značky -> Wikipedia article slug (česká Wikipedia)
ARTICLES = {
    # Banking
    'Raiffeisenbank': 'Raiffeisenbank',
    'Česká spořitelna': 'Česká_spořitelna',
    'ČSOB': 'Československá_obchodní_banka',
    'Air Bank': 'Air_Bank',
    'mBank': 'MBank',
    'Partners Banka': 'Partners_Banka',
    # FMCG retail
    'Lidl': 'Lidl',
    'Albert': 'Albert_(obchodní_řetězec)',
    'Penny': 'Penny_Market',
    'Kaufland': 'Kaufland',
    'Tesco': 'Tesco_Stores_ČR',
    # E-commerce
    'Temu': 'Temu',
    'Notino': 'Notino',
    'Allegro': 'Allegro',
    'Datart': 'Datart',
    'Alza.cz': 'Alza.cz',
    # Telecom
    'T-Mobile': 'T-Mobile_Czech_Republic',
    'O2': 'O2_Czech_Republic',
    'Vodafone': 'Vodafone_Czech_Republic',
}


def fetch_pageviews(slug, granularity, project='cs.wikipedia',
                    start=DATE_FROM, end=DATE_TO,
                    sleep_ok=0.5, sleep_429=60):
    """
    Stáhne pageviews pro jednu značku v dané granularitě.

    Returns
    -------
    pd.Series indexovaná datem (datetime), hodnoty = počet pageviews.
    None v případě selhání po retry.
    """
    url = f"{WIKI_API}/{project}/all-access/all-agents/{slug}/{granularity}/{start}/{end}"
    for attempt in range(3):
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            df = pd.DataFrame(r.json().get('items', []))
            if df.empty:
                return None
            # Timestamp formát: 'YYYYMMDDHH' pro daily/hourly, 'YYYYMM0100' pro monthly
            if granularity == 'monthly':
                df['date'] = pd.to_datetime(df['timestamp'].str[:6] + '01') + pd.offsets.MonthEnd(0)
            else:  # daily
                df['date'] = pd.to_datetime(df['timestamp'].str[:8])
            time.sleep(sleep_ok)
            return df.set_index('date')['views']
        if r.status_code == 429:
            print(f'    HTTP 429 rate limit, čekám {sleep_429}s...', flush=True)
            time.sleep(sleep_429)
            continue
        # jiná chyba — neopakovat
        print(f'    HTTP {r.status_code} (slug={slug})')
        return None
    return None


def fetch_all(articles, granularity, out_filename):
    """Stáhne pageviews pro všechny značky a uloží CSV."""
    print(f'\nFetching {granularity} pageviews ({len(articles)} značek)...')
    results = {}
    for label, slug in articles.items():
        print(f'  {label} ({slug})...', end=' ', flush=True)
        s = fetch_pageviews(slug, granularity)
        if s is not None:
            results[label] = s
            print(f'{len(s)} bodů')
        else:
            print('FAIL')

    out = OUT_DIR / out_filename
    df = pd.DataFrame(results)
    df.to_csv(out)
    print(f'  Saved: {out}')
    return df


def aggregate_daily_to_weekly(daily_csv_path, out_filename='wiki_pageviews_weekly.csv'):
    """Načte daily CSV a vytvoří weekly agregaci (W-MON, kompatibilní s Google Trends)."""
    df = pd.read_csv(daily_csv_path, parse_dates=[0], index_col=0)
    df_weekly = df.resample('W-MON').sum()
    out = OUT_DIR / out_filename
    df_weekly.to_csv(out)
    print(f'\nWeekly aggregation: {df_weekly.shape} -> {out}')
    return df_weekly


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Monthly
    fetch_all(ARTICLES, 'monthly', 'wiki_pageviews_monthly.csv')

    # Daily (~5x víc requests, potřeba víc času — pause mezi requesty)
    fetch_all(ARTICLES, 'daily', 'wiki_pageviews_daily.csv')

    # Weekly aggregace z daily
    aggregate_daily_to_weekly(OUT_DIR / 'wiki_pageviews_daily.csv')


if __name__ == '__main__':
    main()
