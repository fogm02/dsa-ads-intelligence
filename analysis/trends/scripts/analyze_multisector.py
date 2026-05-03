"""
Empirické testování vazby reach na vyhledávací zájem
=====================================================

Komplexní analýza pro 4 sektory × 2 awareness proxy:
  - Sektory: banking, FMCG retail, čistě e-commerce, telecom
  - Proxy 1: Google Trends (manual CSV download z trends.google.com)
  - Proxy 2: Wikipedia pageviews (oficiální Wikimedia REST API)

Skript:
  1. Načte manual CSV soubory s Google Trends Topic data
  2. Stáhne Wikipedia pageviews přes Wikimedia REST API
  3. Načte reach data z gold parquetu (cross_platform_daily.parquet)
  4. Spočítá Pearsonovu korelaci (Trends + Wiki) na měsíční I týdenní úrovni
  5. Srovná měsíční vs týdenní výsledky (sensitivity check)
  6. Vygeneruje per-sektor grafy (reach + Trends + Wiki) + souhrnný cross-sektor

Vstupy
------
- /local_data/test_output/cross_platform_daily.parquet  (gold daily reach)
- /local_data/trends/manual/*.csv                       (Google Trends Topic data)

Výstupy
-------
- /local_data/trends/wiki_pageviews_monthly.csv         (Wiki pageviews)
- /local_data/trends/correlations_monthly.csv           (Pearson, monthly)
- /local_data/trends/correlations_weekly.csv            (Pearson, weekly)
- /local_data/trends/sector_comparison.png              (4-sektor souhrn)
- /local_data/trends/sector_{name}_panels.png           (per-sektor grafy, 4 soubory)

Spuštění
--------
python analyze_multisector.py

Závislosti: pandas, scipy, matplotlib, requests, pyarrow
"""

import time
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
from scipy import stats

TRENDS_ROOT = Path(__file__).resolve().parent.parent
MANUAL_DIR = TRENDS_ROOT / 'data' / 'google_trends'
WIKI_DIR = TRENDS_ROOT / 'data' / 'wikipedia'
CORRELATIONS_DIR = TRENDS_ROOT / 'output' / 'correlations'
CHARTS_DIR = TRENDS_ROOT / 'output' / 'charts'

# Path to gold parquet from main pipeline (set via env var or override here)
import os
GOLD_DAILY = Path(os.environ.get('GOLD_DAILY_PATH', TRENDS_ROOT.parent.parent / 'azure_functions' / 'gold' / 'cross_platform_daily.parquet'))

DATE_FROM = '2025-04-01'
DATE_TO = '2026-03-20'

# ── Sektorové konfigurace ─────────────────────────────────────────────────────

SECTORS = {
    'Banking': {
        'reach_sector': 'banking',
        'brands': ['Raiffeisenbank', 'Česká spořitelna', 'ČSOB', 'Air Bank', 'mBank', 'Partners Banka'],
        'trends_csv': 'time_series_CZ_20250401-0000_20260430-2057.csv',
        'rename': {'Československá obchodní banka': 'ČSOB'},
        'color': '#ef4444',
    },
    'FMCG retail': {
        'reach_sector': 'ecommerce',
        'brands': ['Lidl', 'Albert', 'Penny', 'Kaufland', 'Tesco'],
        'trends_csv': 'Časové řady CZ 2025-04 do 2026-04 (1).csv',
        'rename': {'Penny Market': 'Penny'},
        'color': '#f59e0b',
    },
    'E-commerce': {
        'reach_sector': 'ecommerce',
        'brands': ['Temu', 'Notino', 'Allegro', 'Datart', 'Alza.cz'],
        'trends_csv': 'Časové řady CZ 2025-04 do 2026-04 (2).csv',
        'rename': {},
        'color': '#10b981',
    },
    'Telecom': {
        'reach_sector': 'telecom',
        'brands': ['T-Mobile', 'O2', 'Vodafone'],
        'trends_csv': 'Časové řady CZ 2025-04 do 2026-04.csv',
        'rename': {'O2 Czech Republic': 'O2'},
        'color': '#6366f1',
    },
}

# Wikipedia article slugy (česká Wikipedia)
WIKI_SLUGS = {
    'Raiffeisenbank': 'Raiffeisenbank',
    'Česká spořitelna': 'Česká_spořitelna',
    'ČSOB': 'Československá_obchodní_banka',
    'Air Bank': 'Air_Bank',
    'mBank': 'MBank',
    'Partners Banka': 'Partners_Banka',
    'Lidl': 'Lidl',
    'Albert': 'Albert_(obchodní_řetězec)',
    'Penny': 'Penny_Market',
    'Kaufland': 'Kaufland',
    'Tesco': 'Tesco_Stores_ČR',
    'Temu': 'Temu',
    'Notino': 'Notino',
    'Allegro': 'Allegro',
    'Datart': 'Datart',
    'Alza.cz': 'Alza.cz',
    'T-Mobile': 'T-Mobile_Czech_Republic',
    'O2': 'O2_Czech_Republic',
    'Vodafone': 'Vodafone_Czech_Republic',
}


# ── 1) Načtení Google Trends z manual CSV ─────────────────────────────────────

def load_trends_csv(filename, rename=None):
    df = pd.read_csv(MANUAL_DIR / filename).rename(columns={'Time': 'date', **(rename or {})})
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df[df.index <= DATE_TO]


# ── 2) Stažení Wikipedia pageviews ────────────────────────────────────────────

def fetch_wikipedia_pageviews(slugs, date_from='20250401', date_to='20260320'):
    """Stáhne measured pageviews z Wikimedia REST API. Bez API klíče, free."""
    api_url = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    headers = {'User-Agent': 'thesis-research/1.0'}
    results = {}
    for label, slug in slugs.items():
        url = f"{api_url}/cs.wikipedia/all-access/all-agents/{slug}/monthly/{date_from}/{date_to}"
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            df = pd.DataFrame(r.json().get('items', []))
            if not df.empty:
                df['date'] = pd.to_datetime(df['timestamp'].str[:6] + '01') + pd.offsets.MonthEnd(0)
                results[label] = df.set_index('date')['views']
                print(f'  ✓ {label}: {len(df)} měsíců')
            else:
                print(f'  ! {label}: prázdná odpověď')
        else:
            print(f'  ✗ {label}: HTTP {r.status_code}')
        time.sleep(0.4)  # zdvořilé tempo
    return pd.DataFrame(results)


# ── 3) Načtení reach dat z gold ───────────────────────────────────────────────

def load_reach():
    g = pd.read_parquet(GOLD_DAILY)
    g['date'] = pd.to_datetime(g['date'])
    g['reach_daily'] = pd.to_numeric(g['reach_daily'], errors='coerce').fillna(0)
    g = g[(g['date'] >= DATE_FROM) & (g['date'] <= DATE_TO)].copy()
    return g


# ── 4) Korelace per granularita (monthly / weekly) ────────────────────────────

def correlate(reach_series, proxy_series, label):
    """Pearson korelace. Vrátí dict s r, p, n."""
    common = reach_series.index.intersection(proxy_series.index)
    if len(common) < 5:
        return {'r': np.nan, 'p': np.nan, 'n': len(common)}
    r, p = stats.pearsonr(proxy_series.loc[common], reach_series.loc[common])
    return {'r': r, 'p': p, 'n': len(common)}


def run_correlations(gold, wiki, granularity='monthly'):
    """Spočítá per-brand i sektorový agregát pro Trends + Wiki."""
    if granularity == 'monthly':
        freq = 'ME'
    elif granularity == 'weekly':
        freq = 'W-MON'  # week starting Monday — kompatibilní s Trends weekly
    else:
        raise ValueError(granularity)

    rows_brand = []
    rows_sector = []

    for sname, cfg in SECTORS.items():
        g = gold[gold['sector'] == cfg['reach_sector']]
        g = g[g['advertiser_normalized'].isin(cfg['brands'])]
        # Reach agregát do periody (sum daily reach)
        reach_per = (g.groupby([pd.Grouper(key='date', freq=freq), 'advertiser_normalized'])
                     ['reach_daily'].sum().unstack().fillna(0) / 1e6)
        # Trends — load + agregace (mean v periodě)
        trends = load_trends_csv(cfg['trends_csv'], cfg['rename'])
        # Trends data jsou týdenní; pro weekly resample na W-MON, pro monthly na ME-mean
        if granularity == 'weekly':
            trends_per = trends.resample(freq).mean()  # už týdenní, ale align na pondělí
        else:
            trends_per = trends.resample('ME').mean()

        # Wiki: pro monthly i weekly se používá nativní granularita (monthly z fetch nebo daily→weekly sum)
        # wiki argument je už ve správné granularitě dle volajícího (viz main)
        wiki_per = wiki.resample(freq).mean() if granularity == 'monthly' else wiki

        # Per-brand korelace
        for brand in cfg['brands']:
            if brand not in reach_per.columns:
                continue
            r = reach_per[brand].dropna()
            t_res = correlate(r, trends_per[brand].dropna(), f'{brand} trends') if brand in trends_per.columns else {'r': np.nan, 'p': np.nan, 'n': 0}
            w_res = correlate(r, wiki_per[brand].dropna(), f'{brand} wiki') if brand in wiki_per.columns else {'r': np.nan, 'p': np.nan, 'n': 0}
            rows_brand.append({
                'sector': sname, 'brand': brand,
                'r_trends': t_res['r'], 'p_trends': t_res['p'],
                'r_wiki': w_res['r'], 'p_wiki': w_res['p'],
                'n': t_res['n'],
            })

        # Sektorový agregát
        reach_agg = (g.groupby(pd.Grouper(key='date', freq=freq))['reach_daily'].sum() / 1e6)
        trends_agg = trends_per[cfg['brands']].mean(axis=1, skipna=True) if all(b in trends_per.columns for b in cfg['brands']) else trends_per.mean(axis=1)
        wiki_cols = [b for b in cfg['brands'] if b in wiki_per.columns]
        wiki_agg = wiki_per[wiki_cols].sum(axis=1) if wiki_cols else None

        t_res = correlate(reach_agg, trends_agg, f'{sname} trends agg')
        w_res = correlate(reach_agg, wiki_agg, f'{sname} wiki agg') if wiki_agg is not None else {'r': np.nan, 'p': np.nan, 'n': 0}
        rows_sector.append({
            'sector': sname,
            'r_trends': t_res['r'], 'p_trends': t_res['p'],
            'r_wiki': w_res['r'], 'p_wiki': w_res['p'],
            'n': t_res['n'],
        })

    return pd.DataFrame(rows_brand), pd.DataFrame(rows_sector)


# ── 5) Vizualizace per sektor ─────────────────────────────────────────────────

def plot_sector_panels(gold, wiki, sname, cfg, granularity='monthly'):
    """Vykreslí panel s reach + Trends + Wiki line pro každou značku v sektoru."""
    g = gold[gold['sector'] == cfg['reach_sector']]
    g = g[g['advertiser_normalized'].isin(cfg['brands'])]

    if granularity == 'monthly':
        reach_per = (g.groupby([pd.Grouper(key='date', freq='ME'), 'advertiser_normalized'])
                     ['reach_daily'].sum().unstack().fillna(0) / 1e6)
        trends = load_trends_csv(cfg['trends_csv'], cfg['rename'])
        trends_per = trends.resample('ME').mean()
        wiki_per = wiki.resample('ME').mean()
    else:
        return  # weekly per-bank panel ne — moc šumu, nemá smysl

    n = len(cfg['brands'])
    cols = 2
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(14, 3.5 * rows), sharex=True)
    if rows == 1:
        axes = axes.reshape(1, -1)

    for i, brand in enumerate(cfg['brands']):
        ax = axes[i // cols, i % cols]
        if brand not in reach_per.columns:
            ax.text(0.5, 0.5, f'{brand}: chybí reach data', ha='center', va='center', transform=ax.transAxes)
            continue

        r = reach_per[brand].dropna()
        ax.plot(r.index, r.values, color='#10b981', marker='o', markersize=5, linewidth=2, label='Reach (mil)')
        ax.set_ylabel('Reach (mil)', color='#10b981', fontsize=9)
        ax.tick_params(axis='y', labelcolor='#10b981')

        ax2 = ax.twinx()
        if brand in trends_per.columns:
            t = trends_per[brand].dropna()
            ax2.plot(t.index, t.values, color='#f59e0b', marker='s', markersize=4,
                     linewidth=1.8, linestyle='--', label='Google Trends')
        if brand in wiki_per.columns:
            w = wiki_per[brand].dropna()
            # Normalizace Wiki na škálu 0-100 pro vizuální srovnání s Trends
            if w.max() > 0:
                w_norm = w / w.max() * 100
                ax2.plot(w_norm.index, w_norm.values, color='#6366f1', marker='^', markersize=4,
                         linewidth=1.8, linestyle=':', label='Wikipedia (% max)')
        ax2.set_ylabel('Awareness (norm.)', color='#475569', fontsize=9)
        ax2.set_ylim(0, 110)

        # Computer corr for title
        from scipy import stats as st
        rt = '?'
        if brand in trends_per.columns:
            common = trends_per[brand].dropna().index.intersection(r.index)
            if len(common) >= 5:
                rt = f'{st.pearsonr(trends_per[brand].loc[common], r.loc[common])[0]:+.2f}'
        rw = '?'
        if brand in wiki_per.columns:
            common = wiki_per[brand].dropna().index.intersection(r.index)
            if len(common) >= 5:
                rw = f'{st.pearsonr(wiki_per[brand].loc[common], r.loc[common])[0]:+.2f}'
        ax.set_title(f'{brand}    r(Trends)={rt}, r(Wiki)={rw}', fontsize=11, fontweight='bold')
        ax.grid(alpha=0.2)

        if i == 0:
            ax.legend(loc='upper left', fontsize=8)
            ax2.legend(loc='upper right', fontsize=8)

    # Skrýt prázdné subploty
    for j in range(n, rows * cols):
        axes[j // cols, j % cols].set_visible(False)

    plt.suptitle(f'{sname} — reach vs Google Trends vs Wikipedia (měsíční, {DATE_FROM} – {DATE_TO})',
                 fontsize=13, fontweight='bold', y=1.005)
    plt.tight_layout()
    out = CHARTS_DIR / f'sector_{sname.lower().replace(" ","_")}_panels.png'
    plt.savefig(out, dpi=130, bbox_inches='tight')
    plt.close(fig)
    print(f'  Saved: {out}')


# ── 6) Cross-sector summary chart ─────────────────────────────────────────────

def plot_summary(sector_monthly, sector_weekly):
    fig, axes = plt.subplots(1, 2, figsize=(15, 5.5))

    # Panel 1: monthly bar chart
    ax = axes[0]
    sectors = sector_monthly['sector'].tolist()
    x = np.arange(len(sectors))
    w = 0.35
    ax.bar(x - w/2, sector_monthly['r_trends'], w, label='Google Trends', color='#f59e0b')
    ax.bar(x + w/2, sector_monthly['r_wiki'], w, label='Wikipedia', color='#6366f1')
    ax.axhline(0, color='gray', linewidth=0.5)
    ax.set_xticks(x); ax.set_xticklabels(sectors)
    ax.set_ylabel('Pearson r (sektorový agregát, monthly)')
    ax.set_title('Korelace reach × awareness proxy — měsíční')
    ax.legend()
    ax.grid(alpha=0.2, axis='y')
    ax.set_ylim(-0.1, 1.0)
    for i, p in enumerate(sector_monthly['p_trends']):
        sig = '★★' if p<0.01 else ('★' if p<0.05 else '')
        ax.text(i-w/2, sector_monthly['r_trends'].iloc[i]+0.025, f'p={p:.3f}{sig}', ha='center', fontsize=8)
    for i, p in enumerate(sector_monthly['p_wiki']):
        sig = '★★' if p<0.01 else ('★' if p<0.05 else '')
        ax.text(i+w/2, sector_monthly['r_wiki'].iloc[i]+0.025, f'p={p:.3f}{sig}', ha='center', fontsize=8)

    # Panel 2: monthly vs weekly comparison
    ax = axes[1]
    ax.scatter(sector_monthly['r_trends'], sector_weekly['r_trends'], s=120, color='#f59e0b', label='Trends', alpha=0.8)
    ax.scatter(sector_monthly['r_wiki'], sector_weekly['r_wiki'], s=120, color='#6366f1', label='Wikipedia', alpha=0.8)
    for _, row in sector_monthly.iterrows():
        ax.annotate(row['sector'], (row['r_trends'], sector_weekly[sector_weekly['sector']==row['sector']]['r_trends'].values[0]),
                    xytext=(7, 0), textcoords='offset points', fontsize=9)
    ax.plot([-1,1],[-1,1], color='gray', alpha=0.4, linestyle=':')
    ax.set_xlabel('r — měsíční granularita')
    ax.set_ylabel('r — týdenní granularita')
    ax.set_title('Sensitivity check: monthly vs weekly')
    ax.legend()
    ax.grid(alpha=0.2)
    ax.set_xlim(-0.1, 1.0); ax.set_ylim(-0.1, 1.0)

    plt.tight_layout()
    out = CHARTS_DIR / 'sector_comparison.png'
    plt.savefig(out, dpi=130, bbox_inches='tight')
    plt.close(fig)
    print(f'\nSaved: {out}')


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    for d in (WIKI_DIR, CORRELATIONS_DIR, CHARTS_DIR):
        d.mkdir(parents=True, exist_ok=True)

    # 1) Wikipedia — monthly (pro monthly korelace) i weekly (z denních dat sečtených na W-MON)
    wiki_monthly_cache = WIKI_DIR / 'wiki_pageviews_monthly.csv'
    if wiki_monthly_cache.exists():
        print(f'[cache] Wiki monthly: {wiki_monthly_cache}')
        wiki_monthly = pd.read_csv(wiki_monthly_cache, parse_dates=['date'], index_col='date')
    else:
        print('Fetching Wikipedia pageviews (monthly):')
        wiki_monthly = fetch_wikipedia_pageviews(WIKI_SLUGS)
        wiki_monthly.to_csv(wiki_monthly_cache)

    # Weekly Wiki: agregace z denních dat (sum přes W-MON) — pro weekly korelace
    wiki_daily_cache = WIKI_DIR / 'wiki_pageviews_daily.csv'
    if wiki_daily_cache.exists():
        print(f'[cache] Wiki daily: {wiki_daily_cache}')
        wiki_daily = pd.read_csv(wiki_daily_cache, parse_dates=[0], index_col=0)
        wiki_weekly = wiki_daily.resample('W-MON').sum()
    else:
        print(f'WARN: {wiki_daily_cache} not found — fallback na monthly forward-fill')
        wiki_weekly = wiki_monthly.resample('ME').mean()

    # 2) Reach
    gold = load_reach()

    # 3) Korelace — monthly i weekly (každá granularita s vlastní Wiki agregací)
    print('\n[Monthly] Korelace per brand i sektor:')
    brand_m, sector_m = run_correlations(gold, wiki_monthly, 'monthly')

    print('\n[Weekly] Korelace per brand i sektor:')
    brand_w, sector_w = run_correlations(gold, wiki_weekly, 'weekly')

    brand_m.to_csv(CORRELATIONS_DIR / 'correlations_monthly.csv', index=False)
    brand_w.to_csv(CORRELATIONS_DIR / 'correlations_weekly.csv', index=False)
    sector_m.to_csv(CORRELATIONS_DIR / 'sector_monthly.csv', index=False)
    sector_w.to_csv(CORRELATIONS_DIR / 'sector_weekly.csv', index=False)

    # 4) Comparative print
    print('\n' + '=' * 95)
    print('SEKTOROVÝ AGREGÁT — Monthly vs Weekly')
    print('=' * 95)
    print(f'{"Sektor":12s} | {"M r_T":>7s} {"M p_T":>7s} | {"W r_T":>7s} {"W p_T":>7s} | {"M r_W":>7s} {"M p_W":>7s} | {"W r_W":>7s} {"W p_W":>7s}')
    print('-' * 95)
    for _, row_m in sector_m.iterrows():
        row_w = sector_w[sector_w['sector'] == row_m['sector']].iloc[0]
        print(f'{row_m["sector"]:12s} | {row_m["r_trends"]:>+7.3f} {row_m["p_trends"]:>7.3f} | '
              f'{row_w["r_trends"]:>+7.3f} {row_w["p_trends"]:>7.3f} | '
              f'{row_m["r_wiki"]:>+7.3f} {row_m["p_wiki"]:>7.3f} | '
              f'{row_w["r_wiki"]:>+7.3f} {row_w["p_wiki"]:>7.3f}')

    # 5) Per-sector vizualizace
    print('\nGenerating per-sektor panels:')
    for sname, cfg in SECTORS.items():
        plot_sector_panels(gold, wiki_monthly, sname, cfg, 'monthly')

    # 6) Summary chart
    plot_summary(sector_m, sector_w)


if __name__ == '__main__':
    main()
