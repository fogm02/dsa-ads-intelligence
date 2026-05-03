"""
Generuje per-brand panely pro e-commerce v týdenní granularitě.
Standalone skript — vychází z analyze_multisector.py, ale jen pro 1 sektor a 1 granularitu.
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats as st

OUT_DIR = Path('/Users/matfogla/dev/diplom_dev/local_data/trends')
MANUAL_DIR = OUT_DIR / 'manual'
GOLD_DAILY = Path('/Users/matfogla/dev/diplom_dev/local_data/test_output/cross_platform_daily.parquet')
DATE_FROM = '2025-04-01'
DATE_TO = '2026-03-20'

ECOMMERCE = {
    'reach_sector': 'ecommerce',
    'brands': ['Temu', 'Notino', 'Allegro', 'Datart', 'Alza.cz'],
    'trends_csv': 'Časové řady CZ 2025-04 do 2026-04 (2).csv',
    'rename': {},
}


def load_trends_csv(filename, rename=None):
    df = pd.read_csv(MANUAL_DIR / filename).rename(columns={'Time': 'date', **(rename or {})})
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df[df.index <= DATE_TO]


def main():
    # --- Load reach daily ---
    g = pd.read_parquet(GOLD_DAILY)
    g['date'] = pd.to_datetime(g['date'])
    g['reach_daily'] = pd.to_numeric(g['reach_daily'], errors='coerce').fillna(0)
    g = g[(g['date'] >= DATE_FROM) & (g['date'] <= DATE_TO)].copy()
    g = g[g['sector'] == ECOMMERCE['reach_sector']]
    g = g[g['advertiser_normalized'].isin(ECOMMERCE['brands'])]

    # Reach: daily → weekly (W-MON, sum)
    reach_per = (g.groupby([pd.Grouper(key='date', freq='W-MON'), 'advertiser_normalized'])
                 ['reach_daily'].sum().unstack().fillna(0) / 1e6)

    # --- Trends weekly ---
    trends = load_trends_csv(ECOMMERCE['trends_csv'], ECOMMERCE['rename'])
    # Ujistit se, že je číslo
    for c in trends.columns:
        trends[c] = pd.to_numeric(trends[c], errors='coerce').fillna(0)
    trends_per = trends.resample('W-MON').mean()

    # --- Wikipedia weekly ---
    wiki = pd.read_csv(OUT_DIR / 'wiki_pageviews_weekly.csv', parse_dates=[0], index_col=0)
    wiki = wiki.resample('W-MON').mean()

    # --- Plot 5 panels ---
    n = len(ECOMMERCE['brands'])
    cols = 2
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(14, 3.2 * rows), sharex=True)
    if rows == 1:
        axes = axes.reshape(1, -1)

    for i, brand in enumerate(ECOMMERCE['brands']):
        ax = axes[i // cols, i % cols]
        if brand not in reach_per.columns:
            ax.text(0.5, 0.5, f'{brand}: chybí reach data', ha='center', va='center', transform=ax.transAxes)
            continue

        r = reach_per[brand].dropna()
        ax.plot(r.index, r.values, color='#10b981', marker='o', markersize=3, linewidth=1.5, label='Reach (mil)')
        ax.set_ylabel('Reach (mil)', color='#10b981', fontsize=9)
        ax.tick_params(axis='y', labelcolor='#10b981')

        ax2 = ax.twinx()
        rt = '?'
        if brand in trends_per.columns:
            t = trends_per[brand].dropna()
            ax2.plot(t.index, t.values, color='#f59e0b', marker='s', markersize=2.5,
                     linewidth=1.2, linestyle='--', label='Google Trends')
            common = t.index.intersection(r.index)
            if len(common) >= 5:
                rt = f'{st.pearsonr(t.loc[common], r.loc[common])[0]:+.2f}'

        rw = '?'
        if brand in wiki.columns:
            w = wiki[brand].dropna()
            if w.max() > 0:
                w_norm = w / w.max() * 100
                ax2.plot(w_norm.index, w_norm.values, color='#6366f1', marker='^', markersize=2.5,
                         linewidth=1.2, linestyle=':', label='Wikipedia (% max)')
            common = w.index.intersection(r.index)
            if len(common) >= 5:
                rw = f'{st.pearsonr(w.loc[common], r.loc[common])[0]:+.2f}'

        ax2.set_ylabel('Awareness (norm.)', color='#475569', fontsize=9)
        ax2.set_ylim(0, 110)

        ax.set_title(f'{brand}    r(Trends)={rt}, r(Wiki)={rw}', fontsize=11, fontweight='bold')
        ax.grid(alpha=0.2)

        if i == 0:
            ax.legend(loc='upper left', fontsize=8)
            ax2.legend(loc='upper right', fontsize=8)

    for j in range(n, rows * cols):
        axes[j // cols, j % cols].set_visible(False)

    plt.suptitle(f'E-commerce — reach vs Google Trends vs Wikipedia (týdenní, {DATE_FROM} – {DATE_TO})',
                 fontsize=13, fontweight='bold', y=1.005)
    plt.tight_layout()
    out = OUT_DIR / 'sector_e-commerce_panels_weekly.png'
    plt.savefig(out, dpi=130, bbox_inches='tight')
    plt.close(fig)
    print(f'Saved: {out}')


if __name__ == '__main__':
    main()
