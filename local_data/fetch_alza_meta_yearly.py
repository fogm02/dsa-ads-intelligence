"""
Catch-up Meta dat pro Alza.cz za celý rok — měsíční windows pro úplnou pagination.

Důvod: Pipeline data Alza.cz začínají až 2025-12-31, ale Meta API potvrzuje
že má reklamy už z ~2024 (retention 1 rok od date_stop).

Strategie: rozdělit duben 2025 — duben 2026 do měsíčních oken; pro každý měsíc
spustit collect_page(date_min, date_max) s vysokým max_pages. Tím obejdeme
jakýkoli per-query pagination limit.

Output: integrace do silver/meta/ecommerce/current.csv + lokální backup.
"""
import os, sys, json, time, datetime

sys.path.insert(0, '/Users/matfogla/dev/diplom_dev/azure_functions')
with open('/Users/matfogla/dev/diplom_dev/azure_functions/local.settings.json') as f:
    settings = json.load(f)
for k, v in settings.get("Values", {}).items():
    os.environ.setdefault(k, v)
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

from shared.meta_api import collect_page
from shared.transform_utils import flatten_meta_ad, meta_rows_to_csv, csv_to_rows, merge_meta_rows
from shared.blob_helpers import download_text, get_container_client

PAGE_ID = "70488720814"
PAGE_NAME = "Alza.cz"
SECTOR = "ecommerce"
SECTOR_NAME = "E-commerce / Retail"

# Měsíční windows od dubna 2025 do dnešního měsíce
def monthly_windows(start='2025-04-01', end=None):
    if end is None:
        end = datetime.date.today().replace(day=1)
    else:
        end = datetime.date.fromisoformat(end)
    cur = datetime.date.fromisoformat(start)
    out = []
    while cur < end:
        # poslední den měsíce
        if cur.month == 12:
            nxt = cur.replace(year=cur.year+1, month=1)
        else:
            nxt = cur.replace(month=cur.month+1)
        last = nxt - datetime.timedelta(days=1)
        out.append((cur.isoformat(), last.isoformat()))
        cur = nxt
    # plus ongoing měsíc
    today = datetime.date.today()
    out.append((end.isoformat(), today.isoformat()))
    return out


def main():
    token = os.environ.get("META_ACCESS_TOKEN", "")
    if not token:
        print("ERROR: META_ACCESS_TOKEN not set"); return

    windows = monthly_windows('2025-04-01')
    print(f"Window count: {len(windows)} (od {windows[0][0]} do {windows[-1][1]})\n")

    all_ads_by_id = {}  # dedup přes ad_id
    for i, (dmin, dmax) in enumerate(windows, 1):
        print(f"[{i}/{len(windows)}] {dmin} → {dmax}")
        t0 = time.time()
        try:
            ads, complete, _ = collect_page(
                PAGE_ID, PAGE_NAME, token,
                date_min=dmin, date_max=dmax,
                max_pages=100,
            )
            ads = ads or []
            new = 0
            for a in ads:
                aid = a.get('id')
                if aid and aid not in all_ads_by_id:
                    all_ads_by_id[aid] = a
                    new += 1
            print(f"   → {len(ads)} reklam ({new} nových po dedupu, {time.time()-t0:.0f}s, complete={complete})")
        except Exception as e:
            print(f"   ERROR: {e}")

    all_ads = list(all_ads_by_id.values())
    print(f"\nTotal unique ads: {len(all_ads)}")

    # Save raw
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = f"/tmp/alza_meta_yearly_{ts}.json"
    with open(raw_path, 'w', encoding='utf-8') as f:
        json.dump({
            "metadata": {"sector": SECTOR, "windows": windows,
                         "fetched_at": datetime.datetime.now().isoformat()},
            "ads_per_advertiser": {PAGE_NAME: all_ads},
        }, f, indent=2, ensure_ascii=False)
    print(f"Saved RAW: {raw_path}")

    # Flatten
    new_rows = []
    for ad in all_ads:
        ad_id = ad.get("id", "")
        if not ad_id:
            continue
        row = flatten_meta_ad(ad_id, ad, coverage_labels=[PAGE_NAME])
        row["sector"] = SECTOR
        row["sector_name"] = SECTOR_NAME
        new_rows.append(row)
    print(f"Flattened: {len(new_rows)} řádků")

    if not new_rows:
        return

    container = get_container_client()
    silver_path = f"silver/meta/{SECTOR}/current.csv"
    existing_text = download_text(container, silver_path)
    existing_rows = csv_to_rows(existing_text) if existing_text else []
    print(f"\nExisting silver/meta/{SECTOR}: {len(existing_rows)} řádků")

    merged_rows, new_count, updated_count = merge_meta_rows(existing_rows, new_rows)
    print(f"After merge: {len(merged_rows)} řádků (+{new_count} new, ~{updated_count} updated)")

    new_csv = meta_rows_to_csv(merged_rows)
    container.get_blob_client(silver_path).upload_blob(new_csv.encode('utf-8'), overwrite=True)
    print(f"✓ Uploaded {silver_path} ({len(merged_rows)} řádků)")


if __name__ == "__main__":
    main()
