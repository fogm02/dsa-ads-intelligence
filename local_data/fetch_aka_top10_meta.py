"""
Jednorázový fetch Meta dat pro 4 zadavatele z AKA top 10 (2025) — ověření TV-centric hypotézy.
"""
import os, sys, json, time, datetime
sys.path.insert(0, '/Users/matfogla/dev/diplom_dev/azure_functions')
with open('/Users/matfogla/dev/diplom_dev/azure_functions/local.settings.json') as f:
    settings = json.load(f)
for k, v in settings.get("Values", {}).items():
    os.environ.setdefault(k, v)
import logging; logging.basicConfig(level=logging.INFO, format='%(message)s')

from shared.meta_api import collect_page
from shared.transform_utils import flatten_meta_ad, csv_to_rows, meta_rows_to_csv, merge_meta_rows
from shared.blob_helpers import download_text, get_container_client

TARGETS = [
    ("296826220595", "Sazka (Allwyn)", "betting", "Sázení / Gambling"),
    ("553892697802123", "Simply You Pharmaceuticals", "drugstore", "Drogerie / Kosmetika"),
    ("1906052399647719", "Ferrero pralinky", "qsr", "Rychlé občerstvení (QSR)"),
    ("134937073279530", "Dr.Max", "drugstore", "Drogerie / Kosmetika"),
]
DATE_MIN = "2025-04-30"
DATE_MAX = datetime.date.today().isoformat()


def main():
    token = os.environ.get("META_ACCESS_TOKEN", "")
    if not token:
        print("ERROR: META_ACCESS_TOKEN not set"); return

    print(f"Date range: {DATE_MIN} → {DATE_MAX}\n")

    # Group targets by sector for silver merge
    by_sector = {}
    raw_responses = {}

    for page_id, page_name, sector, sector_name in TARGETS:
        print(f"=== {page_name} (page_id={page_id}, sector={sector}) ===")
        t0 = time.time()
        try:
            ads, complete, cursor = collect_page(
                page_id, page_name, token,
                date_min=DATE_MIN, date_max=DATE_MAX, max_pages=200,
            )
            ads = ads or []
            print(f"  → {len(ads)} ads ({time.time() - t0:.0f}s, complete={complete})")
            raw_responses[page_name] = ads
            by_sector.setdefault(sector, []).append((page_id, page_name, sector_name, ads))
        except Exception as e:
            print(f"  ERROR: {e}")

    # Save raw
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = f"/tmp/aka_top10_meta_{ts}.json"
    with open(raw_path, 'w', encoding='utf-8') as f:
        json.dump({"metadata": {"date_min": DATE_MIN, "date_max": DATE_MAX,
                                "fetched_at": datetime.datetime.now().isoformat()},
                   "ads_per_advertiser": raw_responses}, f, indent=2, ensure_ascii=False)
    print(f"\nSaved RAW: {raw_path}")

    # Per-sector silver merge
    container = get_container_client()
    for sector, items in by_sector.items():
        print(f"\n--- Merging {sector} ---")
        new_rows = []
        for page_id, page_name, sector_name, ads in items:
            for ad in ads:
                ad_id = ad.get("id", "")
                if not ad_id: continue
                row = flatten_meta_ad(ad_id, ad, coverage_labels=[page_name])
                row["sector"] = sector
                row["sector_name"] = sector_name
                new_rows.append(row)
        print(f"  New rows: {len(new_rows)}")
        if not new_rows:
            continue

        silver_path = f"silver/meta/{sector}/current.csv"
        existing_text = download_text(container, silver_path)
        existing_rows = csv_to_rows(existing_text) if existing_text else []
        print(f"  Existing silver: {len(existing_rows)}")

        merged_rows, new_count, updated = merge_meta_rows(existing_rows, new_rows)
        print(f"  After merge: {len(merged_rows)} (+{new_count} new, ~{updated} updated)")

        new_csv = meta_rows_to_csv(merged_rows)
        container.get_blob_client(silver_path).upload_blob(new_csv.encode('utf-8'), overwrite=True)
        print(f"  ✓ Uploaded {silver_path} ({len(merged_rows)} rows)")


if __name__ == "__main__":
    main()
