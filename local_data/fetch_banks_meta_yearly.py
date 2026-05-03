"""
Jednorázový script pro fetch UniCredit Bank a Fio banka Meta dat za celý rok.

Používá Meta Graph API /ads_archive (oficiální).
Output: integrace do silver/meta/banking/current.csv + lokální backup.
"""

import os
import sys
import json
import time
import datetime

# Add azure_functions to path
sys.path.insert(0, '/Users/matfogla/dev/diplom_dev/azure_functions')

with open('/Users/matfogla/dev/diplom_dev/azure_functions/local.settings.json') as f:
    settings = json.load(f)
for k, v in settings.get("Values", {}).items():
    os.environ.setdefault(k, v)

import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

from shared.meta_api import collect_page
from shared.transform_utils import flatten_meta_ad, meta_rows_to_csv, csv_to_rows, merge_meta_rows

# ── Config ──
TARGETS = [
    ("104333862287254", "UniCredit Bank"),
    ("107021016038575", "Fio banka"),
]
DATE_MIN = "2025-04-29"   # full year back
DATE_MAX = datetime.date.today().isoformat()
SECTOR = "banking"
SECTOR_NAME = "Bankovnictví / Finance"


def main():
    token = os.environ.get("META_ACCESS_TOKEN", "")
    if not token:
        print("ERROR: META_ACCESS_TOKEN not set")
        return

    print(f"Date range: {DATE_MIN} → {DATE_MAX}")
    print(f"Targets: {len(TARGETS)} banks\n")

    raw_responses = {}
    for page_id, page_name in TARGETS:
        print(f"=== {page_name} (page_id={page_id}) ===")
        t0 = time.time()
        try:
            ads, complete, cursor = collect_page(
                page_id, page_name, token,
                date_min=DATE_MIN,
                date_max=DATE_MAX,
                max_pages=200,  # high limit for year
            )
            ads = ads or []
            elapsed = time.time() - t0
            print(f"  → {len(ads)} ads ({elapsed:.0f}s, complete={complete})")
            raw_responses[page_name] = ads
        except Exception as e:
            print(f"  ERROR: {e}")
            raw_responses[page_name] = []

    total = sum(len(v) for v in raw_responses.values())
    print(f"\nTotal collected: {total} ads")

    # Save raw
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = f"/tmp/banks_meta_yearly_{ts}.json"
    with open(raw_path, 'w', encoding='utf-8') as f:
        json.dump({
            "metadata": {
                "sector": SECTOR,
                "date_min": DATE_MIN,
                "date_max": DATE_MAX,
                "fetched_at": datetime.datetime.now().isoformat(),
            },
            "ads_per_advertiser": raw_responses,
        }, f, indent=2, ensure_ascii=False)
    print(f"Saved RAW: {raw_path}")

    # Flatten to silver rows
    new_rows = []
    for page_name, ads in raw_responses.items():
        for ad in ads:
            ad_id = ad.get("id", "")
            if not ad_id:
                continue
            row = flatten_meta_ad(ad_id, ad, coverage_labels=[page_name])
            row["sector"] = SECTOR
            row["sector_name"] = SECTOR_NAME
            new_rows.append(row)
    print(f"Flattened: {len(new_rows)} rows")

    # Save flattened to local CSV first
    silver_path = f"/tmp/banks_meta_yearly_{ts}.csv"
    with open(silver_path, 'w', encoding='utf-8') as f:
        f.write(meta_rows_to_csv(new_rows))
    print(f"Saved silver-format: {silver_path}")

    # Now upload-merge into blob silver/meta/banking/current.csv
    from azure.storage.blob import BlobServiceClient
    conn = os.environ['BLOB_CONNECTION_STRING']
    container = BlobServiceClient.from_connection_string(conn).get_container_client("diploma")
    blob = container.get_blob_client("silver/meta/banking/current.csv")
    existing_text = blob.download_blob().readall().decode('utf-8')
    existing_rows = csv_to_rows(existing_text)
    print(f"\nExisting silver/meta/banking: {len(existing_rows)} rows")

    merged_rows, new_count, updated_count = merge_meta_rows(existing_rows, new_rows)
    print(f"After merge: {len(merged_rows)} rows (+{new_count} new, ~{updated_count} updated)")

    # Upload back
    new_csv = meta_rows_to_csv(merged_rows)
    blob.upload_blob(new_csv.encode('utf-8'), overwrite=True)
    print(f"✓ Uploaded silver/meta/banking/current.csv ({len(merged_rows)} rows)")


if __name__ == "__main__":
    main()
