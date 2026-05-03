"""
Jednorázový script pro fetch UniCredit Bank TikTok dat přes neoficiální Library API.
"""

import os
import sys
import json
import time
import random
from datetime import datetime, timezone

sys.path.insert(0, '/Users/matfogla/dev/tiktok_sov')

with open('/Users/matfogla/dev/tiktok_sov/local.settings.json') as f:
    settings = json.load(f)
for k, v in settings.get("Values", {}).items():
    os.environ[k] = v

import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

from shared.tiktok_api import library_fetch_ads, fetch_detail


UNICREDIT_BIZ_ID = "6982126610019255042"
LOOKBACK_DAYS = 500
OUT_PATH = '/tmp/unicredit_unofficial_full.json'


def main():
    print(f"Fetching UniCredit Bank TikTok via Library API")
    print(f"  business_id: {UNICREDIT_BIZ_ID}")
    print(f"  lookback: {LOOKBACK_DAYS} days\n")

    deadline = time.time() + 1800
    ads, offset, search_id, has_more = library_fetch_ads(
        UNICREDIT_BIZ_ID, "UniCredit Bank",
        lookback_days=LOOKBACK_DAYS, region="CZ",
        deadline=deadline,
    )
    print(f"\nLibrary search returned {len(ads)} ads (has_more={has_more})")

    if not ads:
        print("No ads — nothing to enrich.")
        return

    dates = sorted([
        datetime.fromtimestamp(a.get('first_shown_date', 0) / 1000).strftime('%Y-%m-%d')
        for a in ads if a.get('first_shown_date')
    ])
    print(f"Date range: {dates[0]} → {dates[-1]}")

    print(f"\nEnriching {len(ads)} ads via Library Detail API...")
    details = {}
    failed = []
    consecutive_429 = 0
    MAX_CONSECUTIVE = 5

    for idx, ad in enumerate(ads, 1):
        ad_id = str(ad['id'])
        if idx % 5 == 1 or idx == len(ads):
            print(f"  {idx}/{len(ads)} | OK={len(details)} fail={len(failed)}")

        time.sleep(0.6 + random.uniform(0, 0.5))

        try:
            detail = fetch_detail(ad_id)
        except Exception as e:
            print(f"    ERROR ad_id={ad_id}: {e}")
            failed.append(ad_id)
            continue

        if detail is None:
            failed.append(ad_id)
            consecutive_429 += 1
            if consecutive_429 >= MAX_CONSECUTIVE:
                print(f"  Stop: {MAX_CONSECUTIVE} consecutive failures")
                print(f"  Waiting 60s before continuing...")
                time.sleep(60)
                consecutive_429 = 0
            continue

        details[ad_id] = detail
        consecutive_429 = 0

    print(f"\nEnrichment done: {len(details)} OK, {len(failed)} failed")

    output = {
        "metadata": {
            "sector": "banking",
            "sector_name": "banking",
            "collection_method": "unofficial_library_full",
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "lookback_days": LOOKBACK_DAYS,
            "business_ids": {UNICREDIT_BIZ_ID: "UniCredit Bank"},
            "total_ads_unique": len(ads),
            "total_details": len(details),
            "source": {
                "ad_ids": "library.tiktok.com/api/v1/search",
                "details": "library.tiktok.com/api/v1/items/{ad_id}/details",
            },
        },
        "ads": {str(a['id']): a for a in ads},
        "details": details,
    }

    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\nSaved: {OUT_PATH}")


if __name__ == "__main__":
    main()
