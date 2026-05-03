"""
Jednorázový script pro fetch Lidl TikTok dat přes neoficiální Library API.

Používá:
  - library.tiktok.com/api/v1/search           (search per business_id)
  - library.tiktok.com/api/v1/items/{id}/details (detail per ad_id)

Žádný oficiální TikTok Research API token, žádná denní kvóta.

Output: /tmp/lidl_unofficial_full.json (raw + details v Official API formátu)
"""

import os
import sys
import json
import time
import random
from datetime import datetime, timezone

# Borrow tiktok_sov shared module
sys.path.insert(0, '/Users/matfogla/dev/tiktok_sov')

# Load env from tiktok_sov local.settings.json (token-related env vars used by shared module)
with open('/Users/matfogla/dev/tiktok_sov/local.settings.json') as f:
    settings = json.load(f)
for k, v in settings.get("Values", {}).items():
    os.environ[k] = v

import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

from shared.tiktok_api import library_fetch_ads, fetch_detail, normalize_library_detail


LIDL_BIZ_ID = "6876505704203551490"
LOOKBACK_DAYS = 500  # ~1.5 years to be safe

OUT_PATH = '/tmp/lidl_unofficial_full.json'


def main():
    print("=" * 60)
    print(f"Fetching Lidl TikTok data via unofficial Library API")
    print(f"  business_id: {LIDL_BIZ_ID}")
    print(f"  lookback: {LOOKBACK_DAYS} days")
    print("=" * 60)

    # ── STEP 1: search for ad_ids ──
    deadline = time.time() + 1800  # 30 min budget
    ads, offset, search_id, has_more = library_fetch_ads(
        LIDL_BIZ_ID, "Lidl",
        lookback_days=LOOKBACK_DAYS, region="CZ",
        deadline=deadline,
    )
    print(f"\nLibrary search returned {len(ads)} ads (has_more={has_more})")

    if not ads:
        print("No ads — nothing to enrich.")
        return

    # Date range
    dates = sorted([
        datetime.fromtimestamp(a.get('first_shown_date', 0) / 1000).strftime('%Y-%m-%d')
        for a in ads if a.get('first_shown_date')
    ])
    print(f"Date range: {dates[0]} → {dates[-1]}")

    # ── STEP 2: enrich each via Library Detail API ──
    print(f"\nEnriching {len(ads)} ads via Library Detail API...")
    details = {}
    failed = []
    consecutive_429 = 0
    MAX_CONSECUTIVE = 5

    for idx, ad in enumerate(ads, 1):
        ad_id = str(ad['id'])
        if idx % 5 == 1 or idx == len(ads):
            print(f"  {idx}/{len(ads)} | OK={len(details)} fail={len(failed)}")

        # Random pause to avoid rate limit
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
                print(f"  Stop: {MAX_CONSECUTIVE} consecutive failures (likely rate limit)")
                # Wait longer and reset
                print(f"  Waiting 60s before continuing...")
                time.sleep(60)
                consecutive_429 = 0
            continue

        details[ad_id] = detail
        consecutive_429 = 0

    print(f"\nEnrichment done: {len(details)} OK, {len(failed)} failed")

    # ── STEP 3: save ──
    output = {
        "metadata": {
            "sector": "ecommerce",
            "sector_name": "ecommerce",
            "collection_method": "unofficial_library_full",
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "lookback_days": LOOKBACK_DAYS,
            "business_ids": {LIDL_BIZ_ID: "Lidl"},
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
    print(f"  ads: {len(ads)}, details: {len(details)}")

    # Quick summary of CZ reach
    if details:
        sample = list(details.values())[0]
        print(f"\nSample detail structure:")
        print(f"  ad.id: {sample.get('ad', {}).get('id')}")
        print(f"  ad.reach.unique_users_seen: {sample.get('ad', {}).get('reach', {}).get('unique_users_seen')}")
        print(f"  ad.reach.unique_users_seen_by_country: {sample.get('ad', {}).get('reach', {}).get('unique_users_seen_by_country')}")
        print(f"  advertiser.business_id: {sample.get('advertiser', {}).get('business_id')}")
        print(f"  advertiser.business_name: {sample.get('advertiser', {}).get('business_name')}")
        print(f"  advertiser.paid_for_by: {sample.get('advertiser', {}).get('paid_for_by')}")


if __name__ == "__main__":
    main()
