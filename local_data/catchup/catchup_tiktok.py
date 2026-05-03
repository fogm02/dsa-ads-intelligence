"""
LOKÁLNÍ catch-up TikTok dat přes neoficiální Library API + oficiální Detail API.

Library API:
  - bez kvóty, bez OAuth
  - vrací ad_id + základní metadata (reach bucket, dates, status)
  - lookback v sekundách (nastaveno na 35 dní)

Detail API:
  - oficiální, OAuth, denní kvóta 1000 calls
  - pro každý ad_id stáhne plná data (targeting, reach_by_country)

Strategie:
  1. Pro každý sektor: Library API → list ad_ids za posledních 35 dní
  2. Diff s existujícím silver — najdi NOVÉ ad_ids (které tam nejsou)
  3. Detail API → enrichment jen pro NOVÉ
  4. Flatten + CZ filter → silver-format CSV
  5. Uloží do catchup/silver/{sector}_tiktok.csv

Použití:
    python3 catchup/catchup_tiktok.py                # všechny sektory
    python3 catchup/catchup_tiktok.py ecommerce      # jeden sektor
    python3 catchup/catchup_tiktok.py --list         # vypíše stale per sektor

Pozor — Detail API má kvótu 1000/den. Skript zastaví pokud by ji překročil.
"""

import os
import sys
import json
import csv
import io
import time
import datetime
from datetime import timezone

HERE = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(HERE)
sys.path.insert(0, PARENT)

with open(os.path.join(PARENT, "local.settings.json")) as f:
    settings = json.load(f)
for k, v in settings.get("Values", {}).items():
    os.environ.setdefault(k, v)

import logging
logging.basicConfig(level=logging.WARNING, format="%(message)s")

from shared.blob_helpers import (
    get_container_client,
    load_advertisers_from_blob,
    download_text,
)
from shared.tiktok_api import (
    library_fetch_ads,
    fetch_detail,
    TokenManager,
)
from shared.transform_utils import (
    flatten_ad,
    is_cz_relevant,
    rows_to_csv,
    csv_to_rows,
)

# ──── Konfigurace ─────────────────────────────────────────────
LOOKBACK_DAYS = 35       # 30 dní s rezervou (Easter buffer)
DAILY_QUOTA = 1000       # Detail API limit per day
QUOTA_BUFFER = 50        # ponechat rezervu pro normální pipeline

CATCHUP_SILVER_DIR = os.path.join(HERE, "silver")
os.makedirs(CATCHUP_SILVER_DIR, exist_ok=True)


def find_existing_ad_ids(container, sector_key):
    """Vrátí set ad_ids, které už jsou v silver pro daný sektor."""
    silver_path = f"silver/tiktok/{sector_key}/current.csv"
    text = download_text(container, silver_path)
    if not text:
        return set()
    rows = csv_to_rows(text)
    return {r.get("ad_id", "") for r in rows if r.get("ad_id")}


def catchup_sector(container, sector_key, sector_config, token_mgr,
                   max_detail_calls=200):
    """
    Catch-up pro jeden sektor.
    max_detail_calls — strop počtu Detail volání per sektor (chrání kvótu)
    """
    advertisers = sector_config.get("advertisers", {})
    print(f"\n=== {sector_key} ({len(advertisers)} advertiserů) ===")

    existing_ids = find_existing_ad_ids(container, sector_key)
    print(f"  Existující ad_ids v silver: {len(existing_ids)}")

    # Phase 1: Library API → ad_ids per advertiser
    all_library_data = {}   # {ad_id: ad_data_from_library}
    for biz_id, adv_name in advertisers.items():
        biz_id = str(biz_id)
        if not biz_id:
            continue
        print(f"  [LIB] {adv_name}...", end=" ", flush=True)
        try:
            ads, _, _, _ = library_fetch_ads(
                str(biz_id), adv_name,
                lookback_days=LOOKBACK_DAYS,
                region="CZ",
                deadline=time.time() + 300,  # 5 min per advertiser
            )
            for a in ads or []:
                aid = a.get("ad", {}).get("id") or a.get("ad_id")
                if aid:
                    all_library_data[str(aid)] = a
            print(f"{len(ads or [])} ads")
        except Exception as e:
            print(f"ERROR: {type(e).__name__}: {str(e)[:100]}")
        time.sleep(0.5)

    # Phase 2: Diff — najdi NOVÉ ad_ids
    new_ids = set(all_library_data.keys()) - existing_ids
    print(f"  Library našel celkem: {len(all_library_data)} unikátních ad_ids")
    print(f"  Z toho NOVÝCH (gap fill): {len(new_ids)}")

    if not new_ids:
        return {"sector": sector_key, "new_ids": 0, "rows": 0}

    # Phase 3: Detail API enrichment jen pro NOVÉ
    new_ids_list = sorted(new_ids)
    if len(new_ids_list) > max_detail_calls:
        print(f"  ⚠️  {len(new_ids_list)} > max_detail_calls={max_detail_calls} — beru jen prvních {max_detail_calls}")
        new_ids_list = new_ids_list[:max_detail_calls]

    rows = []
    skipped_non_cz = 0
    detail_errors = 0
    for i, ad_id in enumerate(new_ids_list, 1):
        try:
            detail = fetch_detail(token_mgr, ad_id)
            if detail is None or detail == "_RATE_LIMITED":
                detail_errors += 1
                if detail_errors >= 5:
                    print(f"  ⛔ Příliš mnoho Detail errors — zastavuji")
                    break
                continue
            if not is_cz_relevant(detail):
                skipped_non_cz += 1
                continue
            ad_data = all_library_data[ad_id]
            row = flatten_ad(ad_id, ad_data, detail)
            row["sector"] = sector_key
            row["sector_name"] = sector_config.get("display_name", sector_key)
            rows.append(row)
            if i % 50 == 0:
                print(f"  [DETAIL] {i}/{len(new_ids_list)}, +{len(rows)} CZ rows")
        except Exception as e:
            detail_errors += 1
            if detail_errors >= 5:
                print(f"  ⛔ Příliš mnoho exceptions — zastavuji ({type(e).__name__}: {str(e)[:80]})")
                break

    print(f"  Detail enrichment: {len(rows)} CZ rows (skipped non-CZ: {skipped_non_cz})")

    # Save catchup CSV
    if rows:
        out_path = os.path.join(CATCHUP_SILVER_DIR, f"{sector_key}_tiktok.csv")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(rows_to_csv(rows))
        print(f"  → Saved: {out_path}")

    return {"sector": sector_key, "new_ids": len(new_ids), "rows": len(rows),
            "skipped_non_cz": skipped_non_cz, "detail_errors": detail_errors}


def main():
    args = sys.argv[1:]
    list_only = "--list" in args
    target = next((a for a in args if not a.startswith("--")), None)

    container = get_container_client()
    tiktok_sectors = load_advertisers_from_blob(container)

    if list_only:
        print("Dostupné TikTok sektory:")
        for sk in sorted(tiktok_sectors.keys()):
            advs = tiktok_sectors[sk].get("advertisers", [])
            print(f"  {sk:<22} {len(advs)} advertiserů")
        return

    if target:
        if target not in tiktok_sectors:
            print(f"ERROR: '{target}' není v configu")
            print(f"Dostupné: {list(tiktok_sectors.keys())}")
            return
        sectors = {target: tiktok_sectors[target]}
    else:
        sectors = tiktok_sectors

    token_mgr = TokenManager()

    print(f"{'='*70}")
    print(f"  TikTok catch-up (Library API + Detail API)")
    print(f"  Lookback: {LOOKBACK_DAYS} dní")
    print(f"  Sektorů: {len(sectors)}")
    print(f"{'='*70}")

    summaries = []
    for sk, cfg in sectors.items():
        s = catchup_sector(container, sk, cfg, token_mgr)
        summaries.append(s)

    print()
    print(f"{'='*70}")
    print(f"  SOUHRN")
    print(f"{'='*70}")
    print(f"{'Sektor':<22} {'New IDs':>8} {'Rows':>8}")
    total_new = total_rows = 0
    for s in summaries:
        print(f"  {s['sector']:<20} {s['new_ids']:>8} {s['rows']:>8}")
        total_new += s["new_ids"]
        total_rows += s["rows"]
    print("-" * 70)
    print(f"  {'TOTAL':<20} {total_new:>8} {total_rows:>8}")


if __name__ == "__main__":
    main()
