"""
LOKÁLNÍ catch-up Meta dat.

Sbírá reklamy z posledního měsíce pro stale Meta sektory a ukládá je
LOKÁLNĚ do catchup/raw/ a catchup/silver/. Žádný upload do Blob.

Použití:
    cd azure_functions
    python3 catchup/catchup_meta.py                # všechny stale sektory
    python3 catchup/catchup_meta.py brewing        # jen jeden sektor (test)
    python3 catchup/catchup_meta.py --list         # jen vypíše stale sektory

Po doběhnutí:
    catchup/raw/{sector}_{timestamp}.json — surová API response
    catchup/silver/{sector}.csv           — flat data v silver formátu
"""

import os
import sys
import json
import time
import datetime
from datetime import timezone

# ──── Setup paths ─────────────────────────────────────────────
HERE = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(HERE)
sys.path.insert(0, PARENT)

# Load env vars from local.settings.json
with open(os.path.join(PARENT, "local.settings.json")) as f:
    settings = json.load(f)
for k, v in settings.get("Values", {}).items():
    os.environ.setdefault(k, v)

import logging
logging.basicConfig(level=logging.INFO, format="%(message)s")

# Import shared modules
from shared.blob_helpers import (
    get_container_client,
    load_meta_advertisers_from_blob,
)
from shared.meta_api import collect_page
from shared.transform_utils import flatten_meta_ad, meta_rows_to_csv

# ──── Konfigurace catch-up ────────────────────────────────────
CATCHUP_DATE_MIN = "2026-03-28"   # 30 dní zpět
CATCHUP_DATE_MAX = datetime.date.today().isoformat()   # dnes (Meta odmítá budoucí datum)
STALE_THRESHOLD_DAYS = 14         # sektor je stale pokud bronze > 14 dní

RAW_DIR = os.path.join(HERE, "raw")
SILVER_DIR = os.path.join(HERE, "silver")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(SILVER_DIR, exist_ok=True)


# ──── Helpers ─────────────────────────────────────────────────
def find_stale_sectors(container, meta_sectors):
    """Vrátí list (sector, age_days) sektorů, kde poslední bronze je > N dní."""
    now = datetime.datetime.now(timezone.utc)
    stale = []
    for sector_key in meta_sectors:
        blobs = sorted(
            [b for b in container.list_blobs(name_starts_with=f"bronze/meta/{sector_key}/")
             if b.name.endswith(".json")],
            key=lambda b: b.last_modified, reverse=True
        )
        if not blobs:
            stale.append((sector_key, 999))
            continue
        age = (now - blobs[0].last_modified).days
        if age > STALE_THRESHOLD_DAYS:
            stale.append((sector_key, age))
    return stale


def collect_sector_local(sector_key, sector_config, token):
    """Sbírá data pro jeden sektor a ukládá lokálně. Vrací summary dict."""
    pages = sector_config.get("pages", {})
    display_name = sector_config.get("display_name", sector_key)

    print(f"\n=== {sector_key} ({display_name}) ===")
    print(f"  Advertiserů v configu: {len(pages)}")
    print(f"  DATE_MIN={CATCHUP_DATE_MIN}, DATE_MAX={CATCHUP_DATE_MAX}")

    raw_responses = {}    # {page_name: [ads...]}
    summary = {"sector": sector_key, "advertisers": {}, "total_ads": 0, "errors": []}
    t_start = time.time()

    for idx, (page_id, page_name) in enumerate(pages.items(), 1):
        print(f"  [{idx}/{len(pages)}] {page_name} (ID: {page_id})...", end=" ", flush=True)
        try:
            ads, complete, cursor = collect_page(
                page_id, page_name, token,
                date_min=CATCHUP_DATE_MIN,
                date_max=CATCHUP_DATE_MAX,
                max_pages=20,   # safety cap
            )
            ads = ads or []
            raw_responses[page_name] = ads
            summary["advertisers"][page_name] = len(ads)
            summary["total_ads"] += len(ads)
            status = "complete" if complete else f"INCOMPLETE (cursor saved)"
            print(f"{len(ads)} ads, {status}")
        except Exception as e:
            print(f"ERROR: {e}")
            summary["errors"].append({"advertiser": page_name, "error": str(e)})
            raw_responses[page_name] = []

        time.sleep(1.0)  # rate limit safety

    elapsed = time.time() - t_start
    print(f"  → Total: {summary['total_ads']} ads ({elapsed:.0f}s)")

    # Save raw API responses
    ts = datetime.datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    raw_path = os.path.join(RAW_DIR, f"{sector_key}_{ts}.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump({
            "metadata": {
                "sector": sector_key,
                "display_name": display_name,
                "date_min": CATCHUP_DATE_MIN,
                "date_max": CATCHUP_DATE_MAX,
                "fetched_at": datetime.datetime.now(timezone.utc).isoformat(),
                "ads_count": summary["total_ads"],
            },
            "ads_per_advertiser": raw_responses,
            "summary": summary,
        }, f, indent=2, ensure_ascii=False)
    print(f"  → Saved RAW: {raw_path} ({os.path.getsize(raw_path)/1024:.0f} KB)")

    # Flatten to silver-format CSV
    rows = []
    for page_name, ads in raw_responses.items():
        for ad in ads:
            ad_id = ad.get("id", "")
            if not ad_id:
                continue
            row = flatten_meta_ad(ad_id, ad, coverage_labels=[page_name])
            row["sector"] = sector_key
            row["sector_name"] = display_name
            rows.append(row)

    if rows:
        csv_path = os.path.join(SILVER_DIR, f"{sector_key}.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(meta_rows_to_csv(rows))
        print(f"  → Saved SILVER: {csv_path} ({len(rows)} rows)")
        summary["silver_path"] = csv_path

    return summary


# ──── Main ────────────────────────────────────────────────────
def main():
    args = sys.argv[1:]
    list_only = "--list" in args
    target_sector = next((a for a in args if not a.startswith("--")), None)

    container = get_container_client()
    meta_sectors = load_meta_advertisers_from_blob(container)
    if not meta_sectors:
        print("ERROR: meta_advertisers.csv v Blob nenalezen")
        return

    stale = find_stale_sectors(container, meta_sectors)
    print(f"\nStale Meta sektory ({len(stale)}, threshold > {STALE_THRESHOLD_DAYS} dní):")
    for sector, age in sorted(stale, key=lambda x: -x[1]):
        print(f"  {sector:<20} {age} dní")

    if list_only:
        return

    if target_sector:
        if target_sector not in {s for s, _ in stale}:
            print(f"\nWARNING: '{target_sector}' není mezi stale sektory.")
            print("Pokračuji s ním tak jako tak (asi explicitní test).")
        sectors_to_process = [target_sector]
    else:
        sectors_to_process = [s for s, _ in stale]

    if not sectors_to_process:
        print("\nNic k catch-up.")
        return

    token = os.environ.get("META_ACCESS_TOKEN", "")
    if not token:
        print("\nERROR: META_ACCESS_TOKEN není nastaven")
        return

    print(f"\n{'='*60}")
    print(f"Spouštím catch-up pro {len(sectors_to_process)} sektor(ů)")
    print(f"{'='*60}")

    summaries = []
    for sector_key in sectors_to_process:
        if sector_key not in meta_sectors:
            print(f"\nSKIP: {sector_key} není v configu")
            continue
        summary = collect_sector_local(sector_key, meta_sectors[sector_key], token)
        summaries.append(summary)

    # Final report
    print(f"\n{'='*60}")
    print("SHRNUTÍ CATCH-UP")
    print(f"{'='*60}")
    total_ads = sum(s["total_ads"] for s in summaries)
    total_errors = sum(len(s["errors"]) for s in summaries)
    print(f"Sektorů zpracováno: {len(summaries)}")
    print(f"Reklam celkem:      {total_ads}")
    print(f"Chyb:               {total_errors}")
    print()
    print(f"{'Sektor':<22} {'Ads':>8} {'Errors':>8}")
    for s in summaries:
        print(f"  {s['sector']:<20} {s['total_ads']:>8} {len(s['errors']):>8}")

    print(f"\nSoubory uloženy do:")
    print(f"  {RAW_DIR}/")
    print(f"  {SILVER_DIR}/")


if __name__ == "__main__":
    main()
