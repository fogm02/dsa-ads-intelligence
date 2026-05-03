"""
Speciální catchup pro Temu Czechia (page_id=118177448049194).

Temu publikuje obrovské množství reklam (21k+ za měsíc), takže defaultní
max_pages=20 v catchup_meta.py vrátí maximálně 10000 ads (cap).
Tento skript zvedne max_pages na 100 (až 50000 ads).

Výstup nahradí ecommerce.csv → musíš pak znovu spustit integrate_catchup.
"""

import os
import sys
import json
import csv
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
logging.basicConfig(level=logging.INFO, format="%(message)s")

from shared.meta_api import collect_page
from shared.transform_utils import flatten_meta_ad, meta_rows_to_csv

CATCHUP_DATE_MIN = "2026-03-28"
CATCHUP_DATE_MAX = datetime.date.today().isoformat()
TEMU_PAGE_ID = "118177448049194"
TEMU_NAME = "Temu Czechia"
MAX_PAGES = 100   # vs 20 v default — Temu potřebuje víc

token = os.environ.get("META_ACCESS_TOKEN", "")
if not token:
    print("ERROR: META_ACCESS_TOKEN missing")
    sys.exit(1)

print(f"=== Temu catchup s max_pages={MAX_PAGES} ===")
print(f"DATE_MIN={CATCHUP_DATE_MIN}, DATE_MAX={CATCHUP_DATE_MAX}")
print()

t0 = time.time()
ads, complete, cursor = collect_page(
    TEMU_PAGE_ID, TEMU_NAME, token,
    date_min=CATCHUP_DATE_MIN,
    date_max=CATCHUP_DATE_MAX,
    max_pages=MAX_PAGES,
)
ads = ads or []
elapsed = time.time() - t0
print(f"Fetched {len(ads)} ads ({elapsed:.0f}s)")
print(f"Complete: {complete}, cursor: {'YES' if cursor else 'NO'}")

# Načti existující ecommerce.csv (z předchozího catchupu) a doplň o Temu data
existing_rows = []
ec_path = os.path.join(HERE, "silver", "ecommerce.csv")
if os.path.exists(ec_path):
    with open(ec_path, encoding="utf-8-sig") as f:
        existing_rows = list(csv.DictReader(f))

# Vyhoď staré Temu řádky z existujícího CSV
existing_no_temu = [r for r in existing_rows if r.get("page_name", "") != TEMU_NAME]
print(f"Existující ecommerce.csv: {len(existing_rows)} řádků (Temu: {len(existing_rows) - len(existing_no_temu)})")

# Flattnout nové Temu data
new_rows = []
for ad in ads:
    ad_id = ad.get("id", "")
    if not ad_id:
        continue
    row = flatten_meta_ad(ad_id, ad, coverage_labels=[TEMU_NAME])
    row["sector"] = "ecommerce"
    row["sector_name"] = "E-commerce / Retail"
    new_rows.append(row)
print(f"Nový Temu: {len(new_rows)} reklam")

# Spojit a uložit
all_rows = existing_no_temu + new_rows
csv_text = meta_rows_to_csv(all_rows)
with open(ec_path, "w", encoding="utf-8") as f:
    f.write(csv_text)
print(f"Uloženo: {ec_path} ({len(all_rows)} řádků total)")

# Save raw too
ts = datetime.datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
raw_path = os.path.join(HERE, "raw", f"temu_only_{ts}.json")
with open(raw_path, "w", encoding="utf-8") as f:
    json.dump({"metadata": {"advertiser": TEMU_NAME, "page_id": TEMU_PAGE_ID,
                            "date_min": CATCHUP_DATE_MIN, "date_max": CATCHUP_DATE_MAX,
                            "max_pages": MAX_PAGES, "ads_count": len(ads)},
               "ads": ads}, f, indent=2, ensure_ascii=False)
print(f"Raw: {raw_path}")

print(f"\n✅ Hotovo. Pro integraci spusť:")
print(f"   python3 catchup/integrate_catchup.py ecommerce --apply")
