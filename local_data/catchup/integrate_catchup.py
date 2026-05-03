"""
Integrace lokálních catchup CSV do silver vrstvy v Blob Storage.

Pro každý sektor:
  1. Načte aktuální silver/meta/{sector}/current.csv z Blob
  2. Načte lokální catchup/silver/{sector}.csv
  3. Merge přes upsert by ad_id (stejná logika jako bp_meta_transform)
  4. Backup původního silveru lokálně
  5. Upload merged silver zpět do Blob

DEFAULT je dry-run — žádný upload. Pro skutečný upload použij --apply.

Použití:
    python3 catchup/integrate_catchup.py            # dry-run (default)
    python3 catchup/integrate_catchup.py --apply    # SKUTEČNĚ UPLOAD
    python3 catchup/integrate_catchup.py banking    # jen jeden sektor (dry-run)
    python3 catchup/integrate_catchup.py banking --apply  # jeden sektor, ostře
"""

import os
import sys
import json
import csv
import io
import datetime
from datetime import timezone

# ──── Setup paths ─────────────────────────────────────────────
HERE = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(HERE)
sys.path.insert(0, PARENT)

# Load env vars
with open(os.path.join(PARENT, "local.settings.json")) as f:
    settings = json.load(f)
for k, v in settings.get("Values", {}).items():
    os.environ.setdefault(k, v)

import logging
logging.basicConfig(level=logging.WARNING, format="%(message)s")

from shared.blob_helpers import (
    get_container_client,
    download_text,
    upload_text,
)
from shared.transform_utils import (
    csv_to_rows,
    meta_rows_to_csv,
    merge_meta_rows,
)

# ──── Configuration ──────────────────────────────────────────
CATCHUP_DIR = os.path.join(HERE, "silver")
BACKUP_DIR = os.path.join(HERE, "backup_before_integration")
os.makedirs(BACKUP_DIR, exist_ok=True)


def integrate_sector(container, sector_key, dry_run=True):
    """Integruje catchup CSV pro jeden sektor. Vrací summary dict."""
    catchup_path = os.path.join(CATCHUP_DIR, f"{sector_key}.csv")
    silver_blob_path = f"silver/meta/{sector_key}/current.csv"

    if not os.path.exists(catchup_path):
        return {"sector": sector_key, "error": "catchup CSV nenalezen"}

    # 1. Load catchup CSV
    with open(catchup_path, encoding="utf-8-sig") as f:
        catchup_text = f.read()
    catchup_rows = csv_to_rows(catchup_text)

    # 2. Load current silver from Blob
    silver_text = download_text(container, silver_blob_path)
    silver_rows = csv_to_rows(silver_text) if silver_text else []

    # 3. Backup original silver (always, even in dry-run)
    ts = datetime.datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"silver_meta_{sector_key}_{ts}.csv")
    if silver_text:
        with open(backup_path, "w", encoding="utf-8") as f:
            f.write(silver_text)

    # 4. Merge (upsert by ad_id) — stejná funkce jako produkční pipeline
    merged_rows, new_count, updated_count = merge_meta_rows(silver_rows, catchup_rows)

    # 5. Generate new CSV
    new_csv = meta_rows_to_csv(merged_rows)

    summary = {
        "sector": sector_key,
        "silver_before": len(silver_rows),
        "catchup_rows": len(catchup_rows),
        "new_added": new_count,
        "updated": updated_count,
        "silver_after": len(merged_rows),
        "size_kb": len(new_csv) / 1024,
        "backup_path": backup_path if silver_text else None,
        "uploaded": False,
    }

    if not dry_run:
        upload_text(container, silver_blob_path, new_csv)
        summary["uploaded"] = True

    return summary


def main():
    args = sys.argv[1:]
    apply_mode = "--apply" in args
    target = next((a for a in args if not a.startswith("--")), None)

    container = get_container_client()

    # Find catchup CSVs
    catchup_files = sorted([
        f.replace(".csv", "")
        for f in os.listdir(CATCHUP_DIR)
        if f.endswith(".csv")
    ])

    if target:
        if target not in catchup_files:
            print(f"ERROR: catchup CSV pro '{target}' nenalezen v {CATCHUP_DIR}/")
            print(f"Dostupné: {catchup_files}")
            return
        sectors_to_process = [target]
    else:
        sectors_to_process = catchup_files

    mode = "🔴 APPLY (skutečný upload)" if apply_mode else "🟡 DRY-RUN (žádný upload)"
    print(f"{'='*70}")
    print(f"  Integrace catchup → Blob silver")
    print(f"  Režim: {mode}")
    print(f"  Sektory: {len(sectors_to_process)}")
    print(f"{'='*70}\n")

    summaries = []
    for sector_key in sectors_to_process:
        print(f"--- {sector_key} ---")
        s = integrate_sector(container, sector_key, dry_run=not apply_mode)
        summaries.append(s)

        if "error" in s:
            print(f"  ERROR: {s['error']}")
            continue

        print(f"  Silver před:    {s['silver_before']:>6} reklam")
        print(f"  Catchup:        {s['catchup_rows']:>6} reklam")
        print(f"  → New (přibyde):    {s['new_added']:>6} reklam")
        print(f"  → Update (přepis):  {s['updated']:>6} reklam")
        print(f"  Silver po:      {s['silver_after']:>6} reklam ({s['size_kb']:.0f} KB)")
        if s["backup_path"]:
            print(f"  Backup:         {s['backup_path']}")
        if s["uploaded"]:
            print(f"  ✅ UPLOADED")
        elif apply_mode:
            print(f"  ❌ NEUPLOADED (problém?)")
        else:
            print(f"  🟡 dry-run, žádný upload")
        print()

    # Summary table
    print(f"{'='*70}")
    print(f"  SOUHRN")
    print(f"{'='*70}")
    valid = [s for s in summaries if "error" not in s]
    print(f"{'Sektor':<22} {'Před':>7} {'+New':>6} {'~Upd':>6} {'Po':>7}")
    print("-" * 70)
    total_before = total_after = total_new = total_upd = 0
    for s in valid:
        print(f"  {s['sector']:<20} {s['silver_before']:>7} "
              f"{s['new_added']:>6} {s['updated']:>6} {s['silver_after']:>7}")
        total_before += s["silver_before"]
        total_new += s["new_added"]
        total_upd += s["updated"]
        total_after += s["silver_after"]
    print("-" * 70)
    print(f"  {'TOTAL':<20} {total_before:>7} {total_new:>6} {total_upd:>6} {total_after:>7}")

    if not apply_mode:
        print(f"\n💡 Toto byl DRY-RUN. Pro skutečný upload spusť:")
        print(f"   python3 catchup/integrate_catchup.py --apply")
    else:
        print(f"\n✅ Hotovo! Silver vrstva aktualizována.")
        print(f"   Backup originálů: {BACKUP_DIR}/")
        print(f"   Pro promítnutí do dashboardu spusť gold_reporter:")
        print(f"   curl -X POST <function_url>/admin/functions/gold_reporter ...")


if __name__ == "__main__":
    main()
