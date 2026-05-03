"""
Meta Bronze → Silver transformace.

Transform logika pro Meta bronze JSON → silver CSV.
Volána z bp_bronze_dispatch.py (EventGrid blob trigger)
nebo manuálně přes admin endpoint.
"""

import azure.functions as func
import json
import logging

from shared.blob_helpers import get_container_client, upload_text, download_text
from shared.transform_utils import (
    flatten_meta_ad,
    meta_rows_to_csv,
    csv_to_rows,
    merge_meta_rows,
    extract_reach_detail_rows,
    merge_reach_detail_rows,
    reach_detail_to_csv,
)


def handle_transform(blob: func.InputStream) -> None:
    """
    Meta bronze → silver transformace.

    Flow:
    1. Načti bronze JSON (surová Meta API data)
    2. Pro každou reklamu: flatten_meta_ad() → flat dict
    3. Načti existující silver CSV (pokud existuje)
    4. Merge: upsert by ad_id
    5. Ulož merged silver CSV
    """
    blob_name = blob.name or ""
    logging.info(f"[META_TRANSFORM] Trigger: {blob_name}")

    # Parsuj sector z blob path
    parts = blob_name.split("/")
    try:
        meta_idx = parts.index("meta")
    except ValueError:
        logging.error(f"[META_TRANSFORM] Unexpected blob path (missing 'meta'): {blob_name}")
        return

    if len(parts) < meta_idx + 3:
        logging.error(f"[META_TRANSFORM] Unexpected blob path: {blob_name}")
        return

    sector = parts[meta_idx + 1]
    filename = parts[meta_idx + 2]

    if not filename.endswith(".json"):
        logging.info(f"[META_TRANSFORM] Skipping non-JSON blob: {filename}")
        return

    # 1. Načti bronze data
    try:
        content = blob.read()
        bronze_data = json.loads(content)
    except Exception as e:
        logging.error(f"[META_TRANSFORM] Cannot read bronze JSON: {e}")
        return

    ads = bronze_data.get("ads", {})
    coverage = bronze_data.get("coverage", {})
    metadata = bronze_data.get("metadata", {})

    logging.info(
        f"[META_TRANSFORM] {sector}: {len(ads)} ads "
        f"(method: {metadata.get('collection_method', '?')})"
    )

    # 2. Flatten
    new_rows = []
    new_reach_rows = []
    for ad_id, ad_data in ads.items():
        coverage_labels = coverage.get(ad_id, [])
        row = flatten_meta_ad(ad_id, ad_data, coverage_labels)
        new_rows.append(row)
        new_reach_rows.extend(extract_reach_detail_rows(ad_id, ad_data))

    logging.info(f"[META_TRANSFORM] Flattened: {len(new_rows)} rows")

    if not new_rows:
        logging.warning(f"[META_TRANSFORM] No ads for {sector} — skipping silver write")
        return

    # 3. Načti existující silver CSV
    container = get_container_client()
    silver_path = f"silver/meta/{sector}/current.csv"

    existing_text = download_text(container, silver_path)
    existing_rows = csv_to_rows(existing_text)

    if existing_rows:
        logging.info(f"[META_TRANSFORM] Existing silver: {len(existing_rows)} rows")

    # 4. Merge: upsert by ad_id
    merged_rows, new_count, updated_count = merge_meta_rows(existing_rows, new_rows)

    logging.info(
        f"[META_TRANSFORM] Merge: {new_count} new, {updated_count} updated "
        f"→ total {len(merged_rows)} rows"
    )

    # 5. Ulož merged silver CSV
    csv_content = meta_rows_to_csv(merged_rows)
    upload_text(container, silver_path, csv_content)
    logging.info(f"[META_TRANSFORM] Silver saved: {silver_path} ({len(merged_rows)} rows)")

    # 6. Reach detail — merge a ulož
    reach_detail_path = f"silver/meta/{sector}/reach_detail.csv"
    existing_reach_text = download_text(container, reach_detail_path)
    existing_reach_rows = csv_to_rows(existing_reach_text)

    merged_reach, reach_new, reach_updated = merge_reach_detail_rows(existing_reach_rows, new_reach_rows)
    reach_csv = reach_detail_to_csv(merged_reach)
    upload_text(container, reach_detail_path, reach_csv)
    logging.info(
        f"[META_TRANSFORM] Reach detail saved: {reach_detail_path} "
        f"({reach_new} new, {reach_updated} updated → {len(merged_reach)} rows)"
    )
