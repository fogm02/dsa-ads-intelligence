"""
LinkedIn Bronze → Silver transformace.

Transform logika pro LinkedIn bronze JSON → silver CSV.
Volána z bp_bronze_dispatch.py (EventGrid blob trigger)
nebo manuálně přes admin endpoint.

Na rozdíl od Meta transformu:
- CZ filtrace na úrovni API (countries=CZ) — všechny ads v bronze jsou CZ relevantní
- Upsert by ad_url (ne ad_id — LinkedIn nemá numerické ID)
- Žádný reach_detail (LinkedIn nemá demografický breakdown)
"""

import azure.functions as func
import json
import logging

from shared.blob_helpers import (
    get_container_client,
    upload_text,
    download_text,
    load_linkedin_advertisers_from_blob,
)
from shared.transform_utils import (
    flatten_linkedin_ad,
    linkedin_rows_to_csv,
    csv_to_rows,
    merge_linkedin_rows,
)


def handle_transform(blob: func.InputStream) -> None:
    """
    LinkedIn bronze → silver transformace.

    Flow:
    1. Načti bronze JSON (CZ-filtrovaná data z API)
    2. Pro každou reklamu: flatten_linkedin_ad() → flat dict
    3. Načti existující silver CSV (pokud existuje)
    4. Merge: upsert by ad_url
    5. Ulož merged silver CSV
    """
    blob_name = blob.name or ""
    logging.info(f"[LINKEDIN_TRANSFORM] Trigger: {blob_name}")

    # Parsuj sector z blob path
    parts = blob_name.split("/")
    try:
        linkedin_idx = parts.index("linkedin")
    except ValueError:
        logging.error(
            f"[LINKEDIN_TRANSFORM] Unexpected blob path (missing 'linkedin'): "
            f"{blob_name}"
        )
        return

    if len(parts) < linkedin_idx + 3:
        logging.error(f"[LINKEDIN_TRANSFORM] Unexpected blob path: {blob_name}")
        return

    sector = parts[linkedin_idx + 1]
    filename = parts[linkedin_idx + 2]

    if not filename.endswith(".json"):
        logging.info(f"[LINKEDIN_TRANSFORM] Skipping non-JSON blob: {filename}")
        return

    # 1. Načti bronze data
    try:
        content = blob.read()
        bronze_data = json.loads(content)
    except Exception as e:
        logging.error(f"[LINKEDIN_TRANSFORM] Cannot read bronze JSON: {e}")
        return

    ads = bronze_data.get("ads", {})
    coverage = bronze_data.get("coverage", {})
    metadata = bronze_data.get("metadata", {})

    logging.info(
        f"[LINKEDIN_TRANSFORM] {sector}: {len(ads)} ads "
        f"(method: {metadata.get('collection_method', '?')})"
    )

    # 2. Flatten + filtrování podle api_names whitelistu
    container = get_container_client()
    sectors_config = load_linkedin_advertisers_from_blob(container)
    allowed_names = sectors_config.get(sector, {}).get("api_names", set())

    new_rows = []
    filtered_count = 0
    for ad_url, ad_data in ads.items():
        # Ověř, že advertiser_name odpovídá whitelistu
        api_adv = (
            ad_data.get("details", {})
            .get("advertiser", {})
            .get("advertiserName", "")
        )
        if allowed_names and api_adv.lower() not in allowed_names:
            filtered_count += 1
            continue

        coverage_labels = coverage.get(ad_url, [])
        row = flatten_linkedin_ad(ad_url, ad_data, coverage_labels)
        new_rows.append(row)

    logging.info(
        f"[LINKEDIN_TRANSFORM] Flattened: {len(new_rows)} rows "
        f"({filtered_count} filtered by api_names whitelist)"
    )

    if not new_rows:
        logging.warning(
            f"[LINKEDIN_TRANSFORM] No ads for {sector} — skipping silver write"
        )
        return

    # 3. Načti existující silver CSV
    silver_path = f"silver/linkedin/{sector}/current.csv"

    existing_text = download_text(container, silver_path)
    existing_rows = csv_to_rows(existing_text)

    if existing_rows:
        logging.info(
            f"[LINKEDIN_TRANSFORM] Existing silver: {len(existing_rows)} rows"
        )

    # 4. Merge: upsert by ad_url
    merged_rows, new_count, updated_count = merge_linkedin_rows(
        existing_rows, new_rows
    )

    logging.info(
        f"[LINKEDIN_TRANSFORM] Merge: {new_count} new, {updated_count} updated "
        f"→ total {len(merged_rows)} rows"
    )

    # 5. Ulož merged silver CSV
    csv_content = linkedin_rows_to_csv(merged_rows)
    upload_text(container, silver_path, csv_content)
    logging.info(
        f"[LINKEDIN_TRANSFORM] Silver saved: {silver_path} "
        f"({len(merged_rows)} rows)"
    )
