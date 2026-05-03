"""
TikTok Bronze → Silver transformace.

Transform logika pro TikTok bronze JSON → silver CSV.
Volána z bp_bronze_dispatch.py (EventGrid blob trigger)
nebo manuálně přes admin endpoint.

Silver = vždy AKTUÁLNÍ stav sektoru (jeden soubor per sector).
Historie zůstává v bronze (timestampované JSONy).
"""

import azure.functions as func
import json
import logging

from shared.blob_helpers import get_container_client, upload_text, download_text
from shared.transform_utils import (
    flatten_ad,
    is_cz_relevant,
    rows_to_csv,
    csv_to_rows,
    merge_rows,
)


def handle_transform(blob: func.InputStream) -> None:
    """
    TikTok bronze → silver transformace.

    Flow:
    1. Načti bronze JSON z blob input
    2. Pro každou reklamu: flatten_ad() → flat dict
    3. Filtruj non-CZ reklamy
    4. Načti existující silver CSV (pokud existuje)
    5. Merge: upsert by ad_id (nové přepíšou staré)
    6. Ulož merged silver CSV do Blob
    """
    blob_name = blob.name or ""
    logging.info(f"[TRANSFORM] Trigger: {blob_name}")

    # Parsuj sector a filename z blob path
    # blob.name vrací celou cestu vč. containeru: diploma/bronze/{sector}/{filename}
    parts = blob_name.split("/")
    # Najdi index "bronze" ať je container name jakýkoliv
    try:
        bronze_idx = parts.index("bronze")
    except ValueError:
        logging.error(f"[TRANSFORM] Neočekávaný blob path (chybí 'bronze'): {blob_name}")
        return

    # Path: bronze/tiktok/{sector}/{filename}
    # bronze_idx+1 = "tiktok", bronze_idx+2 = sector, bronze_idx+3 = filename
    if len(parts) < bronze_idx + 4:
        logging.error(f"[TRANSFORM] Neočekávaný blob path: {blob_name}")
        return

    platform = parts[bronze_idx + 1]

    if platform != "tiktok":
        logging.warning(f"[TIKTOK_TRANSFORM] Neočekávaná platforma: {platform}")
        return

    sector = parts[bronze_idx + 2]
    filename = parts[bronze_idx + 3]

    if not filename.endswith(".json"):
        logging.info(f"[TRANSFORM] Přeskakuji non-JSON blob: {filename}")
        return

    # 1. Načti bronze data
    try:
        content = blob.read()
        bronze_data = json.loads(content)
    except Exception as e:
        logging.error(f"[TRANSFORM] Nelze načíst bronze JSON: {e}")
        return

    ads = bronze_data.get("ads", {})
    details = bronze_data.get("details", {})
    metadata = bronze_data.get("metadata", {})

    logging.info(
        f"[TRANSFORM] {sector}: {len(ads)} ads, {len(details)} details "
        f"(metoda: {metadata.get('collection_method', '?')})"
    )

    # 2. Flatten + CZ filter
    new_rows = []
    skipped_non_cz = 0

    for ad_id, ad_data in ads.items():
        detail = details.get(str(ad_id))

        if not is_cz_relevant(detail):
            skipped_non_cz += 1
            continue

        row = flatten_ad(ad_id, ad_data, detail)
        new_rows.append(row)

    logging.info(
        f"[TRANSFORM] Z bronze: {len(new_rows)} CZ reklam "
        f"(odfiltrováno {skipped_non_cz} non-CZ)"
    )

    if not new_rows:
        logging.warning(f"[TRANSFORM] Žádné CZ reklamy pro {sector} — nepíšu silver")
        return

    # 3. Načti existující silver CSV (merge, ne přepis)
    container = get_container_client()
    silver_path = f"silver/tiktok/{sector}/current.csv"

    existing_text = download_text(container, silver_path)
    existing_rows = csv_to_rows(existing_text)

    if existing_rows:
        logging.info(f"[TRANSFORM] Existující silver: {len(existing_rows)} řádků")

    # 4. Merge: upsert by ad_id
    merged_rows, new_count, updated_count = merge_rows(existing_rows, new_rows)

    logging.info(
        f"[TRANSFORM] Merge: {new_count} nových, {updated_count} aktualizovaných "
        f"→ celkem {len(merged_rows)} řádků"
    )

    # 5. Ulož merged silver CSV
    csv_content = rows_to_csv(merged_rows)
    upload_text(container, silver_path, csv_content)

    logging.info(f"[TRANSFORM] Silver uložen: {silver_path} ({len(merged_rows)} řádků)")
