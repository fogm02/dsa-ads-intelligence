"""
Unified EventGrid blob trigger for all bronze → silver transforms.

Azure Functions routuje všechny EventGrid blob events přes jeden webhook,
takže je potřeba jeden handler který dispatchuje podle platformy.

Individuální transform soubory (bp_tiktok_transform, bp_meta_transform,
bp_linkedin_transform) obsahují samotnou transform logiku.
"""

import azure.functions as func
import logging


bp = func.Blueprint()


@bp.blob_trigger(
    arg_name="blob",
    path="diploma/bronze/{platform}/{sector}/{filename}",
    source="EventGrid",
    connection="BLOB_CONNECTION_STRING",
)
def bronze_dispatch(blob: func.InputStream) -> None:
    """
    Univerzální dispatcher pro bronze → silver transformace.
    Parsuje platformu z blob path a deleguje na správný handler.
    """
    blob_name = blob.name or ""
    parts = blob_name.split("/")

    try:
        bronze_idx = parts.index("bronze")
        platform = parts[bronze_idx + 1]
    except (ValueError, IndexError):
        logging.error(f"[DISPATCH] Neočekávaný blob path: {blob_name}")
        return

    logging.info(f"[DISPATCH] {platform} blob: {blob_name}")

    if platform == "tiktok":
        from blueprints.bp_tiktok_transform import handle_transform
        handle_transform(blob)
    elif platform == "meta":
        from blueprints.bp_meta_transform import handle_transform
        handle_transform(blob)
    elif platform == "linkedin":
        from blueprints.bp_linkedin_transform import handle_transform
        handle_transform(blob)
    else:
        logging.warning(f"[DISPATCH] Neznámá platforma: {platform}")
