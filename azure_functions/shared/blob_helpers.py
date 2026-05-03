"""
Blob Storage helper funkce.

Společné pro všechny blueprinty (collector, transform, reporter).
"""

import json
import io
import os
import re
import csv
import logging
import unicodedata

from azure.storage.blob import BlobServiceClient


CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME", "diploma")


def get_container_client():
    """Vrátí BlobContainerClient, vytvoří container pokud neexistuje."""
    conn_str = os.environ["BLOB_CONNECTION_STRING"]
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container = blob_service.get_container_client(CONTAINER_NAME)
    if not container.exists():
        container.create_container()
        logging.info(f"Vytvořen container '{CONTAINER_NAME}'")
    return container


def upload_json(container, blob_path, data):
    """Nahraje JSON do Blob Storage."""
    blob = container.get_blob_client(blob_path)
    content = json.dumps(data, indent=2, ensure_ascii=False, default=str)
    blob.upload_blob(content, overwrite=True)
    logging.info(f"[BLOB] Uloženo: {blob_path} ({len(content)} bytes)")


def upload_text(container, blob_path, text):
    """Nahraje textový soubor (CSV apod.) do Blob Storage."""
    blob = container.get_blob_client(blob_path)
    blob.upload_blob(text, overwrite=True)
    logging.info(f"[BLOB] Uloženo: {blob_path} ({len(text)} bytes)")


def upload_bytes(container, blob_path, data):
    """Nahraje binární data (Parquet apod.) do Blob Storage."""
    blob = container.get_blob_client(blob_path)
    blob.upload_blob(data, overwrite=True)
    logging.info(f"[BLOB] Uloženo: {blob_path} ({len(data)} bytes)")


def download_json(container, blob_path):
    """Stáhne JSON z Blob Storage. Vrátí None pokud neexistuje."""
    blob = container.get_blob_client(blob_path)
    try:
        data = blob.download_blob().readall()
        return json.loads(data)
    except Exception:
        return None


def download_text(container, blob_path):
    """Stáhne textový soubor z Blob Storage. Vrátí None pokud neexistuje."""
    blob = container.get_blob_client(blob_path)
    try:
        return blob.download_blob().readall().decode("utf-8-sig")
    except Exception:
        return None


def list_blobs(container, prefix):
    """Vrátí list jmen blobů s daným prefixem."""
    return [b.name for b in container.list_blobs(name_starts_with=prefix)]


# ============================================================
# ADVERTISERS CONFIG — CSV v Blob
# ============================================================

ADVERTISERS_BLOB_PATH = "config/tiktok_advertisers.csv"
META_ADVERTISERS_BLOB_PATH = "config/meta_advertisers.csv"
LINKEDIN_ADVERTISERS_BLOB_PATH = "config/linkedin_advertisers.csv"


def load_advertisers_from_blob(container):
    """
    Načte config/tiktok_advertisers.csv z Blob Storage.
    Formát řádků:
      sector,display_name,business_id,name,active

    display_name je volitelný — pokud chybí, použije se sector key.

    Vrátí {sector: {"display_name": str, "advertisers": {business_id_int: name}}}
    jen pro aktivní řádky (active=1).
    """
    text = download_text(container, ADVERTISERS_BLOB_PATH)
    if not text:
        logging.warning("[CONFIG] config/tiktok_advertisers.csv v Blob nenalezen")
        return {}

    result = {}
    for row in csv.DictReader(io.StringIO(text)):
        if row.get("active", "1").strip() != "1":
            continue
        sector = row["sector"].strip()
        display_name = row.get("display_name", "").strip() or sector
        bid = int(row["business_id"].strip())
        name = row["name"].strip()

        sector_cfg = result.setdefault(
            sector,
            {"display_name": display_name, "advertisers": {}},
        )
        if sector_cfg["display_name"] == sector and display_name != sector:
            sector_cfg["display_name"] = display_name

        sector_cfg["advertisers"][bid] = name

    total = sum(len(cfg["advertisers"]) for cfg in result.values())
    logging.info(f"[CONFIG] TikTok: {total} advertiserů v {len(result)} sektorech načteno z Blob")
    return result


def load_meta_advertisers_from_blob(container):
    """
    Načte config/meta_advertisers.csv z Blob Storage.
    Formát řádků:
            sector,display_name,page_id,page_name,active

        Vrátí {sector: {"display_name": str, "pages": {page_id: page_name}}}
    jen pro aktivní řádky (active=1).
    """
    text = download_text(container, META_ADVERTISERS_BLOB_PATH)
    if not text:
        logging.warning("[CONFIG] config/meta_advertisers.csv v Blob nenalezen")
        return {}

    result = {}
    for row in csv.DictReader(io.StringIO(text)):
        if row.get("active", "1").strip() != "1":
            continue

        sector = row.get("sector", "").strip()
        if not sector:
            continue

        display_name = row.get("display_name", "").strip() or sector
        sector_cfg = result.setdefault(
            sector,
            {"display_name": display_name, "pages": {}},
        )
        if sector_cfg.get("display_name", sector) == sector and display_name != sector:
            sector_cfg["display_name"] = display_name

        page_id = row.get("page_id", "").strip()
        page_name = row.get("page_name", "").strip()
        if page_id:
            sector_cfg["pages"][page_id] = page_name or page_id

    total_pages = sum(len(cfg["pages"]) for cfg in result.values())
    logging.info(
        "[CONFIG] Meta: %s sektorů, %s page IDs načteno z Blob",
        len(result),
        total_pages,
    )
    return result


def load_linkedin_advertisers_from_blob(container):
    """
    Načte config/linkedin_advertisers.csv z Blob Storage.
    Formát řádků:
      sector,display_name,advertiser_name,active

    Podporuje i starý formát se sloupcem search_term (zpětná kompatibilita).

    Vrátí {sector: {"display_name": str, "advertisers": [..], "api_names": set}}
    jen pro aktivní řádky (active=1).

    api_names = množina povolených API advertiser názvů pro daný sektor.
    Pokud je sloupec api_names prázdný, použije se advertiser_name jako fallback.
    """
    text = download_text(container, LINKEDIN_ADVERTISERS_BLOB_PATH)
    if not text:
        logging.warning("[CONFIG] config/linkedin_advertisers.csv v Blob nenalezen")
        return {}

    result = {}
    for row in csv.DictReader(io.StringIO(text)):
        if row.get("active", "1").strip() != "1":
            continue

        sector = row.get("sector", "").strip()
        if not sector:
            continue

        display_name = row.get("display_name", "").strip() or sector
        sector_cfg = result.setdefault(
            sector,
            {"display_name": display_name, "advertisers": [], "api_names": set()},
        )
        if sector_cfg.get("display_name", sector) == sector and display_name != sector:
            sector_cfg["display_name"] = display_name

        adv_name = (
            row.get("advertiser_name", "").strip()
            or row.get("search_term", "").strip()
        )
        if adv_name and adv_name not in sector_cfg["advertisers"]:
            sector_cfg["advertisers"].append(adv_name)

        # api_names whitelist — pipe-separated, case-insensitive
        raw_api = row.get("api_names", "").strip()
        if raw_api:
            for name in raw_api.split("|"):
                name = name.strip()
                if name:
                    sector_cfg["api_names"].add(name.lower())
        elif adv_name:
            # Fallback: advertiser_name jako povolený API název
            sector_cfg["api_names"].add(adv_name.lower())

    total = sum(len(cfg["advertisers"]) for cfg in result.values())
    logging.info(
        "[CONFIG] LinkedIn: %s sektorů, %s advertiserů načteno z Blob",
        len(result),
        total,
    )
    return result


def _canonical_advertiser_key(name):
    """
    Kanonizuje název pro robustní lookup: lowercase, ASCII fold,
    odstranění interpunkce a vícenásobných mezer.
    "ČESKOSLOVENSKÁ OBCHODNÍ BANKA, A. S." → "ceskoslovenska obchodni banka a s"
    """
    if not name:
        return ""
    # Unicode → ASCII (česká diakritika fold)
    folded = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    # lowercase + nahradit ne-alfanumerické znaky mezerou
    cleaned = re.sub(r"[^a-z0-9]+", " ", folded.lower())
    # collapse whitespace
    return " ".join(cleaned.split())


def load_advertiser_names_from_blob(container):
    """
    Načte config/advertiser_names.csv z Blob Storage.
    Vrátí dict {canonical_key: normalized_name} — lookup je case/diakritika/
    interpunkce insensitive (řeší TikTok uppercase varianty + různé suffixy).
    Pro zpětnou kompatibilitu obsahuje i původní raw_name jako klíč.
    """
    text = download_text(container, "config/advertiser_names.csv")
    if not text:
        logging.warning("[CONFIG] config/advertiser_names.csv nenalezen v Blob")
        return {}

    reader = csv.DictReader(io.StringIO(text))
    result = {}
    for row in reader:
        raw = row.get("raw_name", "").strip()
        normalized = row.get("normalized_name", "").strip()
        if raw and normalized:
            # exact key (zpětná kompatibilita)
            result[raw] = normalized
            # canonical key (robustní fuzzy match)
            canon = _canonical_advertiser_key(raw)
            if canon:
                result[canon] = normalized

    logging.info(
        "[CONFIG] Advertiser names: %s mapování načteno z Blob", len(result)
    )
    return result


def _save_csv_to_blob(container, local_csv_path, blob_path, label):
    """Nahraje lokální CSV soubor do Blob Storage."""
    with open(local_csv_path, encoding="utf-8-sig") as f:
        content = f.read()
    container.get_blob_client(blob_path).upload_blob(content.encode("utf-8-sig"), overwrite=True)
    logging.info(f"[CONFIG] {label} → Blob ({len(content)} bytes)")


def save_advertisers_to_blob(container, local_csv_path):
    """Nahraje lokální advertisers.csv do Blob (sync po --lookup)."""
    _save_csv_to_blob(container, local_csv_path, ADVERTISERS_BLOB_PATH, "advertisers.csv")


def save_meta_advertisers_to_blob(container, local_csv_path):
    """Nahraje lokální meta_advertisers.csv do Blob."""
    _save_csv_to_blob(container, local_csv_path, META_ADVERTISERS_BLOB_PATH, "meta_advertisers.csv")


def save_linkedin_advertisers_to_blob(container, local_csv_path):
    """Nahraje lokální linkedin_advertisers.csv do Blob."""
    _save_csv_to_blob(container, local_csv_path, LINKEDIN_ADVERTISERS_BLOB_PATH, "linkedin_advertisers.csv")


# ============================================================
# PENDING FILES — mezistupeň discover → enrich
# ============================================================

def _sanitize_name(name):
    """Převede jméno na ASCII slug pro blob path (č→c, mezery→_)."""
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", ascii_name.lower()).strip("_")


def pending_blob_path(sector, business_id, advertiser_name=""):
    """Vrátí cestu k pending blob souboru."""
    if advertiser_name:
        slug = _sanitize_name(advertiser_name)
        return f"pending/{sector}/{business_id}_{slug}.json"
    return f"pending/{sector}/{business_id}.json"


def upload_pending(container, sector, business_id, data, advertiser_name=""):
    """Uloží discovered ads pro jednoho advertisera."""
    path = pending_blob_path(sector, business_id, advertiser_name)
    upload_json(container, path, data)


def load_all_pending(container, sector):
    """
    Načte všechny pending/{sector}/*.json a sloučí do jednoho dict.
    Vrátí {ad_id_str: {"library_data": {...}, "business_id": ..., "advertiser_name": ...}}
    """
    prefix = f"pending/{sector}/"
    blob_names = list_blobs(container, prefix)
    merged = {}
    for blob_name in blob_names:
        data = download_json(container, blob_name)
        if data and "ads" in data:
            merged.update(data["ads"])
    if merged:
        logging.info(f"[PENDING] {sector}: načteno {len(merged)} ads z {len(blob_names)} souborů")
    return merged


def delete_pending_sector(container, sector):
    """Smaže všechny pending soubory pro daný sektor (po dokončení enrichment), včetně directory markeru."""
    prefix = f"pending/{sector}/"
    blob_names = list_blobs(container, prefix)
    for blob_name in blob_names:
        container.get_blob_client(blob_name).delete_blob()
    # Smaž i directory marker (0-byte blob "pending/{sector}")
    try:
        container.get_blob_client(f"pending/{sector}").delete_blob()
    except Exception:
        pass
    if blob_names:
        logging.info(f"[PENDING] Smazáno {len(blob_names)} pending souborů pro {sector}")


def delete_blob(container, blob_path):
    """Smaže jeden blob."""
    try:
        container.get_blob_client(blob_path).delete_blob()
        logging.info(f"[BLOB] Smazáno: {blob_path}")
    except Exception:
        pass
