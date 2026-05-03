"""
Blueprint: TikTok Enrichment — Detail API enrichment.

Timer trigger kazdych 2h (:30, offset od discovery).
Cte discovered ads z pending/{sector}/, enrichuje pres Detail API,
uklada bronze/{sector}/{timestamp}.json → triggeruje transform.

Zpracovava batch ~300 ads za beh (9 min budget).
Sleduje enriched IDs aby se neopakovaly.
"""

import azure.functions as func
import logging
import os
import time
from datetime import datetime

from shared.tiktok_api import (
    TokenManager,
    health_check,
    enrich_ads,
    build_bronze_batch,
)
from shared.blob_helpers import (
    get_container_client,
    upload_json,
    download_json,
    list_blobs,
    load_all_pending,
    delete_pending_sector,
    load_advertisers_from_blob,
)


bp = func.Blueprint()

ENRICHMENT_STATE_PATH = "state/enrichment.json"
DISCOVERY_STATE_PATH = "state/discovery.json"

# Deadline: 29 min z 30 min Flex Consumption timeout
DEADLINE_BUDGET_S = 1740


# ============================================================
# STATE MANAGEMENT
# ============================================================

def load_enrichment_state(container):
    """Nacte enrichment state z Blob."""
    state = download_json(container, ENRICHMENT_STATE_PATH)
    if state and state.get("version") == 2:
        if "last_cycle_id" not in state:
            state["last_cycle_id"] = ""
        return state

    return {
        "version": 2,
        "last_cycle_id": "",
        "sectors": {},
        "stats": {
            "total_runs": 0,
            "total_ads_enriched": 0,
            "total_bronze_files": 0,
        },
    }


def save_enrichment_state(container, state):
    """Ulozi enrichment state."""
    upload_json(container, ENRICHMENT_STATE_PATH, state)


def check_and_reset_on_new_cycle(enrich_state, container):
    """
    Zjisti aktualni cycle_id z discovery state.
    Pokud se lisi od last_cycle_id → vymaže enriched_ad_ids pro vsechny sektory,
    aby se reklamy aktivni v novem cyklu znovu enrichly (aktualizace reach_cz).
    """
    discovery_state = download_json(container, DISCOVERY_STATE_PATH)
    if not discovery_state:
        return

    current_cycle_id = discovery_state.get("cycle_id", "")
    last_cycle_id = enrich_state.get("last_cycle_id", "")

    if current_cycle_id and current_cycle_id != last_cycle_id:
        logging.info(
            f"[ENRICH] Novy cyklus detekovan ({last_cycle_id} → {current_cycle_id}) "
            f"— mazu enriched_ad_ids pro re-enrich (aktualizace reach_cz)"
        )
        for sector_data in enrich_state["sectors"].values():
            sector_data["enriched_ad_ids"] = []
            sector_data["enriched_count"] = 0
            if sector_data["status"] == "complete":
                sector_data["status"] = "waiting"
        enrich_state["last_cycle_id"] = current_cycle_id


def get_sector_enrichment(state, sector_key):
    """Vrati enrichment stav sektoru, vytvori defaultni pokud neexistuje."""
    if sector_key not in state["sectors"]:
        state["sectors"][sector_key] = {
            "status": "waiting",
            "enriched_ad_ids": [],
            "enriched_count": 0,
            "total_pending": 0,
            "failed_count": 0,
            "bronze_files": [],
            "last_enriched_at": "",  # ISO timestamp posledniho uspesneho enrichmentu
        }
    # Backfill pro existujici zaznamy bez tohoto pole
    state["sectors"][sector_key].setdefault("last_enriched_at", "")
    return state["sectors"][sector_key]


# ============================================================
# TIMER TRIGGER — kazdych 8h (offset :30 od Discovery)
# ============================================================

@bp.timer_trigger(
    schedule="0 30 */8 * * *",    # Každých 8h v :30 (offset od Discovery :00)
    arg_name="timer",
    run_on_startup=False,
)
def tiktok_enrich(timer: func.TimerRequest) -> None:
    """
    Enrichment funkce — Detail API enrichment.

    Flow:
    1. Health check (Library API + Detail API + token)
    2. Najdi sektor kde discovery = complete a enrichment != complete
    3. Nacti pending ads, odecti uz enriched
    4. Detail API pro batch
    5. Sestav bronze JSON → upload (trigger transform)
    6. Update enrichment state
    """
    t_start = time.time()
    deadline = t_start + DEADLINE_BUDGET_S
    logging.info("=" * 60)
    logging.info("TikTok ENRICHMENT — START")
    logging.info(f"Cas: {datetime.utcnow().isoformat()}")

    if timer.past_due:
        logging.warning("Timer je zpozdeny (past due)")

    # 1. Token + health check
    try:
        token_mgr = TokenManager()
    except Exception as e:
        logging.error(f"Nelze ziskat token: {e}")
        return

    if not health_check(token_mgr):
        logging.warning("API nedostupne — koncim")
        return

    # 2. Container + state
    container = get_container_client()
    enrich_state = load_enrichment_state(container)
    check_and_reset_on_new_cycle(enrich_state, container)

    # 3. Najdi sektory s pending soubory (nezavisle na discovery stavu)
    pending_blobs = list_blobs(container, "pending/")
    sector_blob_counts = {}
    for blob_name in pending_blobs:
        parts = blob_name.split("/")
        if len(parts) >= 3:
            sector = parts[1]
            sector_blob_counts[sector] = sector_blob_counts.get(sector, 0) + 1

    if not sector_blob_counts:
        logging.info("Zadne pending soubory — neni co enrichovat")
        enrich_state["stats"]["total_runs"] += 1
        save_enrichment_state(container, enrich_state)
        return

    # Vyber sektor ktery nejdele cekal (starvation-free scheduling).
    # "" (nikdy neenrichovan) je mensi nez jakykoli ISO timestamp → dostane prioritu.
    for sector_key in sector_blob_counts:
        enr_data = get_sector_enrichment(enrich_state, sector_key)
        if enr_data["status"] == "complete":
            # Pending soubory existuji pro "complete" sektor → nove ads, reopen
            logging.info(f"[ENRICH] {sector_key}: nove pending po completion — reopen")
            enr_data["status"] = "waiting"

    target_sector = min(
        sector_blob_counts,
        key=lambda s: get_sector_enrichment(enrich_state, s).get("last_enriched_at", ""),
    )

    # Display name z CSV (jen pro logging)
    tiktok_sectors = load_advertisers_from_blob(container)
    sector_name = tiktok_sectors.get(target_sector, {}).get("display_name", target_sector)
    sector_enr = get_sector_enrichment(enrich_state, target_sector)
    pending_count = sector_blob_counts[target_sector]
    logging.info(f"[ENRICH] Sektor: {sector_name} ({pending_count} pending souboru, "
                 f"last_enriched={sector_enr['last_enriched_at'] or 'nikdy'})")

    # 4. Nacti vsechny pending ads pro tento sektor
    all_pending_ads = load_all_pending(container, target_sector)

    if not all_pending_ads:
        logging.warning(f"[ENRICH] Zadne pending ads pro {sector_name} — oznacuji complete")
        sector_enr["status"] = "complete"
        enrich_state["stats"]["total_runs"] += 1
        save_enrichment_state(container, enrich_state)
        return

    # 5. Odecti uz enriched IDs
    enriched_set = set(sector_enr.get("enriched_ad_ids", []))
    pending_ads = {
        ad_id: data for ad_id, data in all_pending_ads.items()
        if ad_id not in enriched_set
    }

    if not pending_ads:
        logging.info(f"[ENRICH] Vsechny ads uz enriched pro {sector_name} — COMPLETE")
        sector_enr["status"] = "complete"
        sector_enr["total_pending"] = 0
        enrich_state["stats"]["total_runs"] += 1
        save_enrichment_state(container, enrich_state)

        # Cleanup pending files
        delete_pending_sector(container, target_sector)
        return

    logging.info(
        f"[ENRICH] {len(pending_ads)} pending, {len(enriched_set)} uz enriched, "
        f"{len(all_pending_ads)} celkem"
    )

    # 6. Detail API enrichment pro batch
    sector_enr["status"] = "in_progress"

    details, permanently_failed = enrich_ads(
        token_mgr,
        pending_ads,
        deadline=deadline,
        max_consecutive_429=3,
    )

    # 7. Sestav bronze JSON a upload
    if details:
        # Bronze obsahuje JEN enrichovane ads z tohoto batche
        batch_ads = {ad_id: pending_ads[ad_id] for ad_id in details if ad_id in pending_ads}

        # Nacti lookback z discovery state (pokud existuje)
        discovery_state = download_json(container, DISCOVERY_STATE_PATH)
        lookback = discovery_state.get("lookback_days", 365) if discovery_state else 365

        bronze_data = build_bronze_batch(
            sector_key=target_sector,
            sector_name=sector_name,
            batch_ads_dict=batch_ads,
            batch_details=details,
            lookback_days=lookback,
        )

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        blob_path = f"bronze/tiktok/{target_sector}/{ts}.json"
        upload_json(container, blob_path, bronze_data)
        logging.info(f"[ENRICH] Bronze ulozen: {blob_path} ({len(details)} detailu)")

        # Update enrichment state
        newly_enriched = list(details.keys())
        sector_enr["enriched_ad_ids"].extend(newly_enriched)
        enrich_state["stats"]["total_ads_enriched"] += len(newly_enriched)
        enrich_state["stats"]["total_bronze_files"] += 1
        sector_enr["bronze_files"].append(blob_path)
    else:
        logging.warning(f"[ENRICH] Zadne detaily ziskany (rate limit nebo chyba)")
        sector_enr["failed_count"] += 1

    # Trvale selhane ads pridej do enriched setu (nebudou se opakovane zkousel)
    if permanently_failed:
        logging.warning(
            f"[ENRICH] {len(permanently_failed)} ads trvale selhalo — pridavam do enriched setu (skip)"
        )
        sector_enr["enriched_ad_ids"].extend(permanently_failed)

    # Vzdy aktualizuj last_enriched_at — i pri selhani, aby LRU rotovalo sektory
    sector_enr["enriched_count"] = len(sector_enr["enriched_ad_ids"])
    sector_enr["total_pending"] = len(all_pending_ads) - sector_enr["enriched_count"]
    sector_enr["last_enriched_at"] = datetime.utcnow().isoformat()

    # Zkontroluj jestli je sektor hotovy
    remaining = len(all_pending_ads) - sector_enr["enriched_count"]
    if remaining <= 0:
        sector_enr["status"] = "complete"
        sector_enr["total_pending"] = 0
        logging.info(
            f"[ENRICH] Sektor {sector_name} COMPLETE! "
            f"({sector_enr['enriched_count']} enriched, "
            f"{len(sector_enr['bronze_files'])} bronze souboru)"
        )
        # Cleanup pending files
        delete_pending_sector(container, target_sector)
    else:
        logging.info(
            f"[ENRICH] Sektor {sector_name}: {sector_enr['enriched_count']} enriched, "
            f"{remaining} zbyva"
        )

    enrich_state["stats"]["total_runs"] += 1
    save_enrichment_state(container, enrich_state)

    elapsed = time.time() - t_start
    logging.info(f"[ENRICH] HOTOVO ({elapsed:.1f}s)")
    logging.info("=" * 60)
