"""
LinkedIn Ad Library — Azure Function (Collection Blueprint)

Timer trigger: každé 4 hodiny (offset :30 od Meta :15, TikTok :00)
Flow:
  1. Health check (ověření tokenu)
  2. Načtení stavu z state/linkedin_collection.json
  3. Výběr dalšího nehotového sektoru (LRU scheduling)
  4. Sběr reklam přes advertiser parametr s deadline budgetem
  5. Deduplikace + bronze JSON upload → bronze/linkedin/{sector}/{timestamp}.json
  6. Aktualizace stavu

Klíčová rozhodnutí:
- advertiser parametr místo keyword (empiricky přesnější — viz docs/metodika_linkedin.md)
- Jedna funkce (ne discover/enrich split) — API vrací plná data v jednom volání
- Resumable: state trackuje completed_advertisers per sektor
- CZ filtrace přes API parametr countries=CZ (empiricky ověřeno 22.3.2026)
"""

import logging
import os
import time
from datetime import datetime, timezone

import azure.functions as func

from shared.linkedin_api import (
    health_check,
    get_headers,
    collect_advertiser,
    merge_and_deduplicate,
    build_bronze,
)
from shared.blob_helpers import (
    get_container_client,
    download_json,
    upload_json,
    load_linkedin_advertisers_from_blob,
)

bp = func.Blueprint()

# ============================================================
# CONSTANTS
# ============================================================

LINKEDIN_STATE_PATH = "state/linkedin_collection.json"
DEADLINE_BUDGET_S = 1740  # 29 minut z 30min Flex Consumption timeout


# ============================================================
# STATE MANAGEMENT
# ============================================================

def load_state(container):
    """Načte nebo vytvoří stav LinkedIn collection."""
    state = download_json(container, LINKEDIN_STATE_PATH)
    if not state:
        state = {
            "version": 1,
            "cycle_id": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
            "sectors": {},
        }
    return state


def save_state(container, state):
    """Uloží stav do Blob Storage."""
    upload_json(container, LINKEDIN_STATE_PATH, state)


def get_sector_state(state, sector_key):
    """Vrátí stav sektoru, vytvoří default pokud neexistuje."""
    if sector_key not in state["sectors"]:
        state["sectors"][sector_key] = {
            "status": "pending",
            "completed_advertisers": [],
            "ads_collected": 0,
            "last_run_at": None,
        }
    return state["sectors"][sector_key]


# ============================================================
# SECTOR SELECTION
# ============================================================

def select_next_sector(state, linkedin_sectors):
    """
    Vybere nekompletní sektor který nejdéle čekal (starvation-free scheduling).

    None (nikdy nezpracován) je menší než jakýkoli ISO timestamp,
    takže nové sektory dostanou prioritu automaticky.
    """
    candidates = [
        s for s in linkedin_sectors
        if get_sector_state(state, s)["status"] != "complete"
    ]
    if not candidates:
        return None

    return min(
        candidates,
        key=lambda s: get_sector_state(state, s).get("last_run_at") or "",
    )


def check_and_start_new_cycle(state, linkedin_sectors):
    """
    Kontroluje zda jsou všechny sektory complete.
    Pokud ano, resetuje pro nový cyklus.
    """
    all_complete = all(
        get_sector_state(state, sk)["status"] == "complete"
        for sk in linkedin_sectors
    )

    if not all_complete:
        return False

    logging.info("[LINKEDIN] All sectors complete — starting new cycle")
    state["cycle_id"] = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    for sector_key in linkedin_sectors:
        sector_state = get_sector_state(state, sector_key)
        sector_state["status"] = "pending"
        sector_state["completed_advertisers"] = []
        sector_state["ads_collected"] = 0

    return True


# ============================================================
# MAIN COLLECTION LOGIC
# ============================================================

def collect_sector_with_deadline(sector_key, sector_config, sector_state,
                                 headers, deadline):
    """
    Sbírá reklamy pro jeden sektor s respektováním deadline.

    Resumable: sleduje completed_advertisers ve state.

    Returns:
        (all_results, failed, incomplete, is_complete)
    """
    advertisers = sector_config.get("advertisers", [])
    completed = set(sector_state.get("completed_advertisers", []))

    all_results = []
    failed = []
    incomplete = []

    for idx, adv_name in enumerate(advertisers, 1):
        if adv_name in completed:
            continue

        if time.time() >= deadline:
            logging.info(
                f"[LINKEDIN] Deadline reached at advertiser {idx}/{len(advertisers)} "
                f"of {sector_key}"
            )
            return all_results, failed, incomplete, False

        logging.info(f"[LINKEDIN] [{idx}/{len(advertisers)}] {adv_name}")

        ads, complete = collect_advertiser(
            headers, adv_name, deadline=deadline, country="CZ",
        )

        if ads is None:
            failed.append(adv_name)
        else:
            all_results.append((adv_name, ads))
            if complete:
                completed.add(adv_name)
                sector_state["completed_advertisers"] = list(completed)
            else:
                incomplete.append(f"{adv_name} ({len(ads)} ads, incomplete)")

        # Pauza mezi advertisery
        time.sleep(2)

    is_complete = len(completed) >= len(advertisers)
    return all_results, failed, incomplete, is_complete


# ============================================================
# AZURE FUNCTION — TIMER TRIGGER
# ============================================================

@bp.timer_trigger(
    schedule="0 45 */8 * * *",     # Každých 8h v :45 (offset od Meta :15, TikTok :00/:30)
    arg_name="timer",
    run_on_startup=False,
)
def linkedin_collect(timer: func.TimerRequest) -> None:
    """
    LinkedIn Ad Library — periodický sběr reklam.

    Zpracovává jeden sektor per běh (resumable across runs).
    Bronze output triggeruje bp_linkedin_transform přes EventGrid.
    """
    t_start = time.time()
    deadline = t_start + DEADLINE_BUDGET_S

    logging.info("[LINKEDIN] ========== LinkedIn Collection START ==========")

    # 1. Health check
    token = os.environ.get("LINKEDIN_ACCESS_TOKEN", "")
    is_healthy, msg = health_check(token)
    if not is_healthy:
        logging.error(f"[LINKEDIN] Health check FAILED: {msg}")
        return

    logging.info("[LINKEDIN] Health check OK")
    headers = get_headers(token)

    # 2. Load state
    container = get_container_client()
    state = load_state(container)

    # 2b. Načti LinkedIn konfiguraci z Blob CSV (fallback na static config)
    linkedin_sectors = load_linkedin_advertisers_from_blob(container)
    if not linkedin_sectors:
        logging.error(
            "[LINKEDIN] config/linkedin_advertisers.csv nenalezen v Blob Storage! "
            "Nahraj CSV a zkus znovu."
        )
        return
    logging.info(f"[LINKEDIN] Sektory v běhu: {', '.join(linkedin_sectors.keys())}")

    # 3. Check for new cycle
    check_and_start_new_cycle(state, linkedin_sectors)

    # 4. Select sector
    sector_key = select_next_sector(state, linkedin_sectors)
    if sector_key is None:
        logging.info("[LINKEDIN] No sectors to process")
        save_state(container, state)
        return

    sector_config = linkedin_sectors[sector_key]
    sector_state = get_sector_state(state, sector_key)
    sector_state["status"] = "in_progress"

    logging.info(
        f"[LINKEDIN] Processing sector: {sector_key} "
        f"({sector_config['display_name']})"
    )

    # 5. Collect
    all_results, failed, incomplete, is_complete = collect_sector_with_deadline(
        sector_key, sector_config, sector_state, headers, deadline
    )

    # 6. Merge & deduplicate
    ads_dict, coverage = merge_and_deduplicate(all_results)
    raw_total = sum(len(a) for _, a in all_results)
    logging.info(f"[LINKEDIN] Merge: {raw_total} raw → {len(ads_dict)} unique ads")

    # 7. Upload bronze (pokud máme data)
    if ads_dict:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        bronze_path = f"bronze/linkedin/{sector_key}/{ts}.json"

        bronze_data = build_bronze(
            sector_key=sector_key,
            sector_display_name=sector_config["display_name"],
            ads_dict=ads_dict,
            coverage=coverage,
            failed=failed,
            incomplete=incomplete,
        )

        upload_json(container, bronze_path, bronze_data)
        logging.info(f"[LINKEDIN] Bronze uploaded: {bronze_path} ({len(ads_dict)} ads)")

    # 8. Update state
    sector_state["ads_collected"] = len(ads_dict)
    sector_state["last_run_at"] = datetime.now(timezone.utc).isoformat()

    if is_complete:
        sector_state["status"] = "complete"
        logging.info(
            f"[LINKEDIN] Sector {sector_key} COMPLETE "
            f"({len(ads_dict)} ads, {len(failed)} failed)"
        )
    else:
        sector_state["status"] = "in_progress"
        logging.info(
            f"[LINKEDIN] Sector {sector_key} IN PROGRESS "
            f"({len(ads_dict)} ads so far, continuing next run)"
        )

    save_state(container, state)

    elapsed = time.time() - t_start
    logging.info(
        f"[LINKEDIN] ========== LinkedIn Collection END "
        f"({elapsed:.0f}s) =========="
    )
