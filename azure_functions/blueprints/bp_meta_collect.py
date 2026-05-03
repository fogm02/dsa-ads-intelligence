"""
Meta Ad Library — Azure Function (Collection Blueprint)

Timer trigger: každé 4 hodiny (offset :15 od TikTok)
Flow:
  1. Health check (ověření tokenu)
  2. Načtení stavu z state/meta_collection.json
  3. Výběr dalšího nehotového sektoru
    4. Sběr reklam přes page_ids s deadline budgetem
  5. Deduplikace + bronze JSON upload → bronze/meta/{sector}/{timestamp}.json
  6. Aktualizace stavu

Klíčová rozhodnutí (viz docs/meta_integration_design.md):
- Jedna funkce (ne discover/enrich split) — Meta API vrací plná data v jednom volání
- Resumable: state trackuje completed_pages per sektor
- Deadline budget: 29 min z 30 min Flex Consumption timeout
- Bronze formát zachovává surová API data (medallion princip)
"""

import logging
import os
import time
from datetime import datetime, timezone

import azure.functions as func

from shared.meta_api import (
    health_check,
    collect_page,
    merge_and_deduplicate,
    build_bronze,
    DEFAULT_DATE_MIN,
    DEFAULT_DATE_MAX,
)
from shared.blob_helpers import (
    get_container_client,
    download_json,
    upload_json,
    load_meta_advertisers_from_blob,
)

bp = func.Blueprint()

# ============================================================
# CONSTANTS
# ============================================================

META_STATE_PATH = "state/meta_collection.json"
DEADLINE_BUDGET_S = 1740  # 29 minut z 30min Flex Consumption timeout
MAX_PAGES_PER_ADVERTISER = 50  # ~25K ads — prevence OOM při serializaci bronze JSON

# Env vars
DATE_MIN = os.environ.get("META_DATE_MIN", DEFAULT_DATE_MIN)
DATE_MAX = os.environ.get("META_DATE_MAX", DEFAULT_DATE_MAX)


# ============================================================
# STATE MANAGEMENT
# ============================================================

def load_state(container):
    """Načte nebo vytvoří stav Meta collection."""
    state = download_json(container, META_STATE_PATH)
    if not state:
        state = {
            "version": 1,
            "cycle_id": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
            "sectors": {},
        }
    return state


def save_state(container, state):
    """Uloží stav do Blob Storage."""
    upload_json(container, META_STATE_PATH, state)


def get_sector_state(state, sector_key):
    """Vrátí stav sektoru, vytvoří default pokud neexistuje."""
    if sector_key not in state["sectors"]:
        state["sectors"][sector_key] = {
            "status": "pending",
            "completed_pages": [],
            "page_cursors": {},
            "ads_collected": 0,
            "last_run_at": None,
        }
    # Backfill pro existující záznamy bez page_cursors
    state["sectors"][sector_key].setdefault("page_cursors", {})
    return state["sectors"][sector_key]


# ============================================================
# SECTOR SELECTION
# ============================================================

def select_next_sector(state, meta_sectors):
    """
    Vybere nekompletní sektor který nejdéle čekal (starvation-free scheduling).

    Stejný vzor jako TikTok enrich — None (nikdy nezpracován) je menší
    než jakýkoli ISO timestamp, takže nové sektory dostanou prioritu automaticky.
    """
    candidates = [
        s for s in meta_sectors
        if get_sector_state(state, s)["status"] != "complete"
    ]
    if not candidates:
        return None

    return min(
        candidates,
        key=lambda s: get_sector_state(state, s).get("last_run_at") or "",
    )


def reset_complete_sectors_with_new_pages(state, meta_sectors):
    """
    Detekuje sektory s novými pages v configu, které nejsou v completed_pages.

    Pokud byl sektor dříve označen jako 'complete' a mezitím někdo přidal
    do configu nové pages, přepne ho zpět na 'pending'. completed_pages
    zůstávají netknuté, takže už zpracované pages se přeskočí a sebere
    se jen rozdíl.

    Řeší situaci, kdy nový cyklus (check_and_start_new_cycle) nemůže
    startovat, protože jeden ze sektorů je trvale in_progress (zaseknutý
    kurzor), a ostatní complete sektory zamrzají s neaktuálním configem.
    """
    for sector_key, sector_config in meta_sectors.items():
        sector_state = get_sector_state(state, sector_key)
        if sector_state["status"] != "complete":
            continue

        config_page_ids = set(sector_config.get("pages", {}).keys())
        completed = set(sector_state.get("completed_pages", []))
        new_pages = config_page_ids - completed
        if new_pages:
            preview = ", ".join(list(new_pages)[:3])
            suffix = "..." if len(new_pages) > 3 else ""
            logging.info(
                f"[META] {sector_key}: {len(new_pages)} nových pages v configu "
                f"({preview}{suffix}) — reset complete → pending"
            )
            sector_state["status"] = "pending"


MAX_CYCLE_AGE_DAYS = 7  # Pojistka — vynucený nový cyklus pokud poslední je starší


def check_and_start_new_cycle(state, meta_sectors):
    """
    Startuje nový cyklus pokud:
    - VŠECHNY sektory jsou complete (normální průběh), NEBO
    - Aktuální cycle_id je starší než MAX_CYCLE_AGE_DAYS (pojistka proti zaseknutí —
      např. kombinace `reset_complete_sectors_with_new_pages` + opakované úpravy
      configu může způsobit, že cyklus se nikdy nedostane do "all complete" stavu;
      timeout zaručuje force restart).

    Při restartu cyklu vyčistí completed_pages a page_cursors všech sektorů,
    aby pipeline znovu sebrala kompletní data napříč všemi advertisery.
    """
    all_complete = all(
        get_sector_state(state, sk)["status"] == "complete"
        for sk in meta_sectors
    )

    cycle_too_old = False
    cycle_id = state.get("cycle_id", "")
    if cycle_id:
        try:
            cycle_dt = datetime.strptime(cycle_id, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
            age_days = (datetime.now(timezone.utc) - cycle_dt).days
            cycle_too_old = age_days > MAX_CYCLE_AGE_DAYS
            if cycle_too_old:
                logging.warning(
                    f"[META] Cycle {cycle_id} starý {age_days} dní (> {MAX_CYCLE_AGE_DAYS}) "
                    f"— force restart"
                )
        except ValueError:
            logging.warning(f"[META] Cannot parse cycle_id {cycle_id!r}, ignoring age check")

    if not all_complete and not cycle_too_old:
        return False

    reason = "all complete" if all_complete else "timeout"
    logging.info(f"[META] Starting new cycle ({reason})")
    state["cycle_id"] = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    for sector_key in meta_sectors:
        sector_state = get_sector_state(state, sector_key)
        sector_state["status"] = "pending"
        sector_state["completed_pages"] = []
        sector_state["page_cursors"] = {}
        sector_state["ads_collected"] = 0

    return True


# ============================================================
# MAIN COLLECTION LOGIC
# ============================================================

def collect_sector_with_deadline(sector_key, sector_config, sector_state,
                                 token, deadline):
    """
    Sbírá reklamy pro jeden sektor s respektováním deadline.

    Resumable: sleduje completed_pages ve state.
    Pokud deadline vyprší uprostřed, uloží progress a příští běh pokračuje.

    Returns:
        (all_results, failed, incomplete, is_complete)
    """
    pages = sector_config.get("pages", {})
    completed_pages = set(sector_state.get("completed_pages", []))

    all_results = []
    failed = []
    incomplete = []

    # Cursor state pro resumable pagination
    page_cursors = sector_state.get("page_cursors", {})

    # Phase 1: Page ID sběr (přesnější, primární metoda)
    page_items = list(pages.items())
    for idx, (page_id, page_name) in enumerate(page_items, 1):
        if page_id in completed_pages:
            continue

        if time.time() >= deadline:
            logging.info(f"[META] Deadline reached at page {idx}/{len(page_items)} "
                         f"of {sector_key}")
            return all_results, failed, incomplete, False

        # Načti uložený cursor z minulého běhu (pokud existuje)
        saved_cursor = page_cursors.get(page_id)
        if saved_cursor:
            logging.info(f"[META] [{idx}/{len(page_items)}] {page_name} "
                         f"(ID: {page_id}) — RESUMING from cursor")
        else:
            logging.info(f"[META] [{idx}/{len(page_items)}] {page_name} (ID: {page_id})")

        ads, complete, cursor = collect_page(
            page_id, page_name, token, deadline=deadline,
            date_min=DATE_MIN, date_max=DATE_MAX,
            resume_url=saved_cursor,
            max_pages=MAX_PAGES_PER_ADVERTISER,
        )

        if ads is None:
            failed.append(page_name)
        else:
            all_results.append((page_name, ads))
            if complete:
                # Úspěšně dokončeno — označíme jako hotové, smažeme cursor
                completed_pages.add(page_id)
                sector_state["completed_pages"] = list(completed_pages)
                page_cursors.pop(page_id, None)
            else:
                # Přerušeno — uložíme cursor pro resume v příštím běhu
                if cursor:
                    page_cursors[page_id] = cursor
                    logging.info(f"[META] {page_name}: cursor uložen pro resume "
                                 f"({len(ads)} ads v tomto běhu)")
                incomplete.append(f"{page_name} ({len(ads)} ads, incomplete)")

        sector_state["page_cursors"] = page_cursors

    # Vše dokončeno
    is_complete = len(completed_pages) >= len(pages)
    return all_results, failed, incomplete, is_complete


# ============================================================
# AZURE FUNCTION — TIMER TRIGGER
# ============================================================

@bp.timer_trigger(
    schedule="0 15 */8 * * *",     # Každých 8h v :15 (offset od TikTok :00/:30)
    arg_name="timer",
    run_on_startup=False,
)
def meta_collect(timer: func.TimerRequest) -> None:
    """
    Meta Ad Library — periodický sběr reklam.

    Zpracovává jeden sektor per běh (resumable across runs).
    Bronze output triggeruje bp_meta_transform přes EventGrid.
    """
    t_start = time.time()
    deadline = t_start + DEADLINE_BUDGET_S

    logging.info("[META] ========== Meta Collection START ==========")

    # 1. Health check
    token = os.environ.get("META_ACCESS_TOKEN", "")
    is_healthy, msg = health_check(token)
    if not is_healthy:
        logging.error(f"[META] Health check FAILED: {msg}")
        return

    logging.info("[META] Health check OK")

    # 2. Load state
    container = get_container_client()
    state = load_state(container)

    # 2b. Načti Meta konfiguraci z Blob CSV (jediný zdroj pravdy)
    meta_sectors = load_meta_advertisers_from_blob(container)
    if not meta_sectors:
        logging.error("[META] config/meta_advertisers.csv v Blob nenalezen — končím")
        return
    logging.info(f"[META] Sektory v běhu: {', '.join(meta_sectors.keys())}")

    # 3a. Check for new cycle PŘED reset_complete_sectors_with_new_pages
    # (jinak by reset_complete vždy přepnul aspoň jeden sektor na pending,
    # check_and_start_new_cycle by pak vrátil False a cyklus by se nikdy
    # nedostal do "all complete" stavu — timeout v check_and_start_new_cycle
    # je second-line pojistka, ale správné pořadí je primární fix).
    cycle_started = check_and_start_new_cycle(state, meta_sectors)

    # 3b. Reset complete sektorů s novými pages — jen pokud nový cyklus právě
    # nestartoval (po novém cyklu jsou všechny pending, nemá co resetovat).
    if not cycle_started:
        reset_complete_sectors_with_new_pages(state, meta_sectors)

    # 4. Select sector
    sector_key = select_next_sector(state, meta_sectors)
    if sector_key is None:
        logging.info("[META] No sectors to process")
        save_state(container, state)
        return

    sector_config = meta_sectors[sector_key]
    sector_state = get_sector_state(state, sector_key)
    sector_state["status"] = "in_progress"

    logging.info(f"[META] Processing sector: {sector_key} "
                 f"({sector_config['display_name']})")

    # 5. Collect
    all_results, failed, incomplete, is_complete = collect_sector_with_deadline(
        sector_key, sector_config, sector_state, token, deadline
    )

    # 6. Merge & deduplicate
    ads_dict, coverage = merge_and_deduplicate(all_results)
    raw_total = sum(len(a) for _, a in all_results)
    logging.info(f"[META] Merge: {raw_total} raw → {len(ads_dict)} unique ads")

    # 7. Upload bronze (pokud máme data)
    if ads_dict:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        bronze_path = f"bronze/meta/{sector_key}/{ts}.json"

        bronze_data = build_bronze(
            sector_key=sector_key,
            sector_display_name=sector_config["display_name"],
            ads_dict=ads_dict,
            coverage=coverage,
            failed=failed,
            incomplete=incomplete,
            date_min=DATE_MIN,
            date_max=DATE_MAX,
        )

        upload_json(container, bronze_path, bronze_data)
        logging.info(f"[META] Bronze uploaded: {bronze_path} ({len(ads_dict)} ads)")

    # 8. Update state
    sector_state["ads_collected"] = sector_state.get("ads_collected", 0) + len(ads_dict)
    sector_state["last_run_at"] = datetime.now(timezone.utc).isoformat()

    if is_complete:
        sector_state["status"] = "complete"
        logging.info(f"[META] Sector {sector_key} COMPLETE "
                     f"({len(ads_dict)} ads, {len(failed)} failed)")
    else:
        sector_state["status"] = "in_progress"
        logging.info(f"[META] Sector {sector_key} IN PROGRESS "
                     f"({len(ads_dict)} ads so far, continuing next run)")

    save_state(container, state)

    elapsed = time.time() - t_start
    logging.info(f"[META] ========== Meta Collection END "
                 f"({elapsed:.0f}s) ==========")
