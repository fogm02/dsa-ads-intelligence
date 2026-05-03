"""
Blueprint: TikTok Discovery — oficiální query/ads endpoint.

Timer trigger kazdych 2h (:00).
Oficialni TikTok Research API (open.tiktokapis.com/v2/research/adlib/ad/query/)
→ ad IDs + zakladni metadata per business_id.
Uklada discovered ads do pending/{sector}/{business_id}.json.

Resumable: velci inzerenti se zpracuji pres vice behu (cursor-based pagination).
Vyzaduje OAuth token (TIKTOK_CLIENT_KEY/SECRET v env).

Historie: do dubna 2026 byl query/ads endpoint nefunkcni (HTTP 500), Discovery
proto vyuzivala neoficialni library.tiktok.com endpoint. Po obnoveni query/ads
byla migrovana na oficialni endpoint (viz kap 5.2 diplomove prace).
"""

import azure.functions as func
import logging
import os
import time
from datetime import datetime

from shared.tiktok_api import (
    TokenManager,
    health_check_query_ads,
    query_ads_for_advertiser,
    query_ads_to_dict,
)
from shared.blob_helpers import (
    get_container_client,
    upload_json,
    download_json,
    list_blobs,
    upload_pending,
    pending_blob_path,
    load_advertisers_from_blob,
)


bp = func.Blueprint()

DISCOVERY_STATE_PATH = "state/discovery.json"

# Deadline: 29 min z 30 min Flex Consumption timeout (1 min reserve)
DEADLINE_BUDGET_S = 1740


# ============================================================
# STATE MANAGEMENT
# ============================================================

def load_discovery_state(container):
    """Nacte discovery state z Blob. Vytvori defaultni pokud neexistuje."""
    state = download_json(container, DISCOVERY_STATE_PATH)
    if state and state.get("version") == 2:
        return state

    lookback = int(os.environ.get("DISCOVERY_LOOKBACK_DAYS", 365))
    state = {
        "version": 2,
        "cycle_id": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
        "cycle_started_at": datetime.utcnow().isoformat(),
        "lookback_days": lookback,
        "sectors": {},
        "stats": {
            "total_runs": 0,
            "total_ads_discovered": 0,
        },
    }
    logging.info(f"[DISCOVER] Novy state (lookback={lookback}d)")
    return state


def save_discovery_state(container, state):
    """Ulozi state do Blob."""
    upload_json(container, DISCOVERY_STATE_PATH, state)


def init_sector_state(state, sector_key, advertisers):
    """Inicializuje stav sektoru v discovery state pokud neexistuje."""
    if sector_key not in state["sectors"]:
        state["sectors"][sector_key] = {
            "status": "pending",
            "advertisers": {},
        }

    sector = state["sectors"][sector_key]
    # Pridej nove advertisery pokud jeste nejsou v state
    new_added = False
    for biz_id, name in advertisers.items():
        biz_key = str(biz_id)
        if biz_key not in sector["advertisers"]:
            sector["advertisers"][biz_key] = {
                "name": name,
                "status": "pending",
                "ads_found": 0,
                "search_id": "",
                "has_more": True,
            }
            new_added = True

    # Pokud se pridali novi advertiseri do "complete" sektoru, reopen
    if new_added and sector["status"] == "complete":
        sector["status"] = "in_progress"
        logging.info(f"[DISCOVER] {sector_key}: novi advertiseri v CSV — reopen sektoru")


def check_cycle_complete(state, container):
    """
    Zkontroluje jestli jsou vsechny sektory complete.
    Pokud ano A pending soubory jsou zpracovane, zacne novy cyklus.
    """
    all_complete = True
    for sector_key, sector_data in state["sectors"].items():
        if sector_data["status"] not in ("complete", "skipped"):
            all_complete = False
            break

    if not all_complete:
        return False

    # Zkontroluj jestli enrichment zpracoval vsechny pending soubory
    # Ignoruj 0-byte directory markery — jen skutecne JSON soubory
    pending_blobs = [b for b in list_blobs(container, "pending/") if b.endswith(".json")]
    if pending_blobs:
        logging.info(
            f"[DISCOVER] Vsechny sektory complete, ale {len(pending_blobs)} "
            f"pending souboru ceka na enrichment — novy cyklus ODLOZEN"
        )
        return False

    # Novy cyklus — pending soubory zpracovany
    # Lookback 30 dni: bezpecna rezerva proti gapum (i kdyz cyklus stagne 14 dni,
    # dalsi run chyti 30 dni zpet = zadny gap v datech).
    lookback = int(os.environ.get("DISCOVERY_LOOKBACK_DAYS", 30))
    old_cycle = state.get("cycle_id", "?")
    state["cycle_id"] = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    state["cycle_started_at"] = datetime.utcnow().isoformat()
    state["lookback_days"] = lookback

    for sector_key, sector_data in state["sectors"].items():
        sector_data["status"] = "pending"
        for adv_key, adv_data in sector_data.get("advertisers", {}).items():
            adv_data["status"] = "pending"
            adv_data["ads_found"] = 0
            adv_data["search_id"] = ""
            adv_data["has_more"] = True
            adv_data.pop("last_page_offset", None)  # legacy field, query/ads cursor-only

    logging.info(
        f"[DISCOVER] Vsechny sektory complete + pending zpracovany — novy cyklus "
        f"(lookback={lookback}d, old={old_cycle})"
    )
    return True


# ============================================================
# TIMER TRIGGER — kazdych 8h
# ============================================================

@bp.timer_trigger(
    schedule="0 0 */8 * * *",     # Každých 8h v :00 (offset od Enrichment :30)
    arg_name="timer",
    run_on_startup=False,
)
def tiktok_discover(timer: func.TimerRequest) -> None:
    """
    Discovery funkce — oficialni query/ads ad discovery.

    Flow:
    1. OAuth token + health check (query/ads)
    2. Nacti advertisery z Blob CSV
    3. Najdi dalsiho pending advertisera
    4. query/ads s resumable pagination (cursor-based)
    5. Uloz pending/{sector}/{biz_id}.json + state
    """
    t_start = time.time()
    deadline = t_start + DEADLINE_BUDGET_S
    logging.info("=" * 60)
    logging.info("TikTok DISCOVERY — START")
    logging.info(f"Cas: {datetime.utcnow().isoformat()}")

    if timer.past_due:
        logging.warning("Timer je zpozdeny (past due)")

    # 1. OAuth token + health check
    try:
        token_mgr = TokenManager()
    except Exception as e:
        logging.error(f"[DISCOVER] Nelze ziskat OAuth token: {e}")
        return

    if not health_check_query_ads(token_mgr):
        logging.warning("query/ads nedostupne — koncim")
        return

    # 2. Container + advertisery
    container = get_container_client()
    advertisers_by_sector = load_advertisers_from_blob(container)

    if not advertisers_by_sector:
        logging.warning("Zadni advertiseri v CSV — koncim")
        return

    # 3. Nacti/vytvor state
    state = load_discovery_state(container)

    # Inicializuj sektory ktere maji advertisery
    for sector_key, sector_cfg in advertisers_by_sector.items():
        init_sector_state(state, sector_key, sector_cfg["advertisers"])

    # Zkontroluj jestli neni treba novy cyklus
    check_cycle_complete(state, container)

    lookback_days = state.get("lookback_days", 365)
    logging.info(f"[DISCOVER] Cyklus {state['cycle_id']}, lookback={lookback_days}d")

    # 4. Najdi prvni pending sektor + advertisera
    target_sector = None
    target_biz_id = None
    target_adv = None

    for sector_key, sector_data in state["sectors"].items():
        if sector_data["status"] in ("complete", "skipped"):
            continue
        # Hledej pending/in_progress advertisera
        for biz_key, adv_data in sector_data["advertisers"].items():
            if adv_data["status"] in ("pending", "in_progress"):
                target_sector = sector_key
                target_biz_id = biz_key
                target_adv = adv_data
                break
        if target_sector:
            break

    if not target_sector:
        logging.info("Zadny pending advertiser — cekam na dalsi cyklus")
        state["stats"]["total_runs"] += 1
        save_discovery_state(container, state)
        return

    sector_name = advertisers_by_sector.get(target_sector, {}).get("display_name", target_sector)
    logging.info(
        f"[DISCOVER] Sektor: {sector_name} | Advertiser: {target_adv['name']} "
        f"(biz_id={target_biz_id})"
    )

    # 5. query/ads — resumable fetch
    resume_search_id = target_adv.get("search_id", "")

    try:
        ads_list, final_search_id, has_more = query_ads_for_advertiser(
            token_mgr=token_mgr,
            business_id=int(target_biz_id),
            advertiser_name=target_adv["name"],
            lookback_days=lookback_days,
            deadline=deadline,
            resume_search_id=resume_search_id,
        )
    except Exception as e:
        logging.error(f"[DISCOVER] Chyba query/ads: {e}")
        state["stats"]["total_runs"] += 1
        save_discovery_state(container, state)
        return

    # 6. Uloz discovered ads do pending blob
    if ads_list:
        ads_dict = query_ads_to_dict(ads_list, int(target_biz_id), target_adv["name"])

        # Nacti existujici pending file (pokud resume) a mergni
        blob_path = pending_blob_path(target_sector, target_biz_id, target_adv["name"])
        existing_pending = download_json(container, blob_path)
        if existing_pending and "ads" in existing_pending:
            # Mergni: nove ads se pridaji k existujicim
            existing_pending["ads"].update(ads_dict)
            existing_pending["total_ads"] = len(existing_pending["ads"])
            existing_pending["discovered_at"] = datetime.utcnow().isoformat()
            pending_data = existing_pending
        else:
            pending_data = {
                "sector": target_sector,
                "business_id": int(target_biz_id),
                "advertiser_name": target_adv["name"],
                "discovered_at": datetime.utcnow().isoformat(),
                "total_ads": len(ads_dict),
                "ads": ads_dict,
            }

        upload_pending(container, target_sector, target_biz_id, pending_data,
                       advertiser_name=target_adv["name"])
        total_found = pending_data["total_ads"]
    else:
        total_found = target_adv.get("ads_found", 0)

    # 7. Update state
    target_adv["ads_found"] = total_found
    target_adv["search_id"] = final_search_id
    target_adv["has_more"] = has_more

    # Rate-limit / chybový guard: pokud jsme na prvni strance a dostali 0 vysledku
    # bez naznacene dalsi stranky, neoznacuj jako complete — zkusime priste znovu
    if not has_more and not ads_list and not resume_search_id:
        target_adv["status"] = "in_progress"
        target_adv["has_more"] = True
        logging.warning(
            f"[DISCOVER] {target_adv['name']}: 0 ads na prvním pokusu — "
            f"prijde znovu příště"
        )
    elif has_more:
        target_adv["status"] = "in_progress"
        logging.info(
            f"[DISCOVER] {target_adv['name']}: IN PROGRESS — "
            f"{total_found} ads, pokracuje priste"
        )
    else:
        target_adv["status"] = "complete"
        logging.info(
            f"[DISCOVER] {target_adv['name']}: COMPLETE — {total_found} ads"
        )

    # Zkontroluj jestli je cely sektor hotovy
    sector_data = state["sectors"][target_sector]
    all_advertisers_done = all(
        a["status"] == "complete"
        for a in sector_data["advertisers"].values()
    )
    if all_advertisers_done:
        sector_data["status"] = "complete"
        total_sector_ads = sum(
            a["ads_found"] for a in sector_data["advertisers"].values()
        )
        logging.info(
            f"[DISCOVER] Sektor {sector_name} COMPLETE! "
            f"({total_sector_ads} ads celkem)"
        )
    else:
        sector_data["status"] = "in_progress"
        done = sum(1 for a in sector_data["advertisers"].values() if a["status"] == "complete")
        total = len(sector_data["advertisers"])
        logging.info(f"[DISCOVER] Sektor {sector_name}: {done}/{total} advertiserů hotovo")

    # Pokud zbyva cas, zpracuj dalsiho advertisera ve stejnem sektoru
    if time.time() < deadline - 120 and not all_advertisers_done:
        for biz_key, adv_data in sector_data["advertisers"].items():
            if adv_data["status"] == "pending" and time.time() < deadline - 120:
                logging.info(f"[DISCOVER] Cas zbyva — pokracuji s {adv_data['name']}")
                try:
                    more_ads, sid, hm = query_ads_for_advertiser(
                        token_mgr=token_mgr,
                        business_id=int(biz_key),
                        advertiser_name=adv_data["name"],
                        lookback_days=lookback_days,
                        deadline=deadline,
                    )
                except Exception as e:
                    logging.warning(f"[DISCOVER] Chyba: {e}")
                    break

                if more_ads:
                    more_dict = query_ads_to_dict(more_ads, int(biz_key), adv_data["name"])
                    blob_path_extra = pending_blob_path(target_sector, biz_key, adv_data["name"])
                    existing_extra = download_json(container, blob_path_extra)
                    if existing_extra and "ads" in existing_extra:
                        existing_extra["ads"].update(more_dict)
                        existing_extra["total_ads"] = len(existing_extra["ads"])
                        existing_extra["discovered_at"] = datetime.utcnow().isoformat()
                        pd = existing_extra
                    else:
                        pd = {
                            "sector": target_sector,
                            "business_id": int(biz_key),
                            "advertiser_name": adv_data["name"],
                            "discovered_at": datetime.utcnow().isoformat(),
                            "total_ads": len(more_dict),
                            "ads": more_dict,
                        }
                    upload_pending(container, target_sector, biz_key, pd,
                                   advertiser_name=adv_data["name"])

                adv_data["ads_found"] = pd["total_ads"] if more_ads else adv_data.get("ads_found", 0)
                adv_data["search_id"] = sid
                adv_data["has_more"] = hm
                adv_data["status"] = "in_progress" if hm else "complete"

                if adv_data["status"] == "complete":
                    logging.info(f"[DISCOVER] {adv_data['name']}: COMPLETE — {adv_data['ads_found']} ads")

        # Re-check sektor status
        all_done = all(a["status"] == "complete" for a in sector_data["advertisers"].values())
        if all_done:
            sector_data["status"] = "complete"
            total_ads = sum(a["ads_found"] for a in sector_data["advertisers"].values())
            logging.info(f"[DISCOVER] Sektor {sector_name} COMPLETE! ({total_ads} ads)")

    state["stats"]["total_runs"] += 1
    state["stats"]["total_ads_discovered"] += len(ads_list) if ads_list else 0
    save_discovery_state(container, state)

    elapsed = time.time() - t_start
    logging.info(f"[DISCOVER] HOTOVO ({elapsed:.1f}s)")
    logging.info("=" * 60)
