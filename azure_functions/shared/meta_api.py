"""
Meta Ad Library API klient pro Azure Functions pipeline.

Extrahováno a adaptováno z meta/meta_sector_collector.py pro serverless prostředí:
- logging místo print
- deadline budget pattern (9 min z 10 min Azure timeout)
- token z env var (ne z .env souboru)
- Blob Storage výstup (ne lokální filesystem)

API: Meta Graph API /ads_archive (oficiální, stabilní)
Auth: User Access Token (60denní platnost)
Docs: https://developers.facebook.com/docs/marketing-api/reference/ads_archive/
"""

import logging
import os
import time
from datetime import date
import requests

# ============================================================
# KONFIGURACE
# ============================================================

API_VERSION = "v23.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}/ads_archive"

# DSA čl. 39 — pole požadovaná pro CI analýzu
# Obsahuje EU-specifická pole dostupná díky DSA regulaci
EU_FIELDS = ",".join([
    "id",
    "page_name",
    "page_id",
    "ad_creative_bodies",
    "ad_creative_link_captions",
    "ad_creative_link_descriptions",
    "ad_creative_link_titles",
    "ad_snapshot_url",
    "ad_delivery_start_time",
    "ad_delivery_stop_time",
    "languages",
    "publisher_platforms",
    "target_ages",
    "target_gender",
    "target_locations",
    "eu_total_reach",
    "beneficiary_payers",
    "age_country_gender_reach_breakdown",
])

# Výchozí datový rozsah — date_max musí být <= dnes (Meta API požadavek)
DEFAULT_DATE_MIN = "2025-01-01"
DEFAULT_DATE_MAX = date.today().isoformat()  # dynamicky = dnes


# ============================================================
# HEALTH CHECK
# ============================================================

def health_check(token):
    """
    Ověří platnost Meta Access Tokenu jednoduchým API voláním.

    Returns:
        (bool, str) — (is_healthy, message)
    """
    if not token:
        return False, "META_ACCESS_TOKEN neni nastaven"

    params = {
        "search_terms": "test",
        "ad_reached_countries": "['CZ']",
        "ad_type": "ALL",
        "fields": "id",
        "limit": 1,
        "access_token": token,
    }

    try:
        r = requests.get(BASE_URL, params=params, timeout=15)
        if r.status_code == 200:
            return True, "OK"

        error = r.json().get("error", {})
        code = error.get("code", 0)
        msg = error.get("message", "unknown")

        if code == 190:
            return False, (
                "Token expiroval! Regeneruj na "
                "https://developers.facebook.com/tools/explorer/ "
                "a aktualizuj META_ACCESS_TOKEN v Azure App Settings."
            )

        return False, f"API error code={code}: {msg[:200]}"

    except requests.exceptions.RequestException as e:
        return False, f"Connection error: {e}"


# ============================================================
# API VOLÁNÍ
# ============================================================

def _api_call(url, params, max_retries=3, delay=1.5, deadline=None):
    """
    Jedno API volání s retry logikou a deadline kontrolou.

    Adaptováno z meta_sector_collector.py — hlavní změny:
    - logging místo print
    - deadline parameter pro Azure Function timeout safety
    - max_retries sníženo z 5 na 3 (šetříme čas v 9min budgetu)
    - rate limit wait snížen z 5 min na 2 min (radši příští běh)
    """
    for attempt in range(1, max_retries + 1):
        if deadline and time.time() >= deadline:
            logging.warning("[META] Deadline reached during retry, stopping")
            return None, False

        if attempt > 1:
            time.sleep(delay)

        try:
            r = requests.get(url, params=params, timeout=60)
        except requests.exceptions.RequestException as e:
            logging.warning(f"[META] Request error ({attempt}/{max_retries}): {e}")
            time.sleep(5)
            continue

        if r.status_code == 200:
            return r.json(), True

        elif r.status_code == 400:
            error = r.json().get("error", {})
            code = error.get("code")
            msg = error.get("message", "")

            if code in (4, 17):
                # Rate limit — v Azure Function čekáme kratší dobu
                wait = 120  # 2 min (ne 5 min jako v CLI)
                logging.warning(
                    f"[META] Rate limited (code {code}), waiting {wait}s "
                    f"(attempt {attempt}/{max_retries})"
                )
                if deadline and time.time() + wait >= deadline:
                    logging.warning("[META] Rate limit wait would exceed deadline, stopping")
                    return None, False
                time.sleep(wait)
                continue

            if code == 190:
                logging.error(
                    "[META] TOKEN EXPIRED! Regenerate at "
                    "https://developers.facebook.com/tools/explorer/ "
                    "and update META_ACCESS_TOKEN in Azure App Settings."
                )
                return None, False

            logging.error(f"[META] API error code={code}: {msg[:200]}")
            return None, False

        elif r.status_code == 500:
            logging.warning(f"[META] Server error 500 ({attempt}/{max_retries})")
            time.sleep(3)
            continue

        else:
            logging.error(f"[META] HTTP {r.status_code}: {r.text[:200]}")
            return None, False

    logging.error(f"[META] {max_retries}x failed")
    return None, False


def _api_call_next(next_url, max_retries=3, delay=1.5, deadline=None):
    """
    Follow-up volání pro cursor pagination (Meta vrací 'paging.next' URL).
    """
    return _api_call(next_url, params=None, max_retries=max_retries,
                     delay=delay, deadline=deadline)


# ============================================================
# COLLECTION — sběr reklam
# ============================================================

def paginate(params, label, max_pages=None, deadline=None, resume_url=None):
    """
    Stránkuje přes Meta API výsledky s cursor pagination.

    Args:
        params: dict — parametry prvního API volání
        label: str — popis zdroje (pro logging)
        max_pages: int|None — max stránek (None = neomezeno)
        deadline: float|None — Unix timestamp deadline
        resume_url: str|None — cursor URL z předchozího běhu (resumable)

    Returns:
        (ads_list, complete, last_cursor) — list reklam, bool zda hotovo,
        cursor URL pro resume (None pokud complete)
    """
    all_ads = []
    page = 0
    complete = True
    next_url = resume_url  # Resume z uloženého cursoru pokud existuje

    if resume_url:
        logging.info(f"[META] {label}: resuming from saved cursor")

    while True:
        if deadline and time.time() >= deadline:
            logging.info(f"[META] Deadline reached during pagination of '{label}' "
                         f"at page {page}, {len(all_ads)} ads collected")
            complete = False
            break

        page += 1

        if next_url:
            data, ok = _api_call(next_url, params=None, deadline=deadline)
            # Pokud resume cursor expiroval, zahodíme ho a začneme od nuly
            if not ok and page == 1 and resume_url:
                logging.warning(f"[META] {label}: saved cursor expired, "
                                f"restarting from beginning")
                resume_url = None
                next_url = None
                data, ok = _api_call(BASE_URL, params, deadline=deadline)
        else:
            data, ok = _api_call(BASE_URL, params, deadline=deadline)

        if not ok or not data:
            complete = False
            break

        ads = data.get("data", [])
        all_ads.extend(ads)

        if page <= 3 or page % 10 == 0:
            logging.info(f"[META] {label}: page {page}, +{len(ads)} ads "
                         f"(total {len(all_ads)})")

        next_url = data.get("paging", {}).get("next")
        if not next_url:
            logging.info(f"[META] {label}: {len(all_ads)} ads total — DONE")
            break

        if max_pages and page >= max_pages:
            complete = False
            logging.info(f"[META] {label}: max pages ({max_pages}) reached")
            break

        time.sleep(1.5)

    last_cursor = next_url if not complete else None
    return all_ads, complete, last_cursor


def collect_page(page_id, page_name, token, deadline=None,
                 date_min=DEFAULT_DATE_MIN, date_max=DEFAULT_DATE_MAX,
                 resume_url=None, max_pages=None):
    """
    Sbírá reklamy jednoho inzerenta podle Facebook Page ID.

    Metoda page_id je přesnější než keyword search — 0% false positives.
    CZ filtr: ad_reached_countries=['CZ'] zajistí jen reklamy zobrazené v ČR.
    """
    params = {
        "search_page_ids": page_id,
        "ad_reached_countries": "['CZ']",
        "ad_type": "ALL",
        "ad_active_status": "ALL",
        "ad_delivery_date_min": date_min,
        "ad_delivery_date_max": date_max,
        "fields": EU_FIELDS,
        "limit": 500,
        "access_token": token,
    }
    return paginate(params, page_name, max_pages=max_pages,
                    deadline=deadline, resume_url=resume_url)


def collect_term(search_term, token, deadline=None,
                 date_min=DEFAULT_DATE_MIN, date_max=DEFAULT_DATE_MAX):
    """
    Sbírá reklamy podle klíčového slova (doplňkový sběr).

    Méně přesné než page_id — může zachytit i reklamy třetích stran
    zmíňujících hledaný výraz. Proto se používá jako doplněk k page_ids.
    """
    params = {
        "search_terms": search_term,
        "ad_reached_countries": "['CZ']",
        "ad_type": "ALL",
        "ad_active_status": "ALL",
        "ad_delivery_date_min": date_min,
        "ad_delivery_date_max": date_max,
        "fields": EU_FIELDS,
        "limit": 500,
        "access_token": token,
    }
    return paginate(params, f"term:{search_term}", deadline=deadline)


# ============================================================
# MERGE + DEDUPLICATE
# ============================================================

def merge_and_deduplicate(all_results):
    """
    Sloučí výsledky z více zdrojů (page_ids + terms) a deduplikuje podle ad_id.

    Args:
        all_results: list of (label, ads_list) tuples

    Returns:
        (ads_dict, coverage) — {ad_id: ad_data}, {ad_id: [labels]}
    """
    ads = {}
    coverage = {}
    for label, ads_list in all_results:
        for ad in ads_list:
            ad_id = ad.get("id")
            if not ad_id:
                continue
            if ad_id not in ads:
                ads[ad_id] = ad
                coverage[ad_id] = []
            coverage[ad_id].append(label)
    return ads, coverage


# ============================================================
# BRONZE BUILD
# ============================================================

def build_bronze(sector_key, sector_display_name, ads_dict, coverage,
                 failed, incomplete, date_min, date_max, collection_method="page_ids"):
    """
    Sestaví bronze JSON objekt z nasbíraných dat.

    Bronze vrstva zachovává surová API data beze změny (medallion princip).
    Metadata obsahují provenance informace pro auditovatelnost.
    """
    return {
        "metadata": {
            "platform": "meta",
            "sector": sector_key,
            "display_name": sector_display_name,
            "collected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "date_range": {"min": date_min, "max": date_max},
            "total_ads": len(ads_dict),
            "collection_method": collection_method,
            "failed_sources": failed,
            "incomplete_sources": incomplete,
        },
        "ads": ads_dict,
        "coverage": coverage,
    }
