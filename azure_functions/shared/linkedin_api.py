"""
LinkedIn Ad Library API klient pro Azure Functions pipeline.

Adaptováno z linkedin/linkedin_sector_collector.py pro serverless prostředí:
- logging místo print
- deadline budget pattern (9 min z 10 min Azure timeout)
- token z env var (ne z .json souboru)
- advertiser parametr místo keyword (empiricky přesnější, ověřeno 22.3.2026)

API: LinkedIn Marketing API /rest/adLibrary
Auth: OAuth 2.0 User Access Token
"""

import logging
import time
import requests

# ============================================================
# KONFIGURACE
# ============================================================

BASE_URL = "https://api.linkedin.com/rest/adLibrary"
API_VERSION = "202503"

# CZ filtr — ISO kód pro countries parametr
CZ_COUNTRY_CODE = "CZ"


# ============================================================
# HEADERS
# ============================================================

def get_headers(token):
    """Vrátí headers pro LinkedIn API."""
    return {
        "Authorization": f"Bearer {token}",
        "LinkedIn-Version": API_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
    }


# ============================================================
# HEALTH CHECK
# ============================================================

def health_check(token):
    """
    Ověří platnost LinkedIn tokenu jednoduchým API voláním.

    Returns:
        (bool, str) — (is_healthy, message)
    """
    if not token:
        return False, "LINKEDIN_ACCESS_TOKEN není nastaven"

    headers = get_headers(token)
    params = {
        "q": "criteria",
        "advertiser": "test",
        "start": 0,
        "count": 1,
    }

    try:
        r = requests.get(BASE_URL, headers=headers, params=params, timeout=15)
        if r.status_code == 200:
            return True, "OK"
        if r.status_code == 401:
            return False, (
                "Token expiroval nebo je neplatný! "
                "Regeneruj OAuth token a aktualizuj LINKEDIN_ACCESS_TOKEN "
                "v Azure App Settings."
            )
        return False, f"HTTP {r.status_code}: {r.text[:200]}"
    except requests.exceptions.RequestException as e:
        return False, f"Connection error: {e}"


# ============================================================
# API VOLÁNÍ — burst + recovery strategie
# ============================================================

# Rate limit — konstantní delay mezi requesty.
# LinkedIn Ad Library API: ~8 req burst, pak 5s recovery (~30 req/min).
# Konstantní 2s delay = ~30 req/min bez 429 chyb.
REQUEST_DELAY_S = 2.0


def _rate_limit_wait():
    """Konstantní delay mezi requesty — spolehlivější než burst strategie."""
    time.sleep(REQUEST_DELAY_S)


def _api_call(headers, params, max_retries=3, deadline=None, raw_suffix=""):
    """
    Jedno API volání s burst rate limiting, retry logikou a deadline kontrolou.

    Rate limit strategie (empiricky ověřeno):
    - Burst: 7 requestů rychle (0.5s delay v paginaci)
    - Recovery: 6s pauza po každém burstu
    - 429 fallback: 10s wait + retry

    Args:
        raw_suffix: raw URL suffix (pre-encoded), např. pro countries parametr
    """
    for attempt in range(1, max_retries + 1):
        if deadline and time.time() >= deadline:
            logging.warning("[LINKEDIN] Deadline reached during retry")
            return None

        if attempt > 1:
            time.sleep(2)

        _rate_limit_wait()

        try:
            # Pokud máme raw suffix (např. countries), sestavíme URL ručně
            if raw_suffix:
                req = requests.Request("GET", BASE_URL, headers=headers, params=params)
                prepared = req.prepare()
                prepared.url += raw_suffix
                session = requests.Session()
                r = session.send(prepared, timeout=30)
            else:
                r = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            logging.warning(f"[LINKEDIN] Request error ({attempt}/{max_retries}): {e}")
            time.sleep(5)
            continue

        if r.status_code == 200:
            return r.json()

        elif r.status_code == 429:
            wait = 10 * attempt  # Exponential: 10s, 20s, 30s
            logging.warning(
                f"[LINKEDIN] Rate limited, waiting {wait}s "
                f"(attempt {attempt}/{max_retries})"
            )
            if deadline and time.time() + wait >= deadline:
                logging.warning("[LINKEDIN] Rate limit wait would exceed deadline")
                return None
            time.sleep(wait)
            continue

        elif r.status_code == 401:
            logging.error("[LINKEDIN] Token expired or invalid (401)")
            return None

        elif r.status_code in (500, 502, 503):
            logging.warning(
                f"[LINKEDIN] Server error {r.status_code} "
                f"({attempt}/{max_retries})"
            )
            time.sleep(10 * attempt)
            continue

        else:
            logging.error(f"[LINKEDIN] HTTP {r.status_code}: {r.text[:200]}")
            return None

    logging.error(f"[LINKEDIN] {max_retries}x failed")
    return None


# ============================================================
# COLLECTION — sběr reklam per advertiser
# ============================================================

def collect_advertiser(headers, advertiser_name, max_results=500, deadline=None,
                       country=None):
    """
    Sbírá reklamy jednoho inzerenta přes advertiser parametr.

    Offset-based pagination (LinkedIn nepodporuje cursor).
    Max 25 per stránka.
    Volitelný countries filtr (API parametr, empiricky ověřeno 22.3.2026).

    Args:
        country: ISO kód země (např. "CZ") — filtruje na úrovni API

    Returns:
        (ads_list, complete) — list reklam a bool zda jsme viděli vše
    """
    all_ads = []
    start = 0
    count = 25  # LinkedIn max per page
    complete = True

    while True:
        if deadline and time.time() >= deadline:
            logging.info(
                f"[LINKEDIN] Deadline reached during pagination of '{advertiser_name}' "
                f"at offset {start}, {len(all_ads)} ads collected"
            )
            complete = False
            break

        params = {
            "q": "criteria",
            "advertiser": advertiser_name,
            "start": start,
            "count": count,
        }

        # Countries filtr — raw URL kvůli LinkedIn REST encoding
        raw_countries_suffix = ""
        if country:
            raw_countries_suffix = f"&countries=(value:List(urn%3Ali%3Acountry%3A{country}))"

        data = _api_call(headers, params, deadline=deadline,
                         raw_suffix=raw_countries_suffix)
        if data is None:
            complete = False
            break

        elements = data.get("elements", [])
        api_total = data.get("paging", {}).get("total", 0)
        all_ads.extend(elements)

        if not elements or start + count >= api_total:
            logging.info(
                f"[LINKEDIN] {advertiser_name}: {len(all_ads)} ads "
                f"(API total: {api_total}) — DONE"
            )
            break

        if len(all_ads) >= max_results:
            logging.info(
                f"[LINKEDIN] {advertiser_name}: max_results ({max_results}) reached"
            )
            break

        start += count
        time.sleep(0.5)  # Burst rate limiter v _api_call řídí pauzy

    return all_ads, complete


# ============================================================
# MERGE + DEDUPLICATE
# ============================================================

def merge_and_deduplicate(all_results):
    """
    Sloučí výsledky z více advertiser queries a deduplikuje podle adUrl.

    Args:
        all_results: list of (advertiser_name, ads_list) tuples

    Returns:
        (ads_dict, coverage) — {ad_url: ad_data}, {ad_url: [advertiser_names]}
    """
    ads = {}
    coverage = {}
    for label, ads_list in all_results:
        for ad in ads_list:
            ad_url = ad.get("adUrl", "")
            if not ad_url:
                continue

            ad["_search_advertiser"] = label

            if ad_url not in ads:
                ads[ad_url] = ad
                coverage[ad_url] = []
            coverage[ad_url].append(label)

    return ads, coverage


# ============================================================
# BRONZE BUILD
# ============================================================

def build_bronze(sector_key, sector_display_name, ads_dict, coverage,
                 failed, incomplete):
    """
    Sestaví bronze JSON objekt z nasbíraných LinkedIn dat.

    Bronze vrstva zachovává surová API data (medallion princip).
    Kategorizace (_categorization) se přidává jako metadata,
    raw API response zůstává nedotčená.
    """
    return {
        "metadata": {
            "platform": "linkedin",
            "sector": sector_key,
            "display_name": sector_display_name,
            "collected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total_ads": len(ads_dict),
            "collection_method": "advertiser",
            "failed_sources": failed,
            "incomplete_sources": incomplete,
        },
        "ads": ads_dict,
        "coverage": coverage,
    }
