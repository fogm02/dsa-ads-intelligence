"""
TikTok Hybrid API — Official query/ads (ad discovery) + Official Detail API (data).

Společný modul pro Azure Functions collector blueprint.
Credentials z os.environ, logging místo print.

Hybridní přístup (oba endpointy oficiální, OAuth 2.0):
  1. /research/adlib/ad/query/ → ad IDs + základní metadata per business_id (Discovery)
  2. /research/adlib/ad/detail/ → plná data: targeting, reach, videa (Enrichment)

Historický kontext: v únoru 2026 endpoint query/ads vracel HTTP 500 a Discovery
využívala neoficiální library.tiktok.com/api/v1/search. V dubnu 2026 byl query/ads
obnoven a Discovery byla migrována na oficiální endpoint. Funkce library_fetch_ads
zůstává v modulu pro reprodukovatelnost historických dat, ale nepoužívá se v aktivní
pipeline (viz kap 5.2 diplomové práce).
"""

import requests
import json
import time
import os
import random
import logging
from datetime import datetime, timezone

# ============================================================
# API CONSTANTS
# ============================================================

DETAIL_FIELDS = (
    "ad.id,ad.first_shown_date,ad.last_shown_date,ad.status,ad.reach,"
    "ad.videos,ad.image_urls,"
    "advertiser.business_id,advertiser.business_name,advertiser.paid_for_by,"
    "ad_group.targeting_info"
)

# query/ads vrací vlastní podmnožinu polí (cílení tam není — to plní Detail API)
QUERY_ADS_FIELDS = (
    "ad.id,ad.first_shown_date,ad.last_shown_date,ad.status,ad.reach,"
    "ad.videos,ad.image_urls,"
    "advertiser.business_id,advertiser.business_name,advertiser.paid_for_by"
)

QUERY_ADS_URL = "https://open.tiktokapis.com/v2/research/adlib/ad/query/"


# ============================================================
# TOKEN MANAGER (pro Official Detail API)
# ============================================================

class TokenManager:
    """Auto-refresh TikTok token (platný 7200s, refreshneme po 5400s)."""

    def __init__(self):
        self.token = None
        self.obtained_at = 0
        self.client_key = os.environ["TIKTOK_CLIENT_KEY"]
        self.client_secret = os.environ["TIKTOK_CLIENT_SECRET"]
        self.refresh()

    def refresh(self):
        r = requests.post(
            "https://open.tiktokapis.com/v2/oauth/token/",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "client_key": self.client_key,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
        )
        r.raise_for_status()
        data = r.json()
        self.token = data["access_token"]
        self.obtained_at = time.time()
        logging.info(f"[TOKEN] Získán (platný {data['expires_in']}s)")

    def get(self):
        if time.time() - self.obtained_at > 5400:
            logging.info("[TOKEN] Refreshuji (>1.5h)...")
            self.refresh()
        return self.token

    def force_refresh(self):
        logging.info("[TOKEN] Force refresh po 401...")
        self.refresh()


# ============================================================
# OFFICIAL QUERY/ADS — Discovery endpoint (oficiální, OAuth 2.0)
# ============================================================

def health_check_query_ads(token_mgr):
    """Test dostupnosti query/ads endpointu. Vrátí True/False."""
    try:
        from datetime import date, timedelta
        yesterday = (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y%m%d")
        thirty_days_ago = (datetime.utcnow().date() - timedelta(days=31)).strftime("%Y%m%d")

        r = requests.post(
            QUERY_ADS_URL,
            params={"fields": "ad.id"},
            headers={
                "Authorization": f"Bearer {token_mgr.get()}",
                "Content-Type": "application/json",
            },
            json={
                "filters": {
                    "advertiser_business_ids": ["7405161478984089601"],  # placeholder reálného CZ ID
                    "ad_published_date_range": {"min": thirty_days_ago, "max": yesterday},
                },
                "max_count": 1,
            },
            timeout=10,
        )
        if r.status_code == 401:
            token_mgr.force_refresh()
            return False
        if r.status_code == 200:
            data = r.json()
            err_code = data.get("error", {}).get("code", "")
            if err_code in ("ok", ""):
                logging.info("[HEALTH] query/ads OK")
                return True
            logging.warning(f"[HEALTH] query/ads error: {err_code}: {data['error'].get('message','')}")
        else:
            logging.warning(f"[HEALTH] query/ads HTTP {r.status_code}")
        return False
    except Exception as e:
        logging.warning(f"[HEALTH] query/ads exception: {e}")
        return False


def query_ads_for_advertiser(token_mgr, business_id, advertiser_name, lookback_days=365,
                              deadline=None, resume_search_id=""):
    """
    Získá reklamy pro jednoho inzerenta přes oficiální query/ads endpoint.
    Cursor-based paginace přes search_id.

    Resumable: pokud resume_search_id vyplněn, pokračuje od dané stránky.
    deadline — unix timestamp; přeruší paginaci pokud zbývá méně než 60 s.

    Returns:
        tuple: (list_of_ads, final_search_id, has_more)
        - list_of_ads: list dict z query/ads response (každý: {"ad": {...}, "advertiser": {...}})
        - final_search_id: cursor pro resume (prázdný string pokud has_more=False)
        - has_more: True pokud existují další stránky
    """
    # query/ads vyžaduje datum_max < dnes (musí být alespoň včerejšek)
    today = datetime.utcnow().date()
    end_date = today - _ONE_DAY
    start_date = end_date - _DAY * lookback_days

    all_ads = []
    seen_ids = set()
    search_id = resume_search_id
    page = 0
    consecutive_errors = 0
    MAX_ERRORS = 3
    last_has_more = False

    if resume_search_id:
        logging.info(f"  [QRY] {advertiser_name} (biz_id={business_id}) — RESUME (search_id present)")
    else:
        logging.info(f"  [QRY] {advertiser_name} (biz_id={business_id})")

    url = f"{QUERY_ADS_URL}?fields={requests.utils.quote(QUERY_ADS_FIELDS)}"

    while True:
        page += 1
        if page > 500:
            break

        if deadline and time.time() > deadline - 60:
            logging.warning(
                f"    [!] {advertiser_name}: deadline blíží, přerušuji po {page-1} stranách "
                f"({len(all_ads)} reklam)"
            )
            break

        body = {
            "filters": {
                "advertiser_business_ids": [str(business_id)],
                "ad_published_date_range": {
                    "min": start_date.strftime("%Y%m%d"),
                    "max": end_date.strftime("%Y%m%d"),
                },
            },
            "max_count": 50,
        }
        if search_id:
            body["search_id"] = search_id

        time.sleep(0.4 + random.uniform(0, 0.3))

        try:
            r = requests.post(
                url,
                json=body,
                headers={
                    "Authorization": f"Bearer {token_mgr.get()}",
                    "Content-Type": "application/json",
                },
                timeout=15,
            )
        except requests.exceptions.RequestException as e:
            consecutive_errors += 1
            logging.warning(f"    [!] Network error str. {page} ({consecutive_errors}/{MAX_ERRORS}): {e}")
            if consecutive_errors >= MAX_ERRORS:
                logging.warning(f"    [X] {advertiser_name}: {MAX_ERRORS}× network error — zastavuji")
                break
            time.sleep(3)
            continue

        if r.status_code == 401:
            token_mgr.force_refresh()
            consecutive_errors += 1
            if consecutive_errors >= MAX_ERRORS:
                break
            continue

        if r.status_code == 429:
            consecutive_errors += 1
            logging.warning(
                f"    [!] query/ads rate limit (HTTP 429), error {consecutive_errors}/{MAX_ERRORS}, čekám 30 s"
            )
            if consecutive_errors >= MAX_ERRORS:
                logging.warning(f"    [X] {advertiser_name}: {MAX_ERRORS}× rate limit — zastavuji")
                break
            time.sleep(30)
            continue

        if r.status_code != 200:
            consecutive_errors += 1
            logging.warning(
                f"    [!] query/ads HTTP {r.status_code} (str. {page}), "
                f"error {consecutive_errors}/{MAX_ERRORS}: {r.text[:200]}"
            )
            if consecutive_errors >= MAX_ERRORS:
                break
            time.sleep(5)
            continue

        try:
            payload = r.json()
        except (json.JSONDecodeError, ValueError):
            consecutive_errors += 1
            if consecutive_errors >= MAX_ERRORS:
                break
            continue

        err = payload.get("error", {})
        if err.get("code") and err.get("code") != "ok":
            logging.warning(
                f"    [!] query/ads API error: {err.get('code')}: {err.get('message','')[:200]}"
            )
            consecutive_errors += 1
            if consecutive_errors >= MAX_ERRORS:
                break
            continue

        consecutive_errors = 0
        data = payload.get("data", {})
        ads = data.get("ads", [])
        search_id = data.get("search_id", "")
        last_has_more = bool(data.get("has_more"))

        for ad in ads:
            aid = str(ad.get("ad", {}).get("id", ""))
            if aid and aid not in seen_ids:
                seen_ids.add(aid)
                all_ads.append(ad)

        if not ads or not last_has_more:
            last_has_more = False
            break

    logging.info(
        f"  [QRY] {advertiser_name}: {len(all_ads)} reklam, has_more={last_has_more}"
    )
    return all_ads, search_id, last_has_more


def query_ads_to_dict(ads_list, business_id, advertiser_name):
    """
    Konvertuje list z query_ads_for_advertiser() na dict pro pending storage.

    Output formát:
      {ad_id_str: {"discovery_data": {ad: {...}, advertiser: {...}},
                   "business_id": int, "advertiser_name": str}}
    """
    result = {}
    for entry in ads_list:
        ad_id = str(entry.get("ad", {}).get("id", ""))
        if ad_id:
            result[ad_id] = {
                "discovery_data": entry,
                "business_id": business_id,
                "advertiser_name": advertiser_name,
            }
    return result


# Pomocné konstanty pro datum aritmetiku v query_ads_for_advertiser
from datetime import timedelta as _td
_DAY = _td(days=1)
_ONE_DAY = _DAY


# ============================================================
# LIBRARY API — neoficiální endpoint (legacy, ad discovery)
# Nepoužívá se v aktivní pipeline; ponecháno pro historickou
# reprodukovatelnost dat sebraných před dubnem 2026.
# ============================================================

LIBRARY_API_URL = "https://library.tiktok.com/api/v1/search"

_UA_POOL = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="131", "Not:A-Brand";v="99"',
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) "
                      "Gecko/20100101 Firefox/128.0",
        "sec-ch-ua": "",
    },
]


def _library_headers():
    """Vrátí headers s náhodným UA profilem."""
    ua = random.choice(_UA_POOL)
    h = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
        "Content-Type": "application/json",
        "Origin": "https://library.tiktok.com",
        "Referer": "https://library.tiktok.com/",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    h.update(ua)
    return h


def _ms_to_datestr(ms_timestamp):
    """Konvertuje Library API timestamp (ms) na YYYYMMDD string."""
    if not ms_timestamp:
        return ""
    try:
        ts = int(ms_timestamp)
        if ts > 1e12:
            ts = ts // 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%d")
    except (ValueError, TypeError, OverflowError):
        return str(ms_timestamp)


def _map_audit_status(audit_status):
    """Mapuje Library API audit_status (1/2/3) na Research API status string."""
    return {
        "1": "active",
        "2": "expired",
        "3": "rejected",
    }.get(str(audit_status), "unknown")


# ============================================================
# HEALTH CHECK — přes Library API (vždy funguje)
# ============================================================

def health_check_library():
    """Rychlý test Library API dostupnosti (nepotřebuje token)."""
    try:
        r = requests.post(
            LIBRARY_API_URL,
            params={"region": "CZ", "type": 1,
                    "start_time": int(time.time()) - 86400 * 30,
                    "end_time": int(time.time())},
            json={"query": "test", "query_type": "",
                  "adv_biz_ids": "", "order": "last_shown_date,desc",
                  "offset": 0, "search_id": "", "limit": 1},
            headers=_library_headers(),
            timeout=10,
        )
        if r.status_code != 200 or "limit exceed" in r.text.lower():
            logging.warning("[HEALTH] Library API blocked/down")
            return False
        logging.info("[HEALTH] Library API OK")
        return True
    except Exception as e:
        logging.warning(f"[HEALTH] Library API error: {e}")
        return False


def health_check(token_mgr):
    """
    Rychlý test dostupnosti — Library API ping + token validity.
    Vrátí True pokud oba fungují, False pokud je problém.
    """
    # 1. Library API — jednoduchý search
    try:
        r = requests.post(
            LIBRARY_API_URL,
            params={"region": "CZ", "type": 1,
                    "start_time": int(time.time()) - 86400 * 30,
                    "end_time": int(time.time())},
            json={"query": "bank", "query_type": "",
                  "adv_biz_ids": "", "order": "last_shown_date,desc",
                  "offset": 0, "search_id": "", "limit": 1},
            headers=_library_headers(),
            timeout=10,
        )
        if r.status_code != 200 or "limit exceed" in r.text.lower():
            logging.warning("[HEALTH] Library API blocked/down")
            return False
        data = r.json()
        if "data" not in data and "total" not in data:
            logging.warning(f"[HEALTH] Library API neočekávaná odpověď: {r.text[:200]}")
            return False
    except Exception as e:
        logging.warning(f"[HEALTH] Library API error: {e}")
        return False

    # 2. Official API — token test (detail endp. s fake ID → chceme jen 200)
    try:
        headers = {
            "Authorization": f"Bearer {token_mgr.get()}",
            "Content-Type": "application/json",
        }
        r = requests.post(
            "https://open.tiktokapis.com/v2/research/adlib/ad/detail/",
            headers=headers,
            params={"fields": "ad.id"},
            json={"ad_id": 1},
            timeout=10,
        )
        if r.status_code == 401:
            token_mgr.force_refresh()
            return False
        # 200 s error "ok" nebo "invalid" = API dostupné
        if r.status_code == 200:
            logging.info("[HEALTH] OK — Library API + Detail API dostupné")
            return True
    except Exception as e:
        logging.warning(f"[HEALTH] Detail API error: {e}")
        return False

    return True


# ============================================================
# LIBRARY: COLLECT AD IDS BY BUSINESS ID
# ============================================================

def library_fetch_ads(business_id, advertiser_name, lookback_days=365, region="CZ",
                      deadline=None, resume_offset=0, resume_search_id=""):
    """
    Získá reklamy pro jednoho inzerenta přes Library API.
    Cursor-based paginace (search_id + offset).

    Resumable: pokud resume_offset > 0, pokračuje od uloženého místa.
    deadline — unix timestamp; přeruší paginaci pokud zbývá méně než 60s

    Returns:
        tuple: (list_of_ads, final_offset, final_search_id, has_more)
        - list_of_ads: list of dicts z Library API
        - final_offset: offset kde se zastavil (pro resume)
        - final_search_id: search_id pro resume
        - has_more: True pokud existují další stránky
    """
    end_time = int(time.time())
    start_time = end_time - lookback_days * 86400

    all_ads = []
    seen_ids = set()
    search_id = resume_search_id
    offset = resume_offset
    page = 0
    consecutive_errors = 0
    MAX_ERRORS = 3
    last_has_more = False

    if resume_offset > 0:
        logging.info(f"  [LIB] {advertiser_name} (biz_id={business_id}) — RESUME od offset={resume_offset}")
    else:
        logging.info(f"  [LIB] {advertiser_name} (biz_id={business_id})")

    while True:
        page += 1
        if page > 500:
            break

        # Zkontroluj deadline — přeruš pokud zbývá méně než 60s
        if deadline and time.time() > deadline - 60:
            logging.warning(f"    [!] {advertiser_name}: deadline blíží, přerušuji po {page-1} stranách ({len(all_ads)} reklam)")
            break

        body = {
            "query": "",
            "query_type": "",
            "adv_biz_ids": str(business_id),
            "order": "last_shown_date,desc",
            "offset": offset,
            "search_id": search_id,
            "limit": 20,
        }
        params = {
            "region": region,
            "type": 1,
            "start_time": start_time,
            "end_time": end_time,
        }

        time.sleep(0.4 + random.uniform(0, 0.3))

        try:
            r = requests.post(
                LIBRARY_API_URL,
                params=params,
                json=body,
                headers=_library_headers(),
                timeout=15,
            )
        except requests.exceptions.RequestException as e:
            consecutive_errors += 1
            logging.warning(f"    [!] Network error str. {page} ({consecutive_errors}/{MAX_ERRORS}): {e}")
            if consecutive_errors >= MAX_ERRORS:
                logging.warning(f"    [X] {advertiser_name}: {MAX_ERRORS}x network error — zastavuji")
                break
            time.sleep(3)
            continue

        if r.status_code != 200 or "limit exceed" in (r.text or "").lower():
            consecutive_errors += 1
            logging.warning(
                f"    [!] Library API blok (HTTP {r.status_code}), "
                f"error {consecutive_errors}/{MAX_ERRORS}, čekám 15s"
            )
            if consecutive_errors >= MAX_ERRORS:
                logging.warning(
                    f"    [X] {advertiser_name}: {MAX_ERRORS}x rate limit — "
                    f"zastavuji po {len(all_ads)} reklamách ({page} stránkách)"
                )
                break
            time.sleep(15)
            continue

        try:
            data = r.json()
        except (json.JSONDecodeError, ValueError):
            consecutive_errors += 1
            if consecutive_errors >= MAX_ERRORS:
                break
            continue

        consecutive_errors = 0
        ads = data.get("data", [])
        search_id = data.get("search_id", search_id)
        last_has_more = bool(data.get("has_more"))

        for ad in ads:
            aid = ad.get("id")
            if aid and aid not in seen_ids:
                seen_ids.add(aid)
                all_ads.append(ad)

        if not ads or not last_has_more:
            last_has_more = False
            break
        offset += len(ads)

    logging.info(f"  [LIB] {advertiser_name}: {len(all_ads)} reklam (offset={offset}, has_more={last_has_more})")
    return all_ads, offset, search_id, last_has_more


def collect_sector_via_library(sector_key, sector_config, lookback_days=365, deadline=None):
    """
    Hybridní sběr — fáze 1: Library API → ad IDs pro celý sektor.
    (Zpětná kompatibilita — volá se z bp_collector.py)

    deadline — unix timestamp; předává se do library_fetch_ads pro přerušení paginace
    Returns:
        dict: {ad_id_str: {"library_data": {...}, "business_id": int, "advertiser_name": str}}
    """
    advertisers = sector_config.get("advertisers", {})
    if not advertisers:
        logging.warning(f"Sektor {sector_config.get('display_name', sector_key)} — žádné business IDs")
        return {}

    all_ads = {}
    for biz_id, adv_name in advertisers.items():
        # Přeruš celou Phase 1 pokud zbývá méně než 90s (Phase 2 potřebuje čas)
        if deadline and time.time() > deadline - 90:
            logging.warning(f"[LIB] Deadline blíží — přeskakuji zbývající inzerenty (Phase 2 musí začít)")
            break
        ads_list, _, _, _ = library_fetch_ads(biz_id, adv_name, lookback_days=lookback_days, deadline=deadline)
        for ad in ads_list:
            ad_id = str(ad.get("id", ""))
            if ad_id:
                all_ads[ad_id] = {
                    "library_data": ad,
                    "business_id": biz_id,
                    "advertiser_name": adv_name,
                }
        if len(advertisers) > 3:
            time.sleep(1.0)

    logging.info(f"[LIB] Sektor {sector_key}: celkem {len(all_ads)} unikátních ad IDs")
    return all_ads


def ads_list_to_dict(ads_list, business_id, advertiser_name):
    """Konvertuje list z library_fetch_ads() na dict pro pending storage."""
    result = {}
    for ad in ads_list:
        ad_id = str(ad.get("id", ""))
        if ad_id:
            result[ad_id] = {
                "library_data": ad,
                "business_id": business_id,
                "advertiser_name": advertiser_name,
            }
    return result


# ============================================================
# OFFICIAL DETAIL API
# ============================================================

# Sentinel vrácený z fetch_detail() když je rate limit — enrich_ads() to použije
# k rozhodnutí jestli přerušit celý detail phase
_RATE_LIMITED = object()


def fetch_detail(token_mgr, ad_id):
    """
    Stáhne detail jedné reklamy přes oficiální API.
    Vrátí:
      dict  — úspěch
      None  — trvalá chyba (API error, 5xx)
      _RATE_LIMITED  — 429, enrich_ads() to přeruší
    """
    url = "https://open.tiktokapis.com/v2/research/adlib/ad/detail/"

    for attempt in range(1, 4):  # max 3 pokusy (ne 5)
        time.sleep(0.3)  # kratší sleep mezi pokusy
        headers = {
            "Authorization": f"Bearer {token_mgr.get()}",
            "Content-Type": "application/json",
        }
        try:
            r = requests.post(
                url, headers=headers,
                params={"fields": DETAIL_FIELDS},
                json={"ad_id": int(ad_id)},
                timeout=15,
            )
        except requests.exceptions.RequestException as e:
            logging.warning(f"    [!] Detail network error ({attempt}/3): {e}")
            time.sleep(2)
            continue

        if r.status_code == 200:
            data = r.json()
            if data.get("error", {}).get("code") == "ok":
                # Detail endpoint vrací {data: {ad: {...}, ad_group: {...}, advertiser: {...}}}
                return data.get("data")
            err_code = data.get("error", {}).get("code", "")
            if "daily_quota" in err_code:
                raise Exception("TikTok daily quota exceeded")
            logging.warning(f"    [!] Detail API error: {data.get('error')}")
            return None
        elif r.status_code == 401:
            token_mgr.force_refresh()
            continue
        elif r.status_code == 429:
            # Nečekáme dlouho — vrátíme sentinel, enrich_ads() rozhodne
            logging.warning(f"    [!] Rate limit 429 (ad_id={ad_id}, pokus {attempt}/3)")
            time.sleep(15)  # krátká pauza, pak zkusíme ještě 1x
            if attempt >= 2:
                # Po 2x 429 → vzdáme se tohoto ad, signalizujeme rate limit
                return _RATE_LIMITED
            continue
        elif r.status_code in (500, 503):
            time.sleep(2 * attempt)
            continue
        else:
            logging.warning(f"    [!] HTTP {r.status_code}")
            return None

    return None


def enrich_ads(token_mgr, ad_ids_dict, deadline=None, max_consecutive_429=3):
    """
    Stáhne detaily pro reklamy z ad_ids_dict.

    Ochrana před timeoutem:
      deadline       — unix timestamp (time.time()), zastaví se 30s před
      max_consecutive_429 — přeruší se po N za sebou rate-limitech

    Returns:
      tuple: (details_dict, permanently_failed_ids)
        details_dict — {ad_id: detail_data} pro úspěšně stažené reklamy
        permanently_failed_ids — set ad_ids, které selhaly trvale (HTTP 4xx/5xx, ne rate limit)
    """
    details = {}
    permanently_failed = set()
    total = len(ad_ids_dict)
    failed = 0
    consecutive_429 = 0
    STOP_MARGIN_S = 40  # zastavit detail phase 40s před deadline

    logging.info(f"[DETAIL] Stahuji detaily pro {total} reklam...")
    if deadline:
        remaining = deadline - time.time()
        logging.info(f"[DETAIL] Časový limit: {remaining:.0f}s")
    t_start = time.time()

    for idx, ad_id in enumerate(ad_ids_dict, 1):
        # Zkontroluj time budget
        if deadline and time.time() > deadline - STOP_MARGIN_S:
            logging.warning(
                f"[DETAIL] Časový limit — zastavuji po {idx - 1}/{total} "
                f"({len(details)} OK, {failed} failed)"
            )
            break

        if idx % 25 == 1 or idx == total:
            elapsed = time.time() - t_start
            rate = idx / elapsed if elapsed > 0 else 0
            eta = (total - idx) / rate if rate > 0 else 0
            logging.info(
                f"    {idx}/{total} | ok: {len(details)} failed: {failed} "
                f"| {rate:.1f}/s, ETA {eta / 60:.0f}min"
            )

        try:
            detail = fetch_detail(token_mgr, ad_id)
        except Exception as e:
            logging.error(f"  [X] {e}")
            break

        if detail is _RATE_LIMITED:
            consecutive_429 += 1
            failed += 1
            logging.warning(
                f"  [!] Rate limit #{consecutive_429} z {max_consecutive_429} "
                f"(zastaví detail phase)"
            )
            if consecutive_429 >= max_consecutive_429:
                logging.warning(
                    f"[DETAIL] {max_consecutive_429}x rate limit za sebou — "
                    f"quota vyčerpána, přerušuji detail phase"
                )
                break
        elif detail:
            details[ad_id] = detail
            consecutive_429 = 0  # reset po úspěchu
        else:
            # Trvalá chyba (HTTP 400, 5xx bez retry, API error) — přeskočit
            permanently_failed.add(ad_id)
            failed += 1

    if permanently_failed:
        logging.warning(
            f"[DETAIL] {len(permanently_failed)} reklam trvale selhalo (HTTP 4xx/5xx) "
            f"— budou přeskočeny: {list(permanently_failed)[:5]}{'...' if len(permanently_failed) > 5 else ''}"
        )
    logging.info(f"[DETAIL] Hotovo: {len(details)}/{total} (failed: {failed})")
    return details, permanently_failed


# ============================================================
# BUILD BRONZE DATA — Library + Detail → pipeline-compatible JSON
# ============================================================

def _extract_ads_shell(ad_ids_dict):
    """
    Z pending dict vytáhne `{ad_id: {ad: {...}, advertiser: {...}}}` pro bronze.

    Podporuje obě generace pending formátu:
    - "discovery_data" (query/ads, aktuální) — payload je už v target shape
    - "library_data" (Library API, legacy) — fallback s konverzí timestamp/status

    Nový pending vždy používá "discovery_data". Legacy větev je pro
    případ, že by v Blob Storage zbyly nezpracované pending z předchozí verze.
    """
    ads = {}
    for ad_id, info in ad_ids_dict.items():
        biz_id = info.get("business_id")
        adv_name = info.get("advertiser_name", "")

        if "discovery_data" in info:
            disc = info["discovery_data"]
            ads[ad_id] = {
                "ad": disc.get("ad", {"id": int(ad_id) if str(ad_id).isdigit() else ad_id}),
                "advertiser": disc.get("advertiser") or {
                    "business_id": biz_id,
                    "business_name": adv_name,
                },
            }
        elif "library_data" in info:  # legacy
            lib = info["library_data"]
            ads[ad_id] = {
                "ad": {
                    "id": int(ad_id) if str(ad_id).isdigit() else ad_id,
                    "first_shown_date": _ms_to_datestr(lib.get("first_shown_date")),
                    "last_shown_date": _ms_to_datestr(lib.get("last_shown_date")),
                    "status": _map_audit_status(lib.get("audit_status")),
                    "reach": {"unique_users_seen": lib.get("estimated_audience", "")},
                },
                "advertiser": {"business_id": biz_id, "business_name": adv_name},
            }
        else:
            ads[ad_id] = {
                "ad": {"id": int(ad_id) if str(ad_id).isdigit() else ad_id},
                "advertiser": {"business_id": biz_id, "business_name": adv_name},
            }
    return ads


def build_bronze_data(sector_key, sector_config, ad_ids_dict, details, lookback_days):
    """
    Sestaví bronze JSON kompatibilní s transform pipeline.
    Discovery data (query/ads) jsou již v target shape, Detail data 1:1 z oficiálního API.
    """
    ads = _extract_ads_shell(ad_ids_dict)
    return {
        "metadata": {
            "sector": sector_key,
            "sector_name": sector_config["display_name"],
            "collection_method": "official_query_detail",
            "collected_at": datetime.utcnow().isoformat(),
            "lookback_days": lookback_days,
            "business_ids": {str(k): v for k, v in sector_config["advertisers"].items()},
            "total_ads_unique": len(ads),
            "total_details": len(details),
            "source": {
                "ad_ids": "open.tiktokapis.com/v2/research/adlib/ad/query/",
                "details": "open.tiktokapis.com/v2/research/adlib/ad/detail/",
            },
        },
        "ads": ads,
        "details": {str(k): v for k, v in details.items()},
    }


def build_bronze_batch(sector_key, sector_name, batch_ads_dict, batch_details, lookback_days):
    """
    Sestaví bronze JSON pro BATCH enrichovaných reklam.

    batch_ads_dict: {ad_id: {"discovery_data": {...}, "business_id": ..., "advertiser_name": ...}}
                    (legacy "library_data" je tolerován viz _extract_ads_shell)
    batch_details: {ad_id: detail_data_from_api}
    """
    ads = _extract_ads_shell(batch_ads_dict)
    business_ids_seen = {
        str(info["business_id"]): info.get("advertiser_name", "")
        for info in batch_ads_dict.values()
        if info.get("business_id")
    }

    return {
        "metadata": {
            "sector": sector_key,
            "sector_name": sector_name,
            "collection_method": "official_query_detail",
            "collected_at": datetime.utcnow().isoformat(),
            "lookback_days": lookback_days,
            "business_ids": business_ids_seen,
            "total_ads_unique": len(ads),
            "total_details": len(batch_details),
            "batch": True,
            "source": {
                "ad_ids": "open.tiktokapis.com/v2/research/adlib/ad/query/",
                "details": "open.tiktokapis.com/v2/research/adlib/ad/detail/",
            },
        },
        "ads": ads,
        "details": {str(k): v for k, v in batch_details.items()},
    }
