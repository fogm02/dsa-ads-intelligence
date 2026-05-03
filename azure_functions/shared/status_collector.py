"""
Pipeline Status Collector — stáhne state, config a silver data z Blobu
a sestaví kompletní přehled stavu pipeline.

Žádná závislost na Azure Functions SDK — reusable pro CLI i HTTP trigger.
"""

import csv
import io
import logging
from datetime import datetime, timezone

from shared.blob_helpers import (
    download_json,
    download_text,
    list_blobs,
    load_advertisers_from_blob,
    load_meta_advertisers_from_blob,
    load_linkedin_advertisers_from_blob,
    load_advertiser_names_from_blob,
)

# State file paths in Blob Storage
DISCOVERY_STATE_PATH = "state/discovery.json"
ENRICHMENT_STATE_PATH = "state/enrichment.json"
META_STATE_PATH = "state/meta_collection.json"
LINKEDIN_STATE_PATH = "state/linkedin_collection.json"


def collect_pipeline_status(container) -> dict:
    """Hlavní orchestrátor — vrací kompletní status dict."""
    # Load configs
    tiktok_cfg = load_advertisers_from_blob(container)
    meta_cfg = load_meta_advertisers_from_blob(container)
    linkedin_cfg = load_linkedin_advertisers_from_blob(container)
    name_map = load_advertiser_names_from_blob(container)  # raw_name → normalized

    # Load state files
    discovery_state = download_json(container, DISCOVERY_STATE_PATH) or {}
    enrichment_state = download_json(container, ENRICHMENT_STATE_PATH) or {}
    meta_state = download_json(container, META_STATE_PATH) or {}
    linkedin_state = download_json(container, LINKEDIN_STATE_PATH) or {}

    # Collect per-platform status
    tiktok = _collect_tiktok_status(container, tiktok_cfg, discovery_state, enrichment_state, name_map)
    meta = _collect_meta_status(container, meta_cfg, meta_state, name_map)
    linkedin = _collect_linkedin_status(container, linkedin_cfg, linkedin_state, name_map)

    # Build unified sector view (cross-platform per advertiser)
    sectors = _build_sector_view(tiktok, meta, linkedin, tiktok_cfg, meta_cfg, linkedin_cfg, name_map)

    # Silver summary
    silver = _silver_summary(tiktok, meta, linkedin)

    # Trend data from gold files
    trend = _collect_trend_data(container)

    # Health assessment
    health = _assess_health(tiktok, meta, linkedin)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "health": health,
        "platforms": {
            "tiktok": tiktok,
            "meta": meta,
            "linkedin": linkedin,
        },
        "trend": trend,
        "sectors": sectors,
        "silver_summary": silver,
    }


# ============================================================
# TIKTOK STATUS
# ============================================================

def _collect_tiktok_status(container, config, discovery_state, enrichment_state, name_map):
    """TikTok: config + discovery + enrichment + silver."""
    disc_sectors = discovery_state.get("sectors", {})
    enrich_sectors = enrichment_state.get("sectors", {})

    sectors = {}
    total_silver = 0
    total_config = 0

    for sector_key, sector_cfg in config.items():
        advertisers_cfg = sector_cfg.get("advertisers", {})  # {biz_id: name}
        disc_sector = disc_sectors.get(sector_key, {})
        disc_advs = disc_sector.get("advertisers", {})
        enrich_sector = enrich_sectors.get(sector_key, {})

        # Load silver data
        silver_raw, silver_counts = _count_silver_per_advertiser(
            container, "tiktok", sector_key, "advertiser", name_map
        )

        advs = []
        for biz_id, name in advertisers_cfg.items():
            biz_id_str = str(biz_id)
            disc_adv = disc_advs.get(biz_id_str, {})
            silver_count = silver_counts.get(name, 0)
            if silver_count == 0:
                silver_count = silver_counts.get(name_map.get(name, name), 0)

            if silver_count > 0:
                status = "ok"
            elif disc_adv.get("status") == "complete":
                status = "discovered"
            elif disc_adv.get("status") == "in_progress":
                status = "in_progress"
            elif enrich_sector.get("status") == "in_progress":
                status = "enriching"
            else:
                status = "missing"

            advs.append({
                "name": name,
                "business_id": biz_id_str,
                "status": status,
                "ads_discovered": disc_adv.get("ads_found", 0),
                "silver_count": silver_count,
            })
            total_config += 1

        total_silver += silver_raw
        sectors[sector_key] = {
            "display_name": sector_cfg.get("display_name", sector_key),
            "status": disc_sector.get("status", "unknown"),
            "enrichment_status": enrich_sector.get("status", "unknown"),
            "advertisers": advs,
            "silver_total": silver_raw,
        }

    return {
        "cycle_id": discovery_state.get("cycle_id", "—"),
        "cycle_started_at": discovery_state.get("cycle_started_at", "—"),
        "total_runs": discovery_state.get("stats", {}).get("total_runs", 0),
        "total_ads_discovered": discovery_state.get("stats", {}).get("total_ads_discovered", 0),
        "enrichment_runs": enrichment_state.get("stats", {}).get("total_runs", 0),
        "total_ads_enriched": enrichment_state.get("stats", {}).get("total_ads_enriched", 0),
        "sectors": sectors,
        "silver_total": total_silver,
        "config_total": total_config,
    }


# ============================================================
# META STATUS
# ============================================================

def _collect_meta_status(container, config, meta_state, name_map):
    """Meta: config + state + silver."""
    state_sectors = meta_state.get("sectors", {})

    sectors = {}
    total_silver = 0
    total_config = 0

    for sector_key, sector_cfg in config.items():
        pages_cfg = sector_cfg.get("pages", {})  # {page_id: page_name}
        state_sector = state_sectors.get(sector_key, {})
        completed_pages = set(state_sector.get("completed_pages", []))

        # Load silver data
        silver_raw, silver_counts = _count_silver_per_advertiser(
            container, "meta", sector_key, "page_name", name_map
        )

        advs = []
        for page_id, page_name in pages_cfg.items():
            # Try exact match, then normalized name
            silver_count = silver_counts.get(page_name, 0)
            if silver_count == 0:
                norm = name_map.get(page_name, page_name)
                silver_count = silver_counts.get(norm, 0)

            if silver_count > 0:
                status = "ok"
            elif page_id in completed_pages:
                status = "collected"
            else:
                status = "missing"

            advs.append({
                "name": page_name,
                "page_id": page_id,
                "status": status,
                "silver_count": silver_count,
            })
            total_config += 1

        total_silver += silver_raw
        sectors[sector_key] = {
            "display_name": sector_cfg.get("display_name", sector_key),
            "status": state_sector.get("status", "unknown"),
            "ads_collected": state_sector.get("ads_collected", 0),
            "last_run_at": state_sector.get("last_run_at", "—"),
            "advertisers": advs,
            "silver_total": silver_raw,
        }

    return {
        "cycle_id": meta_state.get("cycle_id", "—"),
        "sectors": sectors,
        "silver_total": total_silver,
        "config_total": total_config,
    }


# ============================================================
# LINKEDIN STATUS
# ============================================================

def _collect_linkedin_status(container, config, linkedin_state, name_map):
    """LinkedIn: config + state + silver."""
    state_sectors = linkedin_state.get("sectors", {})

    sectors = {}
    total_silver = 0
    total_config = 0

    for sector_key, sector_cfg in config.items():
        advs_cfg = sector_cfg.get("advertisers", [])
        state_sector = state_sectors.get(sector_key, {})
        completed_advs = set(state_sector.get("completed_advertisers", []))

        # Load silver data
        silver_raw, silver_counts = _count_silver_per_advertiser(
            container, "linkedin", sector_key, "advertiser_name", name_map
        )

        advs = []
        for adv_name in advs_cfg:
            silver_count = silver_counts.get(adv_name, 0)
            if silver_count == 0:
                silver_count = silver_counts.get(name_map.get(adv_name, adv_name), 0)

            if silver_count > 0:
                status = "ok"
            elif adv_name in completed_advs:
                status = "collected"
            else:
                status = "missing"

            advs.append({
                "name": adv_name,
                "status": status,
                "silver_count": silver_count,
            })
            total_config += 1

        total_silver += silver_raw
        sectors[sector_key] = {
            "display_name": sector_cfg.get("display_name", sector_key),
            "status": state_sector.get("status", "unknown"),
            "ads_collected": state_sector.get("ads_collected", 0),
            "last_run_at": state_sector.get("last_run_at", "—"),
            "advertisers": advs,
            "silver_total": silver_raw,
        }

    return {
        "cycle_id": linkedin_state.get("cycle_id", "—"),
        "sectors": sectors,
        "silver_total": total_silver,
        "config_total": total_config,
    }


# ============================================================
# SILVER HELPERS
# ============================================================

def _count_silver_per_advertiser(container, platform, sector, name_column, name_map=None):
    """
    Stáhne silver/{platform}/{sector}/current.csv a spočítá řádky per advertiser.
    Pokud je name_map, normalizuje jména (takže "MONETA Money Bank" → "Moneta Money Bank").
    Vrátí (total_rows, {advertiser_name: count}) — klíče jsou jak raw tak normalized.
    """
    path = f"silver/{platform}/{sector}/current.csv"
    text = download_text(container, path)
    if not text:
        return 0, {}

    counts = {}
    total = 0
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        total += 1
        name = row.get(name_column, "").strip()
        if name:
            # Count under raw name
            counts[name] = counts.get(name, 0) + 1
            # Also count under normalized name (so config lookup works)
            if name_map and name in name_map:
                norm = name_map[name]
                if norm != name:
                    counts[norm] = counts.get(norm, 0) + 1
    return total, counts


# ============================================================
# UNIFIED SECTOR VIEW
# ============================================================

def _build_sector_view(tiktok, meta, linkedin, tiktok_cfg, meta_cfg, linkedin_cfg, name_map):
    """
    Sestaví cross-platform pohled per sektor:
    Pro každý sektor seznam advertiserů s jejich statusem na každé platformě.
    Používá name_map (advertiser_names.csv) pro spojení stejných advertiserů
    s různými jmény napříč platformami.
    """
    # Collect all sector keys
    all_sectors = set()
    all_sectors.update(tiktok_cfg.keys())
    all_sectors.update(meta_cfg.keys())
    all_sectors.update(linkedin_cfg.keys())

    def normalize(name):
        return name_map.get(name, name)

    result = {}
    for sector_key in sorted(all_sectors):
        # Get display name from any platform config
        display_name = sector_key
        for cfg in (tiktok_cfg, meta_cfg, linkedin_cfg):
            if sector_key in cfg:
                display_name = cfg[sector_key].get("display_name", sector_key)
                break

        # Build advertiser map keyed by normalized name
        adv_map = {}

        def ensure_entry(raw_name):
            key = normalize(raw_name)
            if key not in adv_map:
                adv_map[key] = {"name": key, "tiktok": None, "meta": None, "linkedin": None}
            return key

        # TikTok advertisers
        tk_sector = tiktok.get("sectors", {}).get(sector_key, {})
        for adv in tk_sector.get("advertisers", []):
            key = ensure_entry(adv["name"])
            adv_map[key]["tiktok"] = {
                "status": adv["status"],
                "count": adv["silver_count"],
            }

        # Meta advertisers
        mt_sector = meta.get("sectors", {}).get(sector_key, {})
        for adv in mt_sector.get("advertisers", []):
            key = ensure_entry(adv["name"])
            adv_map[key]["meta"] = {
                "status": adv["status"],
                "count": adv["silver_count"],
            }

        # LinkedIn advertisers
        li_sector = linkedin.get("sectors", {}).get(sector_key, {})
        for adv in li_sector.get("advertisers", []):
            key = ensure_entry(adv["name"])
            adv_map[key]["linkedin"] = {
                "status": adv["status"],
                "count": adv["silver_count"],
            }

        result[sector_key] = {
            "display_name": display_name,
            "advertisers": list(adv_map.values()),
        }

    return result


# ============================================================
# TREND DATA FROM GOLD FILES
# ============================================================

def _collect_trend_data(container):
    """
    Extracts historical ad counts from cross_platform gold files.
    Parses the 'platform' column to get per-platform breakdown per day.
    Returns {dates: [...], meta: [...], tiktok: [...], linkedin: [...], total: [...]}.
    """
    import re

    blobs = list_blobs(container, "gold/cross_platform_")

    # Parse gold file names → sorted list of (date_str, blob_name)
    gold_files = []
    for blob_name in blobs:
        m = re.match(r"gold/cross_platform_(\d{8})\.csv", blob_name)
        if m:
            gold_files.append((m.group(1), blob_name))

    gold_files.sort()

    if not gold_files:
        return {"dates": [], "meta": [], "tiktok": [], "linkedin": [], "total": []}

    dates = []
    meta_counts = []
    tiktok_counts = []
    linkedin_counts = []
    total_counts = []

    for date_str, blob_name in gold_files:
        counts = _count_platform_lines(container, blob_name)
        formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        dates.append(formatted)
        meta_counts.append(counts.get("meta", 0))
        tiktok_counts.append(counts.get("tiktok", 0))
        linkedin_counts.append(counts.get("linkedin", 0))
        total_counts.append(sum(counts.values()))

    return {
        "dates": dates,
        "meta": meta_counts,
        "tiktok": tiktok_counts,
        "linkedin": linkedin_counts,
        "total": total_counts,
    }


def _count_platform_lines(container, blob_path):
    """
    Rychlé spočítání řádků per platforma v cross_platform gold CSV.
    Čte jen sloupec 'platform' (první sloupec).
    """
    text = download_text(container, blob_path)
    if not text:
        return {}

    counts = {}
    lines = text.split("\n")
    # Skip header, count platform values
    for line in lines[1:]:
        if not line.strip():
            continue
        # platform is the first column
        platform = line.split(",", 1)[0].strip().strip('"')
        if platform:
            counts[platform] = counts.get(platform, 0) + 1
    return counts


# ============================================================
# SILVER SUMMARY & HEALTH
# ============================================================

def _silver_summary(tiktok, meta, linkedin):
    """Souhrnné statistiky silver vrstvy."""
    tk = tiktok.get("silver_total", 0)
    mt = meta.get("silver_total", 0)
    li = linkedin.get("silver_total", 0)

    tk_with_data = sum(
        1 for s in tiktok.get("sectors", {}).values()
        for a in s.get("advertisers", []) if a.get("silver_count", 0) > 0
    )
    mt_with_data = sum(
        1 for s in meta.get("sectors", {}).values()
        for a in s.get("advertisers", []) if a.get("silver_count", 0) > 0
    )
    li_with_data = sum(
        1 for s in linkedin.get("sectors", {}).values()
        for a in s.get("advertisers", []) if a.get("silver_count", 0) > 0
    )

    return {
        "total_ads": tk + mt + li,
        "by_platform": {"tiktok": tk, "meta": mt, "linkedin": li},
        "advertisers_with_data": {
            "tiktok": f"{tk_with_data}/{tiktok.get('config_total', 0)}",
            "meta": f"{mt_with_data}/{meta.get('config_total', 0)}",
            "linkedin": f"{li_with_data}/{linkedin.get('config_total', 0)}",
        },
    }


def _assess_health(tiktok, meta, linkedin):
    """Určí celkový health status a seznam issues."""
    issues = []

    # Check cycle IDs exist
    if tiktok.get("cycle_id", "—") == "—":
        issues.append("TikTok: discovery state chybí nebo prázdný")
    if meta.get("cycle_id", "—") == "—":
        issues.append("Meta: collection state chybí nebo prázdný")
    if linkedin.get("cycle_id", "—") == "—":
        issues.append("LinkedIn: collection state chybí nebo prázdný")

    # Check for sectors with 0 silver data
    for platform_name, platform_data in [("TikTok", tiktok), ("Meta", meta), ("LinkedIn", linkedin)]:
        for sector_key, sector_data in platform_data.get("sectors", {}).items():
            if sector_data.get("silver_total", 0) == 0:
                issues.append(f"{platform_name}/{sector_key}: 0 reklam v silveru")

    # Check last_run_at freshness for Meta and LinkedIn
    now = datetime.now(timezone.utc)
    for platform_name, platform_data, max_hours in [("Meta", meta, 8), ("LinkedIn", linkedin, 8)]:
        for sector_key, sector_data in platform_data.get("sectors", {}).items():
            last_run = sector_data.get("last_run_at", "—")
            if last_run != "—":
                try:
                    dt = datetime.fromisoformat(last_run)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    hours_ago = (now - dt).total_seconds() / 3600
                    if hours_ago > max_hours:
                        issues.append(
                            f"{platform_name}/{sector_key}: poslední běh před {hours_ago:.0f}h"
                        )
                except (ValueError, TypeError):
                    pass

    if not issues:
        status = "healthy"
    elif any("chybí" in i or "error" in i.lower() for i in issues):
        status = "error"
    else:
        status = "warning"

    return {"status": status, "issues": issues}
