"""
Transform utilities — bronze → silver.

Multi-platform flatten funkce pro převod nested JSON na flat CSV záznamy:
- TikTok: flatten_ad() — z hybrid Library+Detail API dat
- Meta: flatten_meta_ad() — z Graph API /ads_archive dat
- LinkedIn: flatten_linkedin_ad() — z Ad Library API dat

Každá platforma má vlastní SILVER_COLUMNS definici, protože API
poskytují odlišná data (viz docs/meta_integration_design.md).
"""

import csv
import io
import json


# ============================================================
# HELPER FUNKCE
# ============================================================

def parse_reach(reach_str):
    """Parsuje reach string na int.
    Přesné číslo: '163K' → 163000, '1.5M' → 1500000, '500' → 500
    Interval (jen TikTok reach_total): '100K-200K' → 150000 (střed)
    """
    if not reach_str:
        return None
    s = str(reach_str).strip()

    def _parse_single(part):
        part = part.strip()
        if part.endswith("M"):
            return float(part[:-1]) * 1_000_000
        if part.endswith("K"):
            return float(part[:-1]) * 1_000
        return float(part)

    if "-" in s:
        # Interval — vezmi střed (platí jen pro reach_total u TikToku)
        parts = s.split("-", 1)
        try:
            lo = _parse_single(parts[0])
            hi = _parse_single(parts[1])
            return int((lo + hi) / 2)
        except (ValueError, TypeError):
            return None

    try:
        return int(_parse_single(s))
    except (ValueError, TypeError):
        return None


def format_date(d):
    """'20250113' → '2025-01-13'"""
    if not d:
        return ""
    d = str(d)
    if len(d) == 8:
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return d


def extract_age_range(age_dict):
    """{'18-24': true, '25-34': false, ...} → '18-24'"""
    if not age_dict:
        return ""
    active = [k for k, v in age_dict.items() if v]
    return ", ".join(sorted(active)) if active else "all"


def extract_gender(gender_dict):
    """{'female': true, 'male': true, 'other_genders': true} → 'all'"""
    if not gender_dict:
        return ""
    active = [k for k, v in gender_dict.items() if v]
    if len(active) >= 3 or not active:
        return "all"
    return ", ".join(active)


def extract_videos(det_ad):
    """Extrahuje video URL a cover image z detail dat."""
    videos = det_ad.get("videos", [])
    video_urls = []
    cover_urls = []
    for v in videos:
        if v.get("url"):
            video_urls.append(v["url"])
        if v.get("cover_image_url"):
            cover_urls.append(v["cover_image_url"])
    return video_urls, cover_urls


def extract_reach_by_country(det_ad):
    """Extrahuje reach breakdown po zemích jako string."""
    reach_data = det_ad.get("reach", {})
    by_country = reach_data.get("unique_users_seen_by_country", {})
    if not by_country:
        return ""
    parts = [f"{k}: {v}" for k, v in sorted(by_country.items())]
    return " | ".join(parts)


# ============================================================
# FLATTEN AD — hlavní transformační funkce
# ============================================================

def flatten_ad(ad_id, ad_data, detail_data=None):
    """
    Sloučí query + detail data jedné reklamy do flat dictu.

    Args:
        ad_id: ID reklamy (str)
        ad_data: Data z query endpointu (obsahuje 'ad' a 'advertiser' klíče)
        detail_data: Data z detail endpointu (nebo None)

    Returns:
        dict s flat poli (ready pro CSV řádek)
    """
    ad = ad_data.get("ad", {})
    adv = ad_data.get("advertiser", {})
    det = detail_data or {}
    tgt = det.get("ad_group", {}).get("targeting_info", {}) if det else {}
    det_ad = det.get("ad", {}) if det else {}
    det_adv = det.get("advertiser", {}) if det else {}

    # Reach
    reach_raw = ad.get("reach", {}).get("unique_users_seen", "")
    reach_by_country = det_ad.get("reach", {}).get("unique_users_seen_by_country", {}) if det_ad else {}
    reach_cz = reach_by_country.get("CZ", "")

    # Video/Image URLs
    video_urls, cover_urls = extract_videos(det_ad) if det_ad else ([], [])
    image_urls = det_ad.get("image_urls", []) if det_ad else []

    return {
        # Identifikace
        "ad_id": ad_id,
        "ad_url": f"https://library.tiktok.com/ads/detail/?ad_id={ad_id}" if ad_id else "",
        "advertiser": adv.get("business_name", ""),
        "business_id": adv.get("business_id", ""),
        "paid_for_by": det_adv.get("paid_for_by", ""),

        # Status + Timing
        "status": ad.get("status", ""),
        "first_shown": format_date(ad.get("first_shown_date")),
        "last_shown": format_date(ad.get("last_shown_date")),

        # Reach (parsováno na int; reach_total je u TikToku střed intervalu)
        "reach_total": parse_reach(reach_raw),
        "reach_cz": parse_reach(reach_cz),
        "reach_by_country": extract_reach_by_country(det_ad) if det_ad else "",

        # Targeting
        "target_country": ", ".join(tgt.get("country", [])),
        "target_age": extract_age_range(tgt.get("age")),
        "target_gender": extract_gender(tgt.get("gender")),
        "target_interest": str(tgt.get("interest", "")),
        "audience_targeting": str(tgt.get("audience_targeting", "")),
        "video_interactions": str(tgt.get("video_interactions", "")),
        "creator_interactions": str(tgt.get("creator_interactions", "")),
        "users_targeted": str(tgt.get("number_of_users_targeted", "")),

        # Kreativa (časově omezené signed URLs)
        "video_url": video_urls[0] if video_urls else "",
        "video_cover_url": cover_urls[0] if cover_urls else "",
        "image_url": image_urls[0] if image_urls else "",
        "video_count": len(video_urls),
        "image_count": len(image_urls),
        "all_video_urls": " | ".join(video_urls) if len(video_urls) > 1 else "",
        "all_image_urls": " | ".join(image_urls) if len(image_urls) > 1 else "",

        # Meta
        "has_detail": "yes" if det else "no",
    }


# ============================================================
# CZ RELEVANCE CHECK
# ============================================================

def is_cz_relevant(detail_data):
    """
    Vrátí True pokud je reklama CZ-relevantní.

    TikTok ad/query nepodporuje filtr dle země, takže Discovery vrací všechny
    reklamy daného business_id globálně (např. Xiaomi má reklamy pro různé
    regiony). CZ relevance se proto vyhodnocuje až z ad/detail odpovědi:

    - Pokud nemá detail → True (benefit of the doubt, ještě nedoběhl enrichment)
    - Pokud má detail a `targeting_info.country` obsahuje CZ → True
    - Pokud má detail a `reach.unique_users_seen_by_country` obsahuje CZ → True
    - Pokud má detail bez jakéhokoli CZ signálu → False (non-CZ reklama)

    Druhá podmínka pokrývá případy, kdy TikTok nevrátí targeting_info.country,
    ale vrátí reach breakdown — to je dostatečný důkaz, že reklama byla v CZ
    zobrazena.
    """
    if not detail_data:
        return True  # Bez detailu nevíme, necháme ji

    # Signál 1: explicitní CZ v targeting
    countries = (
        detail_data
        .get("ad_group", {})
        .get("targeting_info", {})
        .get("country", [])
    )
    if "CZ" in countries:
        return True

    # Signál 2: CZ v reach breakdown
    reach_by_country = (
        detail_data
        .get("ad", {})
        .get("reach", {})
        .get("unique_users_seen_by_country", {})
    ) or {}
    if "CZ" in reach_by_country:
        return True

    # Žádný CZ signál → reklama není pro CZ
    return False


# ============================================================
# CSV GENERATION
# ============================================================

# Definice sloupců pro silver CSV
SILVER_COLUMNS = [
    "ad_id", "ad_url", "advertiser", "business_id", "paid_for_by",
    "status", "first_shown", "last_shown",
    "reach_total", "reach_cz", "reach_by_country",
    "target_country", "target_age", "target_gender",
    "target_interest", "audience_targeting",
    "video_interactions", "creator_interactions", "users_targeted",
    "video_url", "video_cover_url", "image_url",
    "video_count", "image_count",
    "all_video_urls", "all_image_urls",
    "has_detail",
]


def rows_to_csv(rows):
    """
    Převede list flat dictů na CSV string (UTF-8 BOM pro Excel).

    Backfill: pokud řádek nemá ad_url (starší silver bez tohoto sloupce),
    dopočítá se z ad_id jako stabilní URL na TikTok Ad Library.

    Args:
        rows: list of dicts (z flatten_ad)

    Returns:
        str — celý CSV soubor jako string
    """
    if not rows:
        return ""

    # Backfill ad_url pro řádky, které ho nemají (starší silver před přidáním sloupce)
    for row in rows:
        if not row.get("ad_url") and row.get("ad_id"):
            row["ad_url"] = f"https://library.tiktok.com/ads/detail/?ad_id={row['ad_id']}"

    output = io.StringIO()
    # UTF-8 BOM pro Excel
    output.write("\ufeff")

    writer = csv.DictWriter(output, fieldnames=SILVER_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

    return output.getvalue()


def csv_to_rows(csv_text):
    """
    Parsuje CSV string zpět na list dictů.
    Používá se pro načtení existujícího silver CSV při merge.

    Args:
        csv_text: str — obsah CSV souboru

    Returns:
        list of dicts
    """
    if not csv_text:
        return []
    reader = csv.DictReader(io.StringIO(csv_text))
    return list(reader)


def merge_rows(existing_rows, new_rows):
    """
    Mergne existující silver řádky s novými. Upsert by ad_id:
    - Nový ad_id → přidej
    - Existující ad_id → přepiš novější verzí (aktualizovaný reach, status...)

    Preferuje nové řádky, protože obsahují čerstvější data z API.

    Args:
        existing_rows: list of dicts (z aktuálního silver CSV)
        new_rows: list of dicts (z nového bronze)

    Returns:
        list of dicts — merged, deduplicated, seřazené
    """
    # Index by ad_id — nové přepíšou staré
    merged = {}
    for row in existing_rows:
        ad_id = row.get("ad_id", "")
        if ad_id:
            merged[ad_id] = row

    new_count = 0
    updated_count = 0
    for row in new_rows:
        ad_id = row.get("ad_id", "")
        if not ad_id:
            continue
        if ad_id in merged:
            existing = merged[ad_id]
            # Preferuj verzi s detailem — nepřepisuj has_detail=yes novým has_detail=no
            if existing.get("has_detail") == "yes" and row.get("has_detail") == "no":
                # Zachovej detail data, ale aktualizuj status, reach a datumy z nového
                for field in ("status", "last_shown", "reach_total"):
                    if row.get(field):
                        existing[field] = row[field]
                updated_count += 1
                continue  # Nepřepiš celý řádek
            updated_count += 1
        else:
            new_count += 1
        merged[ad_id] = row

    rows = list(merged.values())
    rows.sort(key=lambda r: (str(r.get("advertiser", "")).lower(), str(r.get("first_shown", ""))))

    return rows, new_count, updated_count


# ============================================================
# META — FLATTEN AD
# ============================================================

def flatten_meta_ad(ad_id, ad_data, coverage_labels=None):
    """
    Převede jednu Meta reklamu z bronze JSON na flat dict pro silver CSV.

    Meta Graph API /ads_archive vrací bohatší data než TikTok:
    - Přesné targeting parametry (věk, pohlaví, lokace)
    - EU reach breakdown po zemích (DSA čl. 39)
    - Beneficiary/payer identifikace
    - Kreativní obsah (text, titulek, snapshot URL)

    Args:
        ad_id: str — Meta ad ID
        ad_data: dict — surová data z API (bronze)
        coverage_labels: list[str] — zdroje (page_name, term:keyword)

    Returns:
        dict — flat záznam pro CSV
    """
    # EU reach (parsováno na int)
    eu_reach = parse_reach(ad_data.get("eu_total_reach", ""))

    # CZ reach + reach_breakdown po zemích
    cz_reach = ""
    reach_breakdown = {}
    breakdown = ad_data.get("age_country_gender_reach_breakdown", [])
    if isinstance(breakdown, list):
        for entry in breakdown:
            if not isinstance(entry, dict):
                continue
            country = entry.get("country")
            total = sum(
                ag.get("male", 0) + ag.get("female", 0) + ag.get("unknown", 0)
                for ag in entry.get("age_gender_breakdowns", [])
            )
            if country and total > 0:
                reach_breakdown[country] = total
                if country == "CZ":
                    cz_reach = total

    # Beneficiary/payer (DSA povinnost)
    beneficiary_payers = ad_data.get("beneficiary_payers", [])
    beneficiary = beneficiary_payers[0].get("beneficiary", "") if beneficiary_payers and isinstance(beneficiary_payers, list) else ""
    payer = beneficiary_payers[0].get("payer", "") if beneficiary_payers and isinstance(beneficiary_payers, list) else ""

    # Platforms
    platforms = ad_data.get("publisher_platforms", [])
    if isinstance(platforms, list):
        platforms = "|".join(platforms)

    # Creative
    bodies = ad_data.get("ad_creative_bodies", [])
    body = bodies[0] if bodies else ""
    titles = ad_data.get("ad_creative_link_titles", [])
    title = titles[0] if titles else ""
    captions = ad_data.get("ad_creative_link_captions", [])
    caption = captions[0] if captions else ""
    descriptions = ad_data.get("ad_creative_link_descriptions", [])
    description = descriptions[0] if descriptions else ""

    # Target locations
    locations = ad_data.get("target_locations", [])
    loc_names = []
    if isinstance(locations, list):
        for loc in locations:
            if isinstance(loc, dict):
                loc_names.append(loc.get("name", ""))

    # Target ages
    target_ages = ad_data.get("target_ages", "")
    if isinstance(target_ages, list) and len(target_ages) == 2:
        target_ages = f"{target_ages[0]}-{target_ages[1]}"
    elif isinstance(target_ages, list):
        target_ages = str(target_ages)

    return {
        "ad_id": ad_id,
        "page_name": ad_data.get("page_name", ""),
        "page_id": ad_data.get("page_id", ""),
        "delivery_start": ad_data.get("ad_delivery_start_time", ""),
        "delivery_stop": ad_data.get("ad_delivery_stop_time", ""),
        "platforms": platforms,
        "target_ages": target_ages,
        "target_gender": ad_data.get("target_gender", ""),
        "target_locations": "|".join(loc_names),
        "eu_total_reach": eu_reach,
        "cz_reach": cz_reach,
        "reach_breakdown": json.dumps(reach_breakdown, ensure_ascii=False) if reach_breakdown else "",
        "beneficiary": beneficiary,
        "payer": payer,
        "languages": "|".join(ad_data.get("languages", []))
                     if isinstance(ad_data.get("languages"), list) else "",
        "creative_title": title,
        "creative_caption": caption,
        "creative_description": description,
        "creative_body": body[:500],
        "snapshot_url": ad_data.get("ad_snapshot_url", ""),
        "source": "|".join(sorted(set(coverage_labels or []))),
    }


# Meta silver CSV sloupce
META_SILVER_COLUMNS = [
    "ad_id", "page_name", "page_id",
    "delivery_start", "delivery_stop",
    "platforms",
    "target_ages", "target_gender", "target_locations",
    "eu_total_reach", "cz_reach", "reach_breakdown",
    "beneficiary", "payer",
    "languages",
    "creative_title", "creative_caption", "creative_description", "creative_body",
    "snapshot_url", "source",
]


def meta_rows_to_csv(rows):
    """Převede Meta flat dicty na CSV string (UTF-8 BOM pro Excel)."""
    if not rows:
        return ""

    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.DictWriter(output, fieldnames=META_SILVER_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


# ============================================================
# META REACH DETAIL — long format breakdown tabulka
# ============================================================

META_REACH_DETAIL_COLUMNS = [
    "ad_id", "page_name", "country", "age_range", "male", "female", "unknown",
]


def extract_reach_detail_rows(ad_id, ad_data):
    """
    Z jedné reklamy vygeneruje řádky pro reach_detail.csv.
    Jeden řádek = jedna kombinace country × age_range.
    """
    rows = []
    breakdown = ad_data.get("age_country_gender_reach_breakdown", [])
    if not isinstance(breakdown, list):
        return rows

    page_name = ad_data.get("page_name", "")
    for entry in breakdown:
        if not isinstance(entry, dict):
            continue
        country = entry.get("country", "")
        for ag in entry.get("age_gender_breakdowns", []):
            male = ag.get("male", 0)
            female = ag.get("female", 0)
            unknown = ag.get("unknown", 0)
            if male + female + unknown == 0:
                continue
            rows.append({
                "ad_id": ad_id,
                "page_name": page_name,
                "country": country,
                "age_range": ag.get("age_range", ""),
                "male": male,
                "female": female,
                "unknown": unknown,
            })
    return rows


def merge_reach_detail_rows(existing_rows, new_rows):
    """
    Upsert reach_detail řádků by ad_id.
    Všechny staré řádky pro daný ad_id se nahradí novými
    (reach data se mění každý cyklus).
    """
    new_ad_ids = {r["ad_id"] for r in new_rows}
    kept = [r for r in existing_rows if r["ad_id"] not in new_ad_ids]
    merged = kept + new_rows
    merged.sort(key=lambda r: (r["ad_id"], r["country"], r["age_range"]))
    new_count = len(new_ad_ids - {r["ad_id"] for r in existing_rows})
    updated_count = len(new_ad_ids) - new_count
    return merged, new_count, updated_count


def reach_detail_to_csv(rows):
    """Převede reach_detail řádky na CSV string (UTF-8 BOM pro Excel)."""
    if not rows:
        return ""

    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.DictWriter(output, fieldnames=META_REACH_DETAIL_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


# ============================================================
# LINKEDIN — FLATTEN AD
# ============================================================

def flatten_linkedin_ad(ad_url, ad_data, coverage_labels=None):
    """
    Převede jednu LinkedIn reklamu z bronze JSON na flat dict pro silver CSV.

    LinkedIn Ad Library API vrací:
    - Advertiser + payer identifikace
    - Impressions jako range (from-to)
    - Country distribution (% per country)
    - Targeting facety (Location, Job Title, Seniority, Industry, ...)
    - Ad type (SPONSORED_STATUS_UPDATE, etc.)

    Args:
        ad_url: str — unikátní URL reklamy (identifikátor)
        ad_data: dict — surová data z API (bronze)
        coverage_labels: list[str] — zdroje (advertiser names)

    Returns:
        dict — flat záznam pro CSV
    """
    details = ad_data.get("details", {})
    advertiser = details.get("advertiser", {})
    stats = details.get("adStatistics", {})
    targeting = details.get("adTargeting", [])
    # _categorization odstraněna — CZ filtrace na úrovni API (countries=CZ)

    # Impressions range
    total_impressions = stats.get("totalImpressions", {})
    impressions_min = total_impressions.get("from", "") if total_impressions else ""
    impressions_max = total_impressions.get("to", "") if total_impressions else ""

    # CZ impressions
    # API vrací impressionPercentage přímo v procentech (0-100), ne jako podíl (0-1)
    cz_pct = 0.0
    impressions_by_country = {}
    for c in stats.get("impressionsDistributionByCountry", []):
        country = c.get("country", "")
        pct = c.get("impressionPercentage", 0)
        # Zkrátit URN: "urn:li:country:CZ" → "CZ"
        short = country.replace("urn:li:country:", "") if country else ""
        if pct > 0:
            impressions_by_country[short] = round(pct, 2)
        if short == "CZ":
            cz_pct = pct

    # Estimated CZ impressions
    midpoint = 0
    if impressions_min != "" and impressions_max != "":
        try:
            midpoint = (int(impressions_min) + int(impressions_max)) / 2
        except (ValueError, TypeError):
            pass
    estimated_cz = midpoint * (cz_pct / 100) if cz_pct > 0 else 0

    # Targeting facety
    facet_names = []
    target_locations = []
    for t in targeting:
        facet = t.get("facetName", "")
        if facet:
            facet_names.append(facet)
        if facet == "Location":
            included = t.get("includedSegments", [])
            target_locations.extend(included)

    return {
        "ad_url": ad_url,
        "advertiser_name": advertiser.get("advertiserName", ""),
        "payer": advertiser.get("adPayer", ""),
        "ad_type": details.get("type", ""),
        "first_impression": stats.get("firstImpressionAt", ""),
        "last_impression": stats.get("latestImpressionAt", ""),
        "impressions_min": impressions_min,
        "impressions_max": impressions_max,
        "cz_impressions_pct": round(cz_pct, 2),
        "estimated_cz_impressions": round(estimated_cz),
        "target_locations": "|".join(target_locations),
        "targeting_facets": "|".join(facet_names),
        "impressions_by_country": json.dumps(impressions_by_country, ensure_ascii=False)
                                  if impressions_by_country else "",
        "source": "|".join(sorted(set(coverage_labels or []))),
    }


# LinkedIn silver CSV sloupce
LINKEDIN_SILVER_COLUMNS = [
    "ad_url", "advertiser_name", "payer", "ad_type",
    "first_impression", "last_impression",
    "impressions_min", "impressions_max",
    "cz_impressions_pct", "estimated_cz_impressions",
    "target_locations", "targeting_facets",
    "impressions_by_country", "source",
]


def linkedin_rows_to_csv(rows):
    """Převede LinkedIn flat dicty na CSV string (UTF-8 BOM pro Excel)."""
    if not rows:
        return ""

    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.DictWriter(output, fieldnames=LINKEDIN_SILVER_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def merge_linkedin_rows(existing_rows, new_rows):
    """
    Mergne existující LinkedIn silver řádky s novými. Upsert by ad_url.
    Nové přepisují staré (čerstvější data z API).
    """
    merged = {}
    for row in existing_rows:
        ad_url = row.get("ad_url", "")
        if ad_url:
            merged[ad_url] = row

    new_count = 0
    updated_count = 0
    for row in new_rows:
        ad_url = row.get("ad_url", "")
        if not ad_url:
            continue
        if ad_url in merged:
            updated_count += 1
        else:
            new_count += 1
        merged[ad_url] = row

    rows = list(merged.values())
    rows.sort(key=lambda r: (
        str(r.get("advertiser_name", "")).lower(),
        str(r.get("first_impression", "")),
    ))
    return rows, new_count, updated_count


def merge_meta_rows(existing_rows, new_rows):
    """
    Mergne existující Meta silver řádky s novými. Upsert by ad_id.
    Nové přepisují staré (čerstvější data z API).
    """
    merged = {}
    for row in existing_rows:
        ad_id = row.get("ad_id", "")
        if ad_id:
            merged[ad_id] = row

    new_count = 0
    updated_count = 0
    for row in new_rows:
        ad_id = row.get("ad_id", "")
        if not ad_id:
            continue
        if ad_id in merged:
            updated_count += 1
        else:
            new_count += 1
        merged[ad_id] = row

    rows = list(merged.values())
    rows.sort(key=lambda r: (
        str(r.get("page_name", "")).lower(),
        str(r.get("delivery_start", "")),
    ))
    return rows, new_count, updated_count
