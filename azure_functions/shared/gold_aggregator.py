"""
Gold Layer — unified cross-platform Parquet + Meta reach detail.

Sjednocuje silver data ze všech platforem (TikTok, Meta, LinkedIn) do jednoho
Parquet souboru s normalizovanými sloupci pro dashboard / BI vizualizaci.

Meta reach_detail zůstává separátní (jiná granularita: 1 řádek = ad × country × age).

Žádná agregace — zachovává plný detail každé reklamy.
Agregace (SoV, ad count, ...) se dělají až v BI nástroji.
"""

import csv
import io
import logging
import re
import unicodedata

import pandas as pd

from shared.blob_helpers import (
    download_text,
    load_advertiser_names_from_blob,
    _canonical_advertiser_key,
)


# ============================================================
# DASHBOARD PRECOMPUTE — přesunuté transformace z dashboard/app.py
# do gold layeru pro rychlejší startup dashboardu
# ============================================================

AGE_BOUNDS = [(13, 17), (18, 24), (25, 34), (35, 44),
              (45, 54), (55, 64), (65, 99)]

LEGAL_SUFFIXES = [
    ", s.r.o.", ", s.r.o", ", a.s.", ", a. s.", ", a.s", ", a. s",
    " s.r.o.", " s.r.o", " a.s.", " a. s.", " a.s", " a. s",
    " spol. s r.o.", " spol. s r.o",
    " gmbh", " ltd.", " ltd", " inc.", " inc", " llc",
    " czech republic", " czechia", " pte.", " pte", ".cz", " cz", " ce",
]

PAYER_ALIASES = {
    "dentsu": "dentsu media services",
    "dentsu media services": "dentsu media services",
}


def _strip_diacritics(s):
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")


def _norm_payer(s):
    """Normalizace plátce: lowercase + ASCII fold + odstranění právních forem + alias."""
    if not s or pd.isna(s):
        return ""
    n = _strip_diacritics(str(s).strip().lower())
    changed = True
    while changed:
        changed = False
        for suf in LEGAL_SUFFIXES:
            if n.endswith(suf):
                n = n[:-len(suf)].rstrip(" ,")
                changed = True
    key = re.sub(r"[^a-z0-9]+", " ", n).strip()
    return PAYER_ALIASES.get(key, key)


def _classify_pub(platform, publisher_platforms):
    """Klasifikuje publisher pro Meta (facebook/instagram/oba); jinak vrací platform."""
    if platform != "meta":
        return platform
    pubs = set(p.strip().lower() for p in str(publisher_platforms or "").split("|") if p.strip())
    has_fb, has_ig = "facebook" in pubs, "instagram" in pubs
    if has_fb and has_ig:
        return "facebook+instagram"
    if has_fb:
        return "facebook"
    if has_ig:
        return "instagram"
    return "meta_other"


def _parse_targeting(platform, targeting_summary):
    """Parse targeting_summary string → (narrow, gender, interests).

    Vrací:
        narrow (bool): True pokud ≤3 věkové buckety nebo gender restrikce
        gender (str): "all" | "male" | "female"
        interests (str): extrahované interesty (jen TikTok)
    """
    ts = str(targeting_summary or "")
    plat = str(platform or "")
    narrow, gender, interests = False, "all", ""
    if not ts or ts == "nan":
        return narrow, gender, interests
    parts = [p.strip() for p in ts.split("|")]
    n_buckets = 7
    if plat == "meta":
        if len(parts) >= 2 and "-" in parts[1]:
            try:
                lo, hi = int(parts[1].split("-")[0]), int(parts[1].split("-")[1])
                n_buckets = sum(1 for blo, bhi in AGE_BOUNDS if lo <= bhi and hi >= blo)
            except (ValueError, IndexError):
                pass
        if len(parts) >= 3:
            g = parts[2].strip().lower()
            if g in ("women", "ženy"):
                gender = "female"
            elif g in ("men", "muži"):
                gender = "male"
    elif plat == "tiktok":
        if len(parts) >= 2:
            n_buckets = len([a.strip() for a in parts[1].split(",") if a.strip()])
        if len(parts) >= 3:
            g = parts[2].strip().lower()
            if g == "female":
                gender = "female"
            elif g == "male":
                gender = "male"
        if len(parts) >= 4 and parts[3].strip():
            interests = parts[3].strip()
    narrow = n_buckets <= 3 or gender != "all"
    return narrow, gender, interests


def _resolve_advertiser_name(raw_name, name_map):
    """
    Resolve raw advertiser name → normalized canonical name.
    Postup:
    1. exact match (rychlá cesta)
    2. canonical key (case/diakritika/interpunkce insensitive)
    3. fallback: vrátí raw_name
    """
    if not raw_name:
        return raw_name
    if raw_name in name_map:
        return name_map[raw_name]
    canon = _canonical_advertiser_key(raw_name)
    if canon and canon in name_map:
        return name_map[canon]
    return raw_name


# ============================================================
# HELPERS — reach parsing, timestamp normalization
# ============================================================

def _parse_reach(s):
    """Parse reach string ('1.2M', '500-1000', '3K') to int. Returns 0 on failure."""
    if pd.isna(s) or s in ("", "—", "None", "nan"):
        return 0
    s = str(s).strip().replace(",", "")
    # Range like "500-1000" → midpoint
    if "-" in s and not s.startswith("-"):
        parts = s.split("-")
        if len(parts) == 2:
            return (_parse_reach(parts[0]) + _parse_reach(parts[1])) // 2
    mul = 1
    su = s.upper()
    if su.endswith("M"):
        mul, s = 1_000_000, s[:-1]
    elif su.endswith("K"):
        mul, s = 1_000, s[:-1]
    elif su.endswith("B"):
        mul, s = 1_000_000_000, s[:-1]
    try:
        return int(float(s) * mul)
    except (ValueError, OverflowError):
        return 0


def _normalize_timestamp(val):
    """Convert ms-epoch timestamps (TikTok) to YYYY-MM-DD. Pass through dates."""
    if not val or pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip()
    if re.match(r"^\d{10,}$", s):
        try:
            return pd.Timestamp(int(s), unit="ms").strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            return s
    return s


# ============================================================
# UNIFIED CROSS-PLATFORM GOLD COLUMNS
# ============================================================

CROSS_PLATFORM_COLUMNS = [
    "platform",
    "sector", "sector_name", "sector_display",
    "ad_id", "advertiser", "advertiser_normalized", "payer",
    "date_start", "date_stop",
    "reach_total", "reach_cz",
    "ad_type",
    "creative_url",
    "targeting_summary",
    "targeting_facets",  # LinkedIn-specific facets (Job|Company|...)
    "source",
    "publisher_platforms",
    # Precomputed dashboard fields (přesunuto z dashboard/app.py kvůli startup performance)
    "pub_platform",     # facebook | instagram | facebook+instagram | tiktok | linkedin | meta_other
    "payer_norm",       # normalizovaný payer (lowercase + ASCII fold + odstranění právních forem)
    "tgt_narrow",       # bool — úzké cílení (≤3 věkové buckety nebo gender restrikce)
    "tgt_gender",       # all | male | female
    "tgt_interests",    # extrahovaný interest string (TikTok)
]

SECTOR_DISPLAY_NAMES = {
    "automotive": "Automotive", "banking": "Bankovnictví / Finance",
    "drugstore": "Drogerie / Kosmetika", "ecommerce": "E-commerce / Retail",
    "edtech": "Vzdělávání / EdTech", "energy": "Energetika",
    "fashion": "Fashion / Oblečení", "fintech": "Fintech / Platební služby",
    "food_delivery": "Food Delivery", "insurance": "Pojišťovnictví",
    "qsr": "Rychlé občerstvení (QSR)", "real_estate": "Reality / Nemovitosti",
    "sport_fitness": "Sport / Fitness", "streaming_media": "Streaming / Média",
    "telecom": "Telekomunikace", "brewing": "Pivovary",
    "betting": "Sázení / Gambling", "consulting": "Consulting",
}


# ============================================================
# MAPOVÁNÍ: silver sloupce → unified gold sloupce
# ============================================================

def _normalize_tiktok_row(row):
    """Mapuje TikTok silver řádek na unified gold sloupce."""
    # ad_type odvozený z video/image count
    video_count = int(row.get("video_count", 0) or 0)
    image_count = int(row.get("image_count", 0) or 0)
    if video_count > 0:
        ad_type = "video"
    elif image_count > 0:
        ad_type = "image"
    else:
        ad_type = ""

    # creative_url — preferuj video, pak image
    creative_url = row.get("video_url", "") or row.get("image_url", "")

    # targeting summary
    parts = []
    for key in ("target_country", "target_age", "target_gender", "target_interest"):
        val = row.get(key, "")
        if val:
            parts.append(val)

    return {
        "platform": "tiktok",
        "sector": row.get("sector", ""),
        "sector_name": row.get("sector_name", ""),
        "ad_id": row.get("ad_id", ""),
        "ad_url": row.get("ad_url", ""),  # stabilní URL na TikTok Ad Library
        "advertiser": row.get("advertiser", ""),
        "payer": row.get("paid_for_by", ""),
        "date_start": row.get("first_shown", ""),
        "date_stop": row.get("last_shown", ""),
        "reach_total": row.get("reach_total", ""),
        "reach_cz": row.get("reach_cz", ""),
        "ad_type": ad_type,
        "creative_url": creative_url,  # signed URL na video/obrázek (časově omezené)
        "targeting_summary": " | ".join(parts),
        "source": row.get("source", "") if "source" in row else "",
        "publisher_platforms": "tiktok",
    }


def _normalize_meta_row(row):
    """Mapuje Meta silver řádek na unified gold sloupce."""
    # targeting summary
    parts = []
    for key in ("target_locations", "target_ages", "target_gender"):
        val = row.get(key, "")
        if val:
            parts.append(val)

    return {
        "platform": "meta",
        "sector": row.get("sector", ""),
        "sector_name": row.get("sector_name", ""),
        "ad_id": row.get("ad_id", ""),
        "ad_url": row.get("snapshot_url", ""),  # Meta Ad Library snapshot URL (stabilní)
        "advertiser": row.get("page_name", ""),
        "payer": row.get("payer", ""),
        "date_start": row.get("delivery_start", ""),
        "date_stop": row.get("delivery_stop", ""),
        "reach_total": row.get("eu_total_reach", ""),
        "reach_cz": row.get("cz_reach") or None,
        "ad_type": "",
        "creative_url": row.get("snapshot_url", ""),  # u Mety je creative_url stejné jako ad_url
        "targeting_summary": " | ".join(parts),
        "source": row.get("source", ""),
        "publisher_platforms": row.get("platforms", ""),
    }


def _normalize_linkedin_row(row):
    """Mapuje LinkedIn silver řádek na unified gold sloupce."""
    return {
        "platform": "linkedin",
        "sector": row.get("sector", ""),
        "sector_name": row.get("sector_name", ""),
        "ad_id": row.get("ad_url", ""),
        "ad_url": row.get("ad_url", ""),  # LinkedIn Ad Library URL (stabilní)
        "advertiser": row.get("advertiser_name", ""),
        "payer": row.get("payer", ""),
        "date_start": row.get("first_impression", ""),
        "date_stop": row.get("last_impression", ""),
        "reach_total": int(row["impressions_max"]) if row.get("impressions_max") else None,
        "reach_cz": row.get("estimated_cz_impressions", ""),
        "ad_type": row.get("ad_type", ""),
        "creative_url": row.get("ad_url", ""),  # u LinkedIn je creative_url stejné jako ad_url
        "targeting_summary": row.get("target_locations", ""),
        "targeting_facets": row.get("targeting_facets", ""),  # LinkedIn facets (Job|Company|...)
        "source": row.get("source", ""),
        "publisher_platforms": "linkedin",
    }


# ============================================================
# LOAD SILVER DATA PER PLATFORM
# ============================================================

def _load_silver(container, platform, sectors):
    """Generická funkce pro načtení silver CSV ze všech sektorů platformy."""
    all_rows = []
    for sector_key, sector_config in sectors.items():
        silver_path = f"silver/{platform}/{sector_key}/current.csv"
        text = download_text(container, silver_path)
        if not text:
            logging.info(f"[GOLD] Žádný silver pro {platform}/{sector_key}")
            continue

        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)

        sector_name = sector_config.get("display_name", sector_key)
        for row in rows:
            row["sector"] = sector_key
            row["sector_name"] = sector_name

        all_rows.extend(rows)
        logging.info(f"[GOLD] {platform}/{sector_key}: {len(rows)} záznamů")

    logging.info(f"[GOLD] {platform} celkem: {len(all_rows)} záznamů")
    return all_rows


# ============================================================
# BUILD CROSS-PLATFORM GOLD
# ============================================================

def build_cross_platform_gold(container, tiktok_sectors, meta_sectors,
                               linkedin_sectors):
    """
    Načte silver data ze všech platforem, normalizuje sloupce
    a vrátí unified list pro jeden gold CSV.

    Returns:
        (unified_rows, stats) — list of dicts, summary dict
    """
    # Načti mapování názvů inzerentů
    name_map = load_advertiser_names_from_blob(container)

    unified = []
    stats = {"tiktok": 0, "meta": 0, "linkedin": 0}

    # TikTok
    tiktok_rows = _load_silver(container, "tiktok", tiktok_sectors)
    for row in tiktok_rows:
        normalized = _normalize_tiktok_row(row)
        normalized["advertiser_normalized"] = _resolve_advertiser_name(
            normalized["advertiser"], name_map
        )
        unified.append(normalized)
    stats["tiktok"] = len(tiktok_rows)

    # Meta
    meta_rows = _load_silver(container, "meta", meta_sectors)
    for row in meta_rows:
        normalized = _normalize_meta_row(row)
        normalized["advertiser_normalized"] = _resolve_advertiser_name(
            normalized["advertiser"], name_map
        )
        unified.append(normalized)
    stats["meta"] = len(meta_rows)

    # LinkedIn
    linkedin_rows = _load_silver(container, "linkedin", linkedin_sectors)
    for row in linkedin_rows:
        normalized = _normalize_linkedin_row(row)
        normalized["advertiser_normalized"] = _resolve_advertiser_name(
            normalized["advertiser"], name_map
        )
        unified.append(normalized)
    stats["linkedin"] = len(linkedin_rows)

    logging.info(
        f"[GOLD] Cross-platform: {len(unified)} reklam "
        f"(TikTok: {stats['tiktok']}, Meta: {stats['meta']}, "
        f"LinkedIn: {stats['linkedin']})"
    )
    return unified, stats


def _build_cross_platform_df(rows):
    """Sestaví unified DataFrame z raw řádků.

    Aplikuje: sector_display mapping, reach parsing, timestamp normalizace,
    výběr sloupců, string type enforcement.

    Returns:
        pd.DataFrame s CROSS_PLATFORM_COLUMNS, nebo prázdný DataFrame.
    """
    if not rows:
        return pd.DataFrame(columns=CROSS_PLATFORM_COLUMNS)

    df = pd.DataFrame(rows)

    # sector_display from mapping (fallback to sector_name → sector)
    df["sector_display"] = (
        df["sector"].map(SECTOR_DISPLAY_NAMES)
        .fillna(df["sector_name"])
        .fillna(df["sector"])
    )

    # Reach → numeric
    df["reach_total"] = df["reach_total"].apply(_parse_reach)
    df["reach_cz"] = df["reach_cz"].apply(_parse_reach)

    # Timestamps → YYYY-MM-DD
    df["date_start"] = df["date_start"].apply(_normalize_timestamp)
    df["date_stop"] = df["date_stop"].apply(_normalize_timestamp)

    # Precomputed dashboard fields (přesunuto z dashboard/app.py)
    df["pub_platform"] = df.apply(
        lambda r: _classify_pub(r["platform"], r.get("publisher_platforms", "")),
        axis=1,
    )
    df["payer_norm"] = df["payer"].apply(_norm_payer)
    _tgt = df.apply(
        lambda r: _parse_targeting(r["platform"], r.get("targeting_summary", "")),
        axis=1,
        result_type="expand",
    )
    df["tgt_narrow"] = _tgt[0].astype(bool)
    df["tgt_gender"] = _tgt[1].astype(str)
    df["tgt_interests"] = _tgt[2].astype(str)

    df = df[CROSS_PLATFORM_COLUMNS]
    # Force string columns to avoid mixed types (int/bytes/str) in Arrow
    _NUMERIC_OR_BOOL = {"reach_total", "reach_cz", "tgt_narrow"}
    for col in df.columns:
        if col in _NUMERIC_OR_BOOL:
            continue  # keep as int/bool
        if df[col].dtype == object:
            df[col] = df[col].astype(str)
    return df


def cross_platform_gold_to_parquet(rows):
    """Převede unified řádky na Parquet bytes (snappy komprese)."""
    df = _build_cross_platform_df(rows)
    if df.empty:
        return b"", df
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    return buf.getvalue(), df


# ============================================================
# META REACH DETAIL — separátní (jiná granularita)
# ============================================================

META_REACH_DETAIL_GOLD_COLUMNS = [
    "sector", "sector_name",
    "ad_id", "page_name", "country", "age_range",
    "male", "female", "unknown",
]


def load_and_merge_meta_reach_detail(container, meta_sectors):
    """
    Načte silver/meta/{sector}/reach_detail.csv ze všech Meta sektorů,
    přidá sloupce 'sector' a 'sector_name'.
    """
    all_rows = []

    for sector_key, sector_config in meta_sectors.items():
        reach_path = f"silver/meta/{sector_key}/reach_detail.csv"
        text = download_text(container, reach_path)

        if not text:
            logging.info(f"[GOLD] Žádný reach_detail pro meta/{sector_key}")
            continue

        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)

        sector_name = sector_config.get("display_name", sector_key)
        for row in rows:
            row["sector"] = sector_key
            row["sector_name"] = sector_name

        all_rows.extend(rows)
        logging.info(f"[GOLD] reach_detail meta/{sector_key}: {len(rows)} řádků")

    logging.info(f"[GOLD] Reach detail celkem: {len(all_rows)} řádků")
    return all_rows


def meta_reach_detail_gold_to_parquet(rows):
    """Převede Meta reach_detail řádky na Parquet bytes (snappy komprese)."""
    if not rows:
        return b""

    df = pd.DataFrame(rows)[META_REACH_DETAIL_GOLD_COLUMNS]
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    return buf.getvalue()


# ============================================================
# DAILY REACH DECOMPOSITION — per-day granularity
# ============================================================

DAILY_COLUMNS = [
    "date", "platform", "sector", "sector_display",
    "advertiser_normalized", "ad_id", "reach_daily",
    "publisher_platforms",
]


def cross_platform_daily_to_parquet(df, max_duration_days=365,
                                     fallback_date_stop=None):
    """Rozloží per-ad gold data na denní granularitu.

    Pro každou reklamu vygeneruje řádek pro každý den kdy běžela.
    reach_daily = reach_cz / počet_dnů (rovnoměrné rozdělení).

    Args:
        df: DataFrame z _build_cross_platform_df (numeric reach, YYYY-MM-DD dates)
        max_duration_days: Max délka reklamy v dnech (cap pro prevenci memory exploze)
        fallback_date_stop: YYYY-MM-DD pro reklamy bez date_stop (Meta ongoing)

    Returns:
        bytes — daily parquet (snappy), nebo b"" pokud žádná data
    """
    if df.empty:
        return b""

    d = df.copy()

    # Parsovat datumy
    d["_start"] = pd.to_datetime(d["date_start"], errors="coerce")
    d["_stop"] = pd.to_datetime(d["date_stop"], errors="coerce")

    # Fallback pro chybějící date_stop
    if fallback_date_stop:
        fb = pd.Timestamp(fallback_date_stop)
        d["_stop"] = d["_stop"].fillna(fb)

    # Filtrovat: potřebujeme platný start, stop a reach > 0
    d["reach_cz"] = pd.to_numeric(d["reach_cz"], errors="coerce").fillna(0)
    valid = d["_start"].notna() & d["_stop"].notna() & (d["reach_cz"] > 0)
    n_skipped = (~valid).sum()
    d = d[valid].copy()

    if d.empty:
        logging.warning("[GOLD-DAILY] Žádné reklamy s platným date_start, date_stop a reach_cz > 0")
        return b""

    # Zajistit stop >= start
    d.loc[d["_stop"] < d["_start"], "_stop"] = d.loc[d["_stop"] < d["_start"], "_start"]

    # Počet dnů (min 1)
    d["_n_days"] = (d["_stop"] - d["_start"]).dt.days + 1

    # Cap duration
    n_capped = (d["_n_days"] > max_duration_days).sum()
    if n_capped > 0:
        logging.warning(f"[GOLD-DAILY] {n_capped} reklam s duration > {max_duration_days} dnů — cap applied")
    d["_n_days"] = d["_n_days"].clip(upper=max_duration_days)

    # Reach per day
    d["reach_daily"] = (d["reach_cz"] / d["_n_days"]).round().astype(int)

    # Explode v chuncích (memory-efficient)
    n_ads = d["ad_id"].nunique()
    CHUNK = 50_000
    chunks = []
    for i in range(0, len(d), CHUNK):
        chunk = d.iloc[i:i+CHUNK]
        chunk = chunk.copy()
        chunk["_dates"] = [
            pd.date_range(s, periods=n, freq="D")
            for s, n in zip(chunk["_start"], chunk["_n_days"])
        ]
        ex = chunk.explode("_dates")
        ex["date"] = ex["_dates"].dt.strftime("%Y-%m-%d")
        chunks.append(ex[DAILY_COLUMNS])
        logging.info(f"[GOLD-DAILY] chunk {i//CHUNK+1}: {len(ex):,} řádků")

    df_daily = pd.concat(chunks, ignore_index=True)
    del chunks

    # Force string columns
    for col in df_daily.columns:
        if col == "reach_daily":
            continue
        if df_daily[col].dtype == object:
            df_daily[col] = df_daily[col].astype(str)

    logging.info(
        f"[GOLD-DAILY] {n_ads:,} reklam → {len(df_daily):,} denních řádků "
        f"(vyřazeno {n_skipped:,} bez platných dat/reach, "
        f"cap {n_capped:,} na {max_duration_days}d)"
    )

    buf = io.BytesIO()
    df_daily.to_parquet(buf, engine="pyarrow", index=False)
    return buf.getvalue()


def daily_from_parquet_file(gold_parquet_path, output_path,
                             fallback_date_stop=None):
    """Vygeneruje daily parquet z existujícího gold parquet (lokální použití).

    Args:
        gold_parquet_path: cesta ke cross_platform_gold.parquet
        output_path: cesta pro výstupní daily parquet
        fallback_date_stop: YYYY-MM-DD pro reklamy bez date_stop
    """
    df = pd.read_parquet(gold_parquet_path)
    daily_bytes = cross_platform_daily_to_parquet(df, fallback_date_stop=fallback_date_stop)
    del df
    if daily_bytes:
        with open(output_path, "wb") as f:
            f.write(daily_bytes)
        logging.info(f"[GOLD-DAILY] Uloženo: {output_path} ({len(daily_bytes) / 1024 / 1024:.1f} MB)")
    else:
        logging.warning("[GOLD-DAILY] Žádná data pro daily export")
