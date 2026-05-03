"""
Blueprint: Silver → Gold — unified cross-platform Parquet.

Timer trigger — denně v 06:00 UTC.
Generuje:
- gold/cross_platform_{date}.parquet   — všechny platformy, sjednocené sloupce
- gold/meta_reach_detail_{date}.parquet — Meta demografický breakdown (separátní granularita)

Žádná agregace — tu si dělá dashboard v Power BI / Tableau.
"""

import io as _io

import azure.functions as func
import logging
import pandas as pd
import time
from collections import Counter
from datetime import datetime

from shared.blob_helpers import (
    get_container_client,
    upload_bytes,
    load_advertisers_from_blob,
    load_meta_advertisers_from_blob,
    load_linkedin_advertisers_from_blob,
)
from shared.gold_aggregator import (
    build_cross_platform_gold,
    cross_platform_gold_to_parquet,
    cross_platform_daily_to_parquet,
    load_and_merge_meta_reach_detail,
    meta_reach_detail_gold_to_parquet,
)


bp = func.Blueprint()


# ============================================================
# TIMER TRIGGER — denně v 06:00 UTC
# ============================================================

@bp.timer_trigger(
    schedule="0 0 6 * * *",
    arg_name="timer",
    run_on_startup=False,
)
def gold_reporter(timer: func.TimerRequest) -> None:
    """
    Denní gold export — spojí silver data ze všech platforem.

    Flow:
    1. Načti silver CSV ze všech sektorů všech platforem
    2. Normalizuj sloupce do unified schématu
    3. Ulož jako Parquet (cross-platform + reach_detail separátně)
    """
    t_start = time.time()
    logging.info("=" * 60)
    logging.info("Gold Reporter — START")
    logging.info(f"Čas: {datetime.utcnow().isoformat()}")

    container = get_container_client()
    date_str = datetime.utcnow().strftime("%Y%m%d")

    # Načti konfigurace ze CSV v Blob Storage
    tiktok_sectors = load_advertisers_from_blob(container)
    meta_sectors = load_meta_advertisers_from_blob(container)
    linkedin_sectors = load_linkedin_advertisers_from_blob(container)

    # ── Cross-platform Gold (Parquet) ─────────────────────────
    unified_rows, stats = build_cross_platform_gold(
        container, tiktok_sectors, meta_sectors, linkedin_sectors
    )

    if unified_rows:
        # Log stats před uvolněním dat
        platform_counts = Counter(r.get("platform") for r in unified_rows)
        advertisers_seen = set(r.get("advertiser", "") for r in unified_rows)
        n_rows = len(unified_rows)
        n_advs = len(advertisers_seen)

        parquet_data, gold_df = cross_platform_gold_to_parquet(unified_rows)
        del unified_rows  # uvolni list of dicts (~500MB)
        del gold_df  # nepotřebujeme — daily načte z parquet_data

        gold_path = f"gold/cross_platform_{date_str}.parquet"
        upload_bytes(container, gold_path, parquet_data)

        logging.info(f"[REPORTER] Cross-platform Gold: {gold_path}")
        logging.info(f"[REPORTER] Celkem: {n_rows} reklam, {n_advs} inzerentů")
        for platform in ("tiktok", "meta", "linkedin"):
            logging.info(f"  {platform}: {platform_counts.get(platform, 0)} reklam")

        # ── Cross-platform Daily (Parquet) ───────────────────
        # Načti DataFrame z právě serializovaného parquet (šetří paměť —
        # unified_rows jsou už smazané)
        gold_df = pd.read_parquet(_io.BytesIO(parquet_data))
        del parquet_data

        daily_data = cross_platform_daily_to_parquet(
            gold_df,
            fallback_date_stop=datetime.utcnow().strftime("%Y-%m-%d"),
        )
        del gold_df

        if daily_data:
            daily_path = f"gold/cross_platform_daily_{date_str}.parquet"
            upload_bytes(container, daily_path, daily_data)
            daily_mb = len(daily_data) / 1024 / 1024
            del daily_data
            logging.info(f"[REPORTER] Daily Gold: {daily_path} ({daily_mb:.1f} MB)")
        else:
            del daily_data
            logging.info("[REPORTER] Žádná data pro daily export")
    else:
        del unified_rows
        logging.info("[REPORTER] Žádná silver data")

    saved_stats = dict(stats) if stats else {}
    del stats

    # ── Meta Reach Detail (Parquet) ───────────────────────────
    reach_detail_rows = load_and_merge_meta_reach_detail(container, meta_sectors)

    if reach_detail_rows:
        n_reach = len(reach_detail_rows)
        parquet_data = meta_reach_detail_gold_to_parquet(reach_detail_rows)
        del reach_detail_rows

        reach_gold_path = f"gold/meta_reach_detail_{date_str}.parquet"
        upload_bytes(container, reach_gold_path, parquet_data)
        del parquet_data

        logging.info(
            f"[REPORTER] Meta Reach Detail: {reach_gold_path} "
            f"({n_reach} řádků)"
        )
    else:
        logging.info("[REPORTER] Žádná Meta reach_detail data")

    # ── Summary ─────────────────────────────────────────────
    duration = time.time() - t_start
    logging.info(
        f"[REPORTER] HOTOVO ({duration:.1f}s) — "
        f"TikTok: {saved_stats.get('tiktok', 0)}, "
        f"Meta: {saved_stats.get('meta', 0)}, "
        f"LinkedIn: {saved_stats.get('linkedin', 0)}"
    )
    logging.info("=" * 60)
