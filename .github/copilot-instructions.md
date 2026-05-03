# Copilot Instructions — Ad Transparency Data Pipeline

## Project Overview
Diploma thesis project: collecting ad transparency data (mandated by EU DSA regulation) from **TikTok** and **Meta** advertising archives for the Czech banking sector. The pipeline collects, deduplicates, and exports ad data for competitive intelligence analysis.

**Language:** Czech comments/UI, English code identifiers. All user-facing output (print, logs) is in Czech.

## Architecture

### Project Structure
```
diplom 3/
├── tiktok/              # TikTok collectors
├── meta/                # Meta collectors
├── linkedin/            # LinkedIn collectors
├── data/
│   ├── bronze/          # Raw API responses
│   │   ├── tiktok/
│   │   ├── meta/
│   │   └── linkedin/
│   ├── metadata/        # CSVs, stats, coverage
│   └── checkpoints/     # Resume state
├── logs/
└── .github/
```

### Collectors (Bronze Layer)
Three standalone collector scripts — no shared base class yet (see `struktura.txt` for planned refactored structure):

| Script | Platform | Auth | API Style |
|---|---|---|---|
| `tiktok/tiktok_collector.py` | TikTok Research API | OAuth2 client_credentials (auto-refresh via `TokenManager`) | POST + JSON body |
| `tiktok/tiktok_simple_collector.py` | TikTok Research API | Same OAuth2 (inline `get_token()`) | Same, simpler/flat version |
| `meta/meta_collector.py` | Meta Ad Library API (Graph API v23.0) | Long-lived User Access Token from `.env` | GET + query params |
| `linkedin/linkedin_collector.py` | LinkedIn (future) | TBD | TBD |

- `tiktok_collector.py` is the **production-grade** version with `dataclass Config`, `RateLimiter`, `CheckpointManager`, and resume support.
- `tiktok_simple_collector.py` is a **simplified/flat** variant for quick single-year runs — fewer abstractions, hardcoded credentials (⚠️ not for production).
- `tiktok_get_tokenik.py`, `meta_get_tokenik.py`, `linkedin_get_tokenik.py` are one-off utilities to manually obtain OAuth tokens.

### Data Flow
```
API → Phase 1 (Query/paginate) → deduplicate → Phase 2 (Detail enrichment) → Phase 3 (Save)
                                                                                ├── data/bronze/{tiktok,meta}/*.json  (raw)
                                                                                ├── data/metadata/*.csv               (summary)
                                                                                └── data/metadata/*.json              (stats, coverage, failed terms)
```

### Key Patterns
- **Two-phase collection (TikTok only):** Phase 1 queries ads by search term, Phase 2 fetches per-ad detail (targeting, creatives). Detail endpoint confirms CZ targeting — non-CZ ads are filtered out. Meta returns everything in one call.
- **Search term variants:** TikTok API returns 500 errors for diacritics (`Česká` → try `Ceska`, `česká`, `ceska`). See `generate_term_variants()` in `tiktok/tiktok_collector.py`.
- **Deduplication:** Ads found via multiple search terms are merged by `ad_id`. A `coverage` map tracks which terms found each ad.
- **Checkpoint/resume:** `tiktok/tiktok_collector.py` saves progress after each term to `data/checkpoints/`. Use `--resume` flag.

## Credentials & Environment
- All secrets go in `.env` (not committed): `TIKTOK_CLIENT_KEY`, `TIKTOK_CLIENT_SECRET`, `META_ACCESS_TOKEN`
- Each collector has its own `.env` loader (no shared utility yet)
- Meta token expires — must be manually renewed at `developers.facebook.com/tools/explorer/`

## Running Collectors
```bash
# TikTok (production)
python tiktok/tiktok_collector.py                    # Full run, all quarters
python tiktok/tiktok_collector.py --test             # Test: Moneta only, Q1
python tiktok/tiktok_collector.py --resume           # Resume from checkpoint
python tiktok/tiktok_collector.py --quarter Q1_2025  # Single quarter

# TikTok (simple)
python tiktok/tiktok_simple_collector.py                    # Full year, 4 terms
python tiktok/tiktok_simple_collector.py --terms Moneta ČSOB
python tiktok/tiktok_simple_collector.py --no-details       # Skip Phase 2

# Meta
python meta/meta_collector.py                      # All banks via Page ID
python meta/meta_collector.py --test               # 1 bank, 2 pages max
python meta/meta_collector.py --pages 166024303411062
python meta/meta_collector.py --terms "pojištění"  # Text search mode
```

## Output Conventions
- **File naming:** `{topic}_{period}_{YYYYMMDD_HHMMSS}.{json,csv}` — timestamp is collection time, not data period
- **Bronze JSON structure:** Always contains `metadata` dict (date range, run info, counts) + `ads` dict keyed by ad ID
- **CSV encoding:** `utf-8-sig` (BOM) for Excel compatibility with Czech characters
- **Directories are auto-created** by each collector via `os.makedirs(..., exist_ok=True)`

## API Quirks to Know
- **TikTok 500 errors** are random (~20-30% of calls), not indicative of real errors. Retry logic is essential (up to 5-10 attempts).
- **TikTok rate limits:** 60/min, 1000/hr, daily quota exists. HTTP 429 with `daily_quota` → stop, try tomorrow.
- **Meta rate limits:** ~200/hr. Error codes 4 and 17 = rate limited (wait 5 min). Code 190 = token expired.
- **Meta pagination:** Uses `paging.next` URL (all params embedded), unlike TikTok's `search_id` cursor.

## Dependencies
Only `requests`, `urllib3`, `pandas`. No async, no ORM, no web framework. Virtual env at `.venv/`.
