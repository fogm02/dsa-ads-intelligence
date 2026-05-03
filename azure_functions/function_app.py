"""
Multi-Platform Ad Intelligence Pipeline — Azure Function App

Serverless pipeline pro sběr reklamních dat z DSA repozitářů.
Medallion architektura: bronze (surová) → silver (normalizovaná) → gold (agregovaná).

TikTok (3 funkce):
- bp_discover:       Timer (2h)    — Library API → ad IDs → pending/
- bp_enrich:         Timer (2h+30) — Detail API → bronze/
- bp_transform:      Blob trigger  — bronze JSON → silver CSV

Meta (2 funkce):
- bp_meta_collect:   Timer (4h)    — Graph API → bronze/meta/
- bp_meta_transform: Blob trigger  — bronze/meta → silver/meta

LinkedIn (2 funkce):
- bp_linkedin_collect:   Timer (4h)    — Ad Library API → bronze/linkedin/
- bp_linkedin_transform: Blob trigger  — bronze/linkedin → silver/linkedin

GOLD REPORTER (1 funkce):
- bp_reporter:       Timer (24h)   — silver/* → gold/

Deployment: func azure functionapp publish <app-name>
Env vars:
  TIKTOK_CLIENT_KEY, TIKTOK_CLIENT_SECRET — TikTok Research API
  META_ACCESS_TOKEN — Meta Graph API (60-day User Access Token)
  LINKEDIN_ACCESS_TOKEN — LinkedIn Marketing API (OAuth User Token)
  BLOB_CONNECTION_STRING — Azure Blob Storage
  Optional: DISCOVERY_LOOKBACK_DAYS, META_DATE_MIN, META_DATE_MAX
"""

import azure.functions as func

# TikTok blueprints
from blueprints.bp_tiktok_discover import bp as discover_bp
from blueprints.bp_tiktok_enrich import bp as enrich_bp
from blueprints.bp_reporter import bp as reporter_bp

# Meta blueprints
from blueprints.bp_meta_collect import bp as meta_collect_bp

# LinkedIn blueprints
from blueprints.bp_linkedin_collect import bp as linkedin_collect_bp

# Unified bronze → silver dispatch (jeden EventGrid handler pro všechny platformy)
from blueprints.bp_bronze_dispatch import bp as bronze_dispatch_bp

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# TikTok
app.register_functions(discover_bp)
app.register_functions(enrich_bp)
app.register_functions(reporter_bp)

# Meta
app.register_functions(meta_collect_bp)

# LinkedIn
app.register_functions(linkedin_collect_bp)

# Bronze → Silver (unified EventGrid blob trigger)
app.register_functions(bronze_dispatch_bp)
