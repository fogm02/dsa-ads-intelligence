"""
Konkurenční analýza na sociálních sítích — Dash dashboard (Azure deploy).
Usage:  gunicorn app:server --bind 0.0.0.0:8000 --timeout 600
Local:  python app.py → http://127.0.0.1:8050
"""

import io, os, re, unicodedata
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import pandas as pd
import numpy as np
import dash
from dash import dcc, html, dash_table, callback, Input, Output, State, ctx, ALL
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

# Workaround for Plotly 6.6.0 bug in apply_default_cascade reading
# bar.marker.pattern.shape from template — setting default explicitly
# before any px.* calls prevents ValueError in figure construction.
pio.templates.default = "plotly_white"

# ── Data source: Azure Blob Storage nebo lokální soubory ─────
_CONN_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING") or os.environ.get("BLOB_CONNECTION_STRING")
_IS_AZURE = bool(_CONN_STR)
_ENABLE_SNAPSHOT = os.environ.get("ENABLE_SNAPSHOT", "").lower() in ("1", "true", "yes")

if _IS_AZURE:
    from azure.storage.blob import BlobServiceClient
    _CONTAINER = "diploma"

    def _blob_client():
        return BlobServiceClient.from_connection_string(_CONN_STR).get_container_client(_CONTAINER)

    def _find_latest_blob(prefix):
        cc = _blob_client()
        blobs = [b for b in cc.list_blobs(name_starts_with=prefix) if b.name.endswith(".parquet")]
        blobs.sort(key=lambda b: b.name, reverse=True)
        return blobs[0].name if blobs else None

    def _read_blob_parquet(blob_path):
        cc = _blob_client()
        data = cc.download_blob(blob_path).readall()
        return pd.read_parquet(io.BytesIO(data))
else:
    _HERE = os.path.dirname(os.path.abspath(__file__))
    _DATA_DIR = os.path.join(_HERE, "data")
    if not os.path.exists(os.path.join(_DATA_DIR, "cross_platform_gold.parquet")):
        # Try azure_functions/test_output (legacy) or local_data/test_output (current)
        for candidate in [
            os.path.join(_HERE, "..", "test_output"),
            os.path.join(_HERE, "..", "..", "local_data", "test_output"),
        ]:
            if os.path.exists(os.path.join(candidate, "cross_platform_gold.parquet")):
                _DATA_DIR = candidate
                break

# Platform brand palette — official brand colors
PLAT_COLOR = {"meta": "#1877F2", "tiktok": "#010101", "linkedin": "#38bdf8",
              "Facebook": "#1877F2", "Instagram": "#E4405F",
              "Facebook + Instagram": "#833AB4", "TikTok": "#010101",
              "LinkedIn": "#38bdf8", "Meta (jiné)": "#94a3b8"}
PLAT_BAR = {"Meta": "#1877F2", "TikTok": "#010101", "LinkedIn": "#38bdf8"}
PUB_DISPLAY = {"facebook": "Facebook", "instagram": "Instagram",
               "facebook+instagram": "Facebook + Instagram",
               "tiktok": "TikTok", "linkedin": "LinkedIn", "meta_other": "Meta (jiné)"}

SECTOR_NAMES = {
    "automotive": "Automotive", "banking": "Bankovnictví / Finance",
    "drugstore": "Drogerie / Kosmetika", "ecommerce": "E-commerce / Retail",
    "edtech": "Vzdělávání / EdTech", "energy": "Energetika",
    "fashion": "Fashion / Oblečení", "fintech": "Fintech / Platební služby",
    "food_delivery": "Food Delivery", "insurance": "Pojišťovnictví",
    "qsr": "Rychlé občerstvení (QSR)", "real_estate": "Reality / Nemovitosti",
    "sport_fitness": "Sport / Fitness", "streaming_media": "Streaming / Média",
    "telecom": "Telekomunikace",
    "brewing": "Pivovary",
}
AGE_BUCKETS = ["13-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
AGE_BOUNDS = [(13,17),(18,24),(25,34),(35,44),(45,54),(55,64),(65,99)]

LEGAL_SUFFIXES = [
    ", s.r.o.", ", s.r.o", ", a.s.", ", a. s.", ", a.s", ", a. s",
    " s.r.o.", " s.r.o", " a.s.", " a. s.", " a.s", " a. s",
    " spol. s r.o.", " spol. s r.o",
    " gmbh", " ltd.", " ltd", " inc.", " inc", " llc", " ag",
    " czech republic", " czechia", " pte.", " pte", ".cz", " cz", " ce",
]

GRAPH_CFG = {"displayModeBar": False, "responsive": True}
FIG_LAYOUT = dict(
    template="plotly_white",
    font=dict(family="'Inter',-apple-system,BlinkMacSystemFont,sans-serif",
              size=12, color="#334155"),
    margin=dict(l=10, r=20, t=30, b=40),
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    colorway=["#6366f1", "#f43f5e", "#06b6d4", "#a855f7", "#10b981",
              "#f59e0b", "#ec4899", "#14b8a6", "#64748b"],
    xaxis=dict(gridcolor="rgba(148,163,184,.1)", zerolinecolor="rgba(148,163,184,.15)",
               tickfont=dict(color="#94a3b8", size=11)),
    yaxis=dict(gridcolor="rgba(148,163,184,.1)", zerolinecolor="rgba(148,163,184,.15)",
               tickfont=dict(color="#475569", size=11)),
    hoverlabel=dict(bgcolor="rgba(15,23,42,.92)", bordercolor="rgba(15,23,42,0)",
                    font=dict(color="#f1f5f9", size=12,
                              family="'Inter',-apple-system,sans-serif")),
    legend=dict(font=dict(color="#475569", size=11),
                bgcolor="rgba(0,0,0,0)"),
    bargap=0.22,
)


# ── Helpers ───────────────────────────────────────────────────


def _strip_diacritics(s):
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def norm_payer(s):
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

PAYER_ALIASES = {
    "dentsu": "dentsu media services",
    "dentsu media services": "dentsu media services",
    # Self-payer aliasy — full legal/parent jména mapovaná na brand canonical
    "ceskoslovenska obchodni banka": "csob",
    "csob pojistovna": "csob",
    "csps": "ceska sporitelna",
    "ceska sporitelna kariera": "ceska sporitelna",
    "ceska sporitelna penzijni spolecnost": "ceska sporitelna",
    "erste group bank": "ceska sporitelna",
    "raiffeisen stavebni sporitelna": "raiffeisenbank",
}

# Stop words for third-party detection: legal forms + geo markers that dominate
# self-payer names (e.g., "Partners Bank" vs "Partners Banka" must match on "partners")
_TP_STOP = {"sro", "ltd", "pte", "inc", "llc", "gmbh", "czech", "republic",
            "czechia", "spol", "group", "holding", "cz", "eu"}


def _name_words(s):
    """Break a company name into significant tokens (≥3 chars, no legal/geo stop words)."""
    if not s or pd.isna(s):
        return set()
    n = _strip_diacritics(str(s).lower())
    n = re.sub(r"[^a-z0-9\s]", " ", n)
    return {w for w in n.split() if len(w) >= 3 and w not in _TP_STOP}


def is_third_party(payer, advertisers):
    """
    Heuristic: payer is a genuine third party if
    - it pays for ≥2 distinct advertisers, OR
    - po normalizaci přes alias mapping není shodný se značkou, AND
    - none of its tokens overlap with the (only) advertiser's tokens.
    If either side has no significant tokens, we conservatively assume self-payer.
    """
    if len(advertisers) >= 2:
        return True
    # Self-payer detekce přes PAYER_ALIASES (zachytí parent companies & full legal names)
    payer_norm = norm_payer(payer)
    adv_norms = {norm_payer(a) for a in advertisers}
    if payer_norm in adv_norms:
        return False
    pw = _name_words(payer)
    aw = _name_words(advertisers[0] if advertisers else "")
    if not pw or not aw:
        return False
    for p in pw:
        for a in aw:
            if p in a or a in p:
                return False
    return True


def fmt(n):
    if n >= 1e9: return f"{n/1e9:.1f} mld.".replace(".", ",", 1)
    if n >= 1e6: return f"{n/1e6:.1f} mil.".replace(".", ",", 1)
    if n >= 1e3: return f"{n/1e3:.0f} tis."
    return f"{int(n):,}".replace(",", " ")


from datetime import datetime as _dt
_CURRENT_MONTH = _dt.now().strftime("%Y-%m")


def empty_fig(msg="Žádná data"):
    fig = go.Figure()
    fig.add_annotation(text=msg, xref="paper", yref="paper", x=0.5, y=0.5,
                       showarrow=False, font=dict(size=14, color="#94a3b8"))
    fig.update_layout(**FIG_LAYOUT, height=300)
    return fig


def styled(fig, h=350):
    fig.update_layout(**FIG_LAYOUT, height=h)
    for trace in fig.data:
        if hasattr(trace, "marker") and trace.type == "bar":
            trace.marker.cornerradius = 4
    return fig


# ── Data Loading ──────────────────────────────────────────────
def _classify_pub(row):
    if row["platform"] != "meta":
        return row["platform"]
    pubs = set(p.strip().lower() for p in str(row.get("publisher_platforms", "")).split("|") if p.strip())
    has_fb, has_ig = "facebook" in pubs, "instagram" in pubs
    if has_fb and has_ig: return "facebook+instagram"
    if has_fb: return "facebook"
    if has_ig: return "instagram"
    return "meta_other"


def _parse_tgt(row):
    ts, plat = str(row.get("targeting_summary", "")), str(row.get("platform", ""))
    narrow, gender, interests = False, "all", ""
    if not ts or ts == "nan":
        return pd.Series([narrow, gender, interests])
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
            if g in ("women", "ženy"): gender = "female"
            elif g in ("men", "muži"): gender = "male"
    elif plat == "tiktok":
        if len(parts) >= 2:
            n_buckets = len([a.strip() for a in parts[1].split(",") if a.strip()])
        if len(parts) >= 3:
            g = parts[2].strip().lower()
            if g == "female": gender = "female"
            elif g == "male": gender = "male"
        if len(parts) >= 4 and parts[3].strip():
            interests = parts[3].strip()
    narrow = n_buckets <= 3 or gender != "all"
    return pd.Series([narrow, gender, interests])


def _load_parquet(name, prefix=None):
    """Load parquet — from Azure Blob or local file."""
    if _IS_AZURE:
        blob = _find_latest_blob(prefix)
        if not blob:
            return None
        print(f"  Blob: {blob}")
        return _read_blob_parquet(blob)
    else:
        path = os.path.join(_DATA_DIR, name)
        if not os.path.exists(path):
            return None
        print(f"  File: {path}")
        return pd.read_parquet(path)


def load_data():
    src = "Azure Blob Storage" if _IS_AZURE else "lokální soubory"
    print(f"Loading data ({src})...")

    # Gold — per-ad
    if _IS_AZURE:
        cc = _blob_client()
        blobs = [b for b in cc.list_blobs(name_starts_with="gold/cross_platform_")
                 if b.name.endswith(".parquet") and "daily" not in b.name]
        blobs.sort(key=lambda b: b.name, reverse=True)
        if not blobs:
            raise RuntimeError("No cross_platform gold parquet found")
        print(f"  Blob: {blobs[0].name}")
        df = _read_blob_parquet(blobs[0].name)
    else:
        path = os.path.join(_DATA_DIR, "cross_platform_gold.parquet")
        if not os.path.exists(path):
            raise RuntimeError(f"Gold parquet not found: {path}")
        print(f"  File: {path}")
        df = pd.read_parquet(path)

    # Default: full dataset (Azure deploy + běžné lokální spuštění).
    # Pro thesis analýzu (kap. 7) je k dispozici snapshot cutoff na 20.3.2026
    # (pre-bug, plně konzistentní napříč Meta/TikTok/LinkedIn — viz kap. 6.8 a 7),
    # který se aktivuje přes env var ENABLE_SNAPSHOT=1.
    SNAPSHOT_DATE = "2026-03-20"
    if _ENABLE_SNAPSHOT:
        n_before = len(df)
        df = df[df["date_start"].astype(str) <= SNAPSHOT_DATE].copy()
        print(f"  Snapshot cutoff at {SNAPSHOT_DATE}: {n_before} → {len(df)} rows ({n_before-len(df)} excluded)")
    else:
        print(f"  Full dataset (no snapshot cutoff)")

    df["reach_num"] = pd.to_numeric(df["reach_cz"], errors="coerce").fillna(0).astype(int)
    df["month"] = df["date_start"].str[:7]
    df["adv"] = df["advertiser_normalized"].fillna(df["advertiser"])
    if "sector_display" not in df.columns:
        df["sector_display"] = df["sector"].map(SECTOR_NAMES).fillna(df["sector_name"])
    # Sloupce pub_platform, payer_norm, tgt_narrow, tgt_gender, tgt_interests
    # jsou předpočítané už v gold layeru (shared/gold_aggregator.py).
    # Fallback pro starší parquety bez těchto sloupců:
    if "pub_platform" not in df.columns:
        df["pub_platform"] = df.apply(_classify_pub, axis=1)
    if "payer_norm" not in df.columns:
        df["payer_norm"] = df["payer"].apply(norm_payer)
    if "tgt_narrow" not in df.columns:
        tgt = df.apply(_parse_tgt, axis=1)
        tgt.columns = ["tgt_narrow", "tgt_gender", "tgt_interests"]
        df = pd.concat([df, tgt], axis=1)

    # Demographics
    df_demo = _load_parquet("meta_reach_detail_gold.parquet", "gold/meta_reach_detail_")
    if df_demo is not None:
        if "country" in df_demo.columns:
            df_demo = df_demo[df_demo["country"] == "CZ"].copy()
        meta_map = df.dropna(subset=["advertiser"]).drop_duplicates("advertiser").set_index("advertiser")["adv"].to_dict()
        df_demo["adv"] = df_demo["page_name"].map(meta_map).fillna(df_demo["page_name"])
        df_demo["sector_display"] = df_demo["sector"].map(SECTOR_NAMES).fillna(df_demo["sector_name"])
        for c in ("male", "female", "unknown"):
            df_demo[c] = pd.to_numeric(df_demo[c], errors="coerce").fillna(0).astype(int)
        print(f"  Demographics: {len(df_demo):,} rows")

    # Daily reach decomposition
    df_daily = _load_parquet("cross_platform_daily.parquet", "gold/cross_platform_daily_")
    if df_daily is not None:
        df_daily["reach_daily"] = pd.to_numeric(df_daily["reach_daily"], errors="coerce").fillna(0)
        df_daily["date"] = pd.to_datetime(df_daily["date"])
        df_daily["month"] = df_daily["date"].dt.strftime("%Y-%m")
        # Aplikuj snapshot cutoff i na daily (konzistence s df)
        if _ENABLE_SNAPSHOT:
            n_before = len(df_daily)
            df_daily = df_daily[df_daily["date"] <= pd.Timestamp(SNAPSHOT_DATE)].copy()
            print(f"  Daily snapshot cutoff at {SNAPSHOT_DATE}: {n_before} → {len(df_daily)} rows")
        df_daily["week"] = df_daily["date"].dt.to_period("W").dt.start_time.dt.strftime("%Y-%m-%d")
        df_daily["quarter"] = df_daily["date"].dt.to_period("Q").dt.start_time.dt.strftime("%Y-%m-%d")
        print(f"  Daily: {len(df_daily):,} rows")

    print(f"  Loaded {len(df):,} ads, {df['adv'].nunique()} advertisers, {df['sector'].nunique()} sectors")
    return df, df_demo, df_daily


DF = DF_DEMO = DF_DAILY = None
ALL_SECTORS = ALL_PLATFORMS = ALL_ADVERTISERS = []
ADVERTISERS_BY_SECTOR = {}
DATE_MIN = DATE_MAX = DATE_DEFAULT_START = None
_DATA_LOADED = False

def _ensure_data():
    global DF, DF_DEMO, DF_DAILY, ALL_SECTORS, ALL_PLATFORMS, ALL_ADVERTISERS, ADVERTISERS_BY_SECTOR
    global DATE_MIN, DATE_MAX, DATE_DEFAULT_START, _DATA_LOADED
    if _DATA_LOADED:
        return
    DF, DF_DEMO, DF_DAILY = load_data()
    ALL_SECTORS = sorted(DF["sector"].unique())
    ALL_PLATFORMS = sorted(DF["platform"].unique())
    ALL_ADVERTISERS = sorted(DF["adv"].dropna().unique())
    ADVERTISERS_BY_SECTOR = {
        s: sorted(DF[DF["sector"] == s]["adv"].dropna().unique())
        for s in ALL_SECTORS
    }
    _dates_clean = DF["date_start"].dropna().astype(str).str[:10]
    _dates_clean = _dates_clean[_dates_clean.str.match(r"^\d{4}-\d{2}-\d{2}$")]
    DATE_MIN = _dates_clean.min() if not _dates_clean.empty else None
    DATE_MAX = _dates_clean.max() if not _dates_clean.empty else None
    # Default start = leden 2025 (drtivá většina dat); historie dostupná přes kalendář
    DATE_DEFAULT_START = "2025-01-01" if DATE_MIN and DATE_MIN <= "2025-01-01" else DATE_MIN
    _DATA_LOADED = True


# ── Filtering ─────────────────────────────────────────────────
# `adv` comes from clickData (Store) — cross-filter from a chart.
# `search` comes from the dropdown — user's explicit selection.
# Both are exact advertiser matches; they are combined with AND.
#
# Datum filter používá OVERLAP (= reklama byla AKTIVNÍ v daném období),
# stejně jako Meta Ad Library / TikTok Ad Library UI:
#   - reklama spuštěná před date_end (date_start <= date_end)
#   - reklama stále aktivní v date_start (date_stop >= date_start nebo date_stop chybí)
def filt(df, sector, platforms, adv, search, date_start=None, date_end=None):
    m = pd.Series(True, index=df.index)
    if sector: m &= df["sector"] == sector
    if platforms: m &= df["platform"].isin(platforms)
    if adv: m &= df["adv"] == adv
    if search: m &= df["adv"] == search
    # Overlap-based filter (matches Ad Library UI behavior)
    if date_end:
        m &= df["date_start"].astype(str) <= str(date_end)
    if date_start:
        # ad ended after range_start, OR ad has no end (still active)
        ds_str = df["date_stop"].astype(str)
        m &= (ds_str >= str(date_start)) | (ds_str == "") | (ds_str == "nan") | (ds_str == "NaT")
    return df[m]


def filt_daily(df_daily, sector, platforms, adv, search, date_start=None, date_end=None):
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()
    m = pd.Series(True, index=df_daily.index)
    if sector: m &= df_daily["sector"] == sector
    if platforms: m &= df_daily["platform"].isin(platforms)
    if adv: m &= df_daily["advertiser_normalized"] == adv
    if search: m &= df_daily["advertiser_normalized"] == search
    # df_daily["date"] může být Timestamp (Azure load_data) nebo string;
    # ořez prefixu na YYYY-MM-DD zajišťuje korektní lexikografické srovnání s filterem.
    if date_start or date_end:
        date_str = df_daily["date"].astype(str).str[:10]
        if date_start: m &= date_str >= str(date_start)
        if date_end: m &= date_str <= str(date_end)
    return df_daily[m]


GRAN_COL = {"day": "date", "week": "week", "month": "month", "quarter": "quarter"}
GRAN_FMT = {"day": "%d.%m", "week": "%d.%m", "month": "%m/%Y", "quarter": "Q%q %Y"}


def agg_by_granularity(dd, gran, group_col):
    """Agreguje daily data dle granularity a group_col."""
    time_col = GRAN_COL.get(gran, "week")
    agg = dd.groupby([time_col, group_col])["reach_daily"].sum().reset_index(name="reach")
    agg = agg.rename(columns={time_col: "period"})
    return agg.sort_values("period")


def _cutoff_end_for_gran(date_max, gran):
    """
    Vrátí nejzazší datum, do kterého se má timeline kreslit (cap na poslední ÚPLNÝ bucket).

    - day:     DATE_MAX (denní data jsou vždy úplná)
    - week:    poslední neděle před start tohoto týdne (tj. konec posledního úplného Po-Ne týdne)
    - month:   poslední den předchozího měsíce
    - quarter: poslední den předchozího kvartálu
    """
    dt = pd.Timestamp(date_max)
    if gran == "week":
        # week_start (Po) tohoto týdne; cap = den před = poslední neděle minulého týdne
        week_start = dt - pd.Timedelta(days=dt.dayofweek)
        return (week_start - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    if gran == "month":
        # první den tohoto měsíce; cap = den před = poslední den minulého měsíce
        return (dt.replace(day=1) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    if gran == "quarter":
        # první den tohoto kvartálu; cap = den před = poslední den minulého kvartálu
        q_start_month = ((dt.month - 1) // 3) * 3 + 1
        q_start = dt.replace(month=q_start_month, day=1)
        return (q_start - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d")  # day → bez cap


def filt_demo(df_demo, sector, adv, search, date_start=None, date_end=None):
    if df_demo is None:
        return pd.DataFrame()
    m = pd.Series(True, index=df_demo.index)
    if sector: m &= df_demo["sector"] == sector
    if adv: m &= df_demo["adv"] == adv
    if search: m &= df_demo["adv"] == search
    # demo nemá date_start na úrovni řádku (je to demografický rozklad), date filter neaplikujeme
    return df_demo[m]


# ── Layout ────────────────────────────────────────────────────
KPI_ACCENTS = {
    "kpi-ads": "#6366f1",
    "kpi-reach": "#a78bfa",
    "kpi-adv": "#06b6d4",
    "kpi-sec": "#10b981",
    "kpi-plat": "#f43f5e",
    "kpi-period": "#94a3b8",
}


def kpi_card(id_, label, small=False):
    accent = KPI_ACCENTS.get(id_, "#4f46e5")
    val_cls = "kpi-value kpi-value--sm" if small else "kpi-value"
    return html.Div([
        html.Div(label, className="kpi-label"),
        html.Div(id=id_, className=val_cls),
    ], className="kpi-card", style={"--kpi-accent": accent})


# Nav tabs definition — sidebar navigation synced to dbc.Tabs.active_tab
NAV_ITEMS = [
    ("tab-overview", "Přehled"),
    ("tab-sov", "Share of Voice"),
    ("tab-demo", "Demografie"),
    ("tab-strat", "Strategie"),
    ("tab-targ", "Cílení"),
    ("tab-creative", "Kreativa"),
]


def nav_link(tab_id, label, active=False):
    icon_key = tab_id.replace("tab-", "")
    return dbc.NavLink(
        [html.Span(className=f"nav-icon nav-icon--{icon_key}"), label],
        id={"type": "nav-link", "index": tab_id},
        active=active, href="#", n_clicks=0,
    )


def chart_card(title, graph_id, subtitle=None, h=None, fill=False):
    header_children = [html.Div(title, className="chart-title")]
    if subtitle:
        header_children.append(html.Div(subtitle, className="chart-subtitle"))
    graph_style = {"height": h} if h else {"height": "420px"}
    cls = "chart-card chart-card--fill" if fill else "chart-card"
    if fill:
        graph_style = {"height": "100%", "flex": "1", "minHeight": "0"}
    return html.Div([
        html.Div(header_children, className="chart-header"),
        dcc.Graph(id=graph_id, config=GRAPH_CFG, style=graph_style),
    ], className=cls)


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY],
                title="Konkurenční analýza na sociálních sítích",
                suppress_callback_exceptions=True)
server = app.server  # WSGI entrypoint pro gunicorn

# EAGER LOAD při importu modulu — kombinováno s gunicorn --preload to znamená,
# že data se načtou jednou v master procesu a všechny workery je sdílí přes
# copy-on-write fork (úspora RAM místo 4× 600 MB = 600 MB shared).
# Pro lokální vývoj bez --preload to znamená, že první spuštění je trochu pomalejší,
# ale následné requesty už jsou rychlé.
_ensure_data()

app.index_string = """<!DOCTYPE html>
<html lang="cs">
<head>
    {%metas%}
    <title>{%title%}</title>
    {%favicon%}
    {%css%}
</head>
<body>
    {%app_entry%}
    <footer>
        {%config%}
        {%scripts%}
        {%renderer%}
    </footer>
    <script>
    // Force Plotly resize after layout settles
    document.addEventListener('DOMContentLoaded', function() {
        var tries = [200, 600, 1200];
        tries.forEach(function(ms) {
            setTimeout(function() {
                window.dispatchEvent(new Event('resize'));
            }, ms);
        });
    });
    // Also resize on tab switch
    new MutationObserver(function() {
        setTimeout(function() { window.dispatchEvent(new Event('resize')); }, 100);
    }).observe(document.body, {childList: true, subtree: true});
    </script>
</body>
</html>"""

app.layout = html.Div([
    dcc.Store(id="sel-adv", data=None),

    html.Div([
        # ═══ SIDEBAR ═══
        html.Div([
            # Brand
            html.Div([
                html.Div("SV", className="brand-logo"),
                html.Div("Social SoV", className="brand-name"),
            ], className="sidebar-brand"),

            # Navigation
            html.Div([
                html.Div("Navigace", className="sidebar-section-label"),
                dbc.Nav(
                    [nav_link(tid, lbl, active=(tid == "tab-overview"))
                     for tid, lbl in NAV_ITEMS],
                    vertical=True, className="sidebar-nav",
                ),
            ], className="sidebar-section"),

            # Filters
            html.Div([
                html.Div([
                    dcc.Dropdown(
                        id="f-sector",
                        options=[{"label": SECTOR_NAMES.get(s, s), "value": s} for s in ALL_SECTORS],
                        placeholder="Sektory", clearable=True,
                    ),
                    dcc.Dropdown(
                        id="f-search",
                        options=[{"label": a, "value": a} for a in ALL_ADVERTISERS],
                        placeholder="Inzerenti",
                        searchable=True, clearable=True,
                    ),
                    dcc.Checklist(
                        id="f-plat", value=ALL_PLATFORMS,
                        options=[{
                            "label": {"linkedin": "LinkedIn", "meta": "Meta",
                                      "tiktok": "TikTok"}.get(p, p.title()),
                            "value": p
                        } for p in ALL_PLATFORMS],
                        className="switch-list",
                    ),
                    dbc.Button("✕  Zrušit výběr inzerenta", id="btn-clear",
                               className="btn-clear",
                               style={"display": "none"}),
                ], className="filter-group"),
            ], className="sidebar-section"),
        ], className="sidebar"),

        # ═══ MAIN ═══
        html.Div([
            # Banner (when advertiser selected)
            dbc.Alert(id="adv-banner", is_open=False, className="adv-banner"),

            # KPI row + Date filter
            html.Div([
                kpi_card("kpi-ads", "Reklamy"),
                kpi_card("kpi-reach", "Celkový reach"),
                kpi_card("kpi-adv", "Inzerenti"),
                kpi_card("kpi-sec", "Sektory"),
                kpi_card("kpi-plat", "Platformy"),
                # Date range filter — design-konzistentní s KPI kartami
                html.Div([
                    html.Div("Filtr období", className="kpi-label"),
                    dcc.DatePickerRange(
                        id="f-daterange",
                        display_format="DD. MM. YYYY",
                        start_date_placeholder_text="Od",
                        end_date_placeholder_text="Do",
                        clearable=True,
                        first_day_of_week=1,  # Monday
                        show_outside_days=False,
                        minimum_nights=0,
                        with_portal=True,  # render calendar in centered modal portal — fixes clipping
                        className="date-range-picker",
                    ),
                ], className="kpi-card filter-card"),
            ], className="kpi-grid"),

            # Tabs — default header hidden (controlled via sidebar)
            dbc.Tabs(id="tabs", active_tab="tab-overview", className="tabs-hidden", children=[
                dbc.Tab(label="Přehled", tab_id="tab-overview", children=[
                    html.Div([
                        html.Div([
                            html.Div([
                                html.Div([
                                    html.Div("Rozložení platforem", className="chart-title"),
                                    dbc.RadioItems(
                                        id="plat-metric",
                                        options=[
                                            {"label": "Reach", "value": "reach"},
                                            {"label": "Počet", "value": "count"},
                                        ],
                                        value="reach",
                                        inline=True,
                                        className="chart-toggle",
                                    ),
                                ], className="chart-header", style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
                                dcc.Graph(id="ch-plat-dist", config=GRAPH_CFG,
                                          style={"height": "100%", "flex": "1", "minHeight": "0"}),
                            ], className="chart-card chart-card--fill"),
                            chart_card("Sektory dle reach", "ch-sectors", fill=True),
                        ], className="grid-row grid-57"),
                        html.Div([
                            html.Div([
                                html.Div([
                                    html.Div("Vývoj dosahu v čase", className="chart-title"),
                                    dbc.RadioItems(
                                        id="timeline-gran",
                                        options=[
                                            {"label": "Den", "value": "day"},
                                            {"label": "Týden", "value": "week"},
                                            {"label": "Měsíc", "value": "month"},
                                        ],
                                        value="week",
                                        inline=True,
                                        className="chart-toggle",
                                    ),
                                ], className="chart-header", style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
                                dcc.Graph(id="ch-timeline", config=GRAPH_CFG,
                                          style={"height": "100%", "flex": "1", "minHeight": "0"}),
                            ], className="chart-card chart-card--fill"),
                        ], className="grid-row grid-12"),
                    ], className="tab-fill"),
                ]),
                dbc.Tab(label="Share of Voice", tab_id="tab-sov", children=[
                    html.Div([
                        html.Div([
                            chart_card("Share of Voice — reach vs. počet reklam", "ch-sov", fill=True),
                        ], className="grid-row grid-12"),
                        html.Div([
                            html.Div([
                                html.Div([
                                    html.Div("Vývoj dle inzerentů", className="chart-title"),
                                    html.Div([
                                        dbc.RadioItems(
                                            id="season-gran",
                                            options=[
                                                {"label": "Den", "value": "day"},
                                                {"label": "Týden", "value": "week"},
                                                {"label": "Měsíc", "value": "month"},
                                            ],
                                            value="week",
                                            inline=True,
                                            className="chart-toggle",
                                        ),
                                        dbc.RadioItems(
                                            id="season-metric",
                                            options=[
                                                {"label": "Reach", "value": "reach"},
                                                {"label": "Počet", "value": "count"},
                                            ],
                                            value="reach",
                                            inline=True,
                                            className="chart-toggle",
                                            style={"marginLeft": "8px"},
                                        ),
                                    ], style={"display": "flex", "gap": "8px"}),
                                ], className="chart-header", style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
                                dcc.Graph(id="ch-season-line", config=GRAPH_CFG,
                                          style={"height": "100%", "flex": "1", "minHeight": "0"}),
                            ], className="chart-card chart-card--fill"),
                        ], className="grid-row grid-12"),
                    ], className="tab-fill"),
                ]),
                dbc.Tab(label="Demografie", tab_id="tab-demo", children=[
                    html.Div([
                        html.Div([
                            chart_card("Dosah dle věkových skupin (Meta, CZ)", "ch-demo-age", fill=True),
                            chart_card("Dosah dle pohlaví (Meta, CZ)", "ch-demo-gender", fill=True),
                        ], className="grid-row grid-66"),
                        html.Div([
                            chart_card("Top inzerentů — rozložení dle pohlaví", "ch-demo-adv", fill=True),
                        ], className="grid-row grid-12"),
                    ], className="tab-fill"),
                ]),
                dbc.Tab(label="Strategie", tab_id="tab-strat", children=[
                    html.Div([
                        html.Div([
                            chart_card("Pokrytí platforem", "ch-coverage", fill=True),
                            chart_card("Inzerenti na více platformách", "ch-cross-plat", fill=True),
                        ], className="grid-row grid-57"),
                        html.Div([
                            chart_card("Plátci třetích stran dle reach", "ch-payers", fill=True),
                        ], className="grid-row grid-12"),
                        html.Div([
                            chart_card("Trvání reklam — top 15 inzerentů (dny)", "ch-duration", fill=True),
                        ], className="grid-row grid-12"),
                    ], className="tab-fill"),
                ]),
                dbc.Tab(label="Cílení", tab_id="tab-targ", children=[
                    html.Div([
                        html.Div([
                            chart_card("Cílení dle věku", "ch-tgt-age-d", fill=True),
                            chart_card("Cílení dle pohlaví", "ch-tgt-gen-d", fill=True),
                        ], className="grid-row grid-66"),
                        html.Div([
                            chart_card("Kdo cílí na konkrétní pohlaví", "ch-tgt-gen-bar", fill=True),
                            chart_card("Zájmové kategorie — TikTok", "ch-tgt-int", fill=True),
                        ], className="grid-row grid-66"),
                    ], className="tab-fill"),
                ]),
                dbc.Tab(label="Kreativa", tab_id="tab-creative", children=[
                    html.Div(id="creative-stats", className="section-note"),
                    dash_table.DataTable(
                        id="tbl-creative",
                        columns=[
                            {"name": "#", "id": "rank", "type": "numeric"},
                            {"name": "Inzerent", "id": "adv"},
                            {"name": "Platforma", "id": "pub_platform"},
                            {"name": "Reach CZ", "id": "reach_fmt"},
                            {"name": "Datum", "id": "date_start"},
                            {"name": "Sektor", "id": "sector_display"},
                            {"name": "Kreativa", "id": "link", "presentation": "markdown"},
                        ],
                        page_size=50,
                        sort_action="native",
                        style_table={"overflowX": "auto"},
                        style_cell={"textAlign": "left", "padding": "6px 12px",
                                    "fontSize": "13px", "fontFamily": "Inter, sans-serif"},
                        style_header={"fontWeight": "bold", "backgroundColor": "#f8fafc",
                                      "borderBottom": "1px solid #cbd5e1"},
                        style_data_conditional=[
                            {"if": {"column_id": "rank"}, "width": "50px", "textAlign": "center"},
                    {"if": {"column_id": "reach_fmt"}, "textAlign": "right", "fontVariantNumeric": "tabular-nums"},
                            {"if": {"column_id": "link"}, "width": "90px", "textAlign": "center"},
                        ],
                        style_cell_conditional=[
                            {"if": {"column_id": "adv"}, "maxWidth": "200px", "overflow": "hidden",
                             "textOverflow": "ellipsis"},
                        ],
                    ),
                ]),
            ]),

        ], className="main"),
    ], className="app-shell"),
])


# ── Common filter inputs ──────────────────────────────────────
FILTER_INPUTS = [
    Input("f-sector", "value"),
    Input("f-plat", "value"),
    Input("sel-adv", "data"),
    Input("f-search", "value"),
    Input("f-daterange", "start_date"),
    Input("f-daterange", "end_date"),
]


# ── Callback: sidebar nav → tabs.active_tab ───────────────────
@callback(
    Output("tabs", "active_tab"),
    Input({"type": "nav-link", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def sidebar_nav_click(clicks):
    tid = ctx.triggered_id
    if isinstance(tid, dict) and "index" in tid:
        return tid["index"]
    return dash.no_update


# ── Callback: highlight active nav link ───────────────────────
@callback(
    Output({"type": "nav-link", "index": ALL}, "active"),
    Input("tabs", "active_tab"),
)
def highlight_active_nav(active_tab):
    return [tid == active_tab for tid, _ in NAV_ITEMS]


# ── Callback: advertiser dropdown scoped by sector ────────────
@callback(
    Output("f-search", "options"),
    Output("f-search", "value"),
    Input("f-sector", "value"),
    State("f-search", "value"),
)
def update_search_options(sector, current):
    advs = ADVERTISERS_BY_SECTOR.get(sector, ALL_ADVERTISERS) if sector else ALL_ADVERTISERS
    options = [{"label": a, "value": a} for a in advs]
    # Reset selection if current advertiser is not in the new sector
    new_value = current if current in advs else None
    return options, new_value


# ── Callback: advertiser selection ────────────────────────────
@callback(
    Output("sel-adv", "data"),
    Input("ch-sov", "clickData"),
    Input("ch-tgt-gen-bar", "clickData"),
    Input("ch-demo-adv", "clickData"),
    Input("ch-payers", "clickData"),
    Input("btn-clear", "n_clicks"),
    State("sel-adv", "data"),
    prevent_initial_call=True,
)
def select_adv(sov_click, tgt_g, demo, pay, clear_n, current):
    tid = ctx.triggered_id
    if tid == "btn-clear":
        return None
    for click in [sov_click, tgt_g, demo, pay]:
        if click and tid != "btn-clear":
            pts = click.get("points", [{}])
            cd = pts[0].get("customdata")
            if cd:
                name = cd[0] if isinstance(cd, list) else cd
                if name and isinstance(name, str):
                    return name
    return current


# ── Callback: sector selection from chart click ─────────────
@callback(
    Output("f-sector", "value"),
    Output("sel-adv", "data", allow_duplicate=True),
    Input("ch-sectors", "clickData"),
    prevent_initial_call=True,
)
def select_sector(click):
    if click:
        pts = click.get("points", [{}])
        cd = pts[0].get("customdata")
        if cd:
            sector_key = cd[0] if isinstance(cd, list) else cd
            if sector_key and isinstance(sector_key, str):
                return sector_key, None
    return dash.no_update, dash.no_update


# ── Callback: banner + clear button visibility ────────────────
@callback(
    Output("adv-banner", "children"),
    Output("adv-banner", "is_open"),
    Output("btn-clear", "style"),
    Input("sel-adv", "data"),
)
def update_banner(adv):
    if adv:
        return [html.Strong(adv), " — klikni ✕ pro zrušení filtru"], True, {"display": "block"}
    return "", False, {"display": "none"}


# ── Callback: populate filter options after lazy load ────────
@callback(
    Output("f-sector", "options", allow_duplicate=True),
    Output("f-plat", "options", allow_duplicate=True),
    Output("f-plat", "value", allow_duplicate=True),
    Output("f-search", "options", allow_duplicate=True),
    Output("f-daterange", "min_date_allowed"),
    Output("f-daterange", "max_date_allowed"),
    Output("f-daterange", "start_date"),
    Output("f-daterange", "end_date"),
    Input("tabs", "active_tab"),
    State("f-daterange", "start_date"),
    State("f-daterange", "end_date"),
    prevent_initial_call='initial_duplicate',
)
def populate_filters(_, cur_start, cur_end):
    _ensure_data()
    return (
        [{"label": SECTOR_NAMES.get(s, s), "value": s} for s in ALL_SECTORS],
        [{"label": {"linkedin": "LinkedIn", "meta": "Meta",
                    "tiktok": "TikTok"}.get(p, p.title()), "value": p}
         for p in ALL_PLATFORMS],
        ALL_PLATFORMS,
        [{"label": a, "value": a} for a in ALL_ADVERTISERS],
        DATE_MIN,
        DATE_MAX,
        cur_start or DATE_DEFAULT_START,
        cur_end or DATE_MAX,
    )


# ── Callback: KPIs ────────────────────────────────────────────
@callback(
    Output("kpi-ads", "children"),
    Output("kpi-reach", "children"),
    Output("kpi-adv", "children"),
    Output("kpi-sec", "children"),
    Output("kpi-plat", "children"),
    *FILTER_INPUTS,
)
def update_kpis(sector, platforms, adv, search, date_start, date_end):
    d = filt(DF, sector, platforms, adv, search, date_start, date_end)
    n_ads = len(d)
    total_reach = d["reach_num"].sum()
    n_adv = d["adv"].nunique()
    n_sec = d["sector"].nunique()
    n_plat = d["platform"].nunique()
    return fmt(n_ads), fmt(total_reach), str(n_adv), str(n_sec), str(n_plat)


# ── Callback: Tab 1 — Přehled ─────────────────────────────────
@callback(
    Output("ch-plat-dist", "figure"),
    Output("ch-sectors", "figure"),
    Input("plat-metric", "value"),
    *FILTER_INPUTS,
)
def update_overview(metric, sector, platforms, adv, search, date_start, date_end):
    d = filt(DF, sector, platforms, adv, search, date_start, date_end)
    if d.empty:
        return empty_fig(), empty_fig()

    # Platform distribution — doughnut (toggle reach/count)
    pp = d.groupby("pub_platform").agg(count=("ad_id", "size"), reach=("reach_num", "sum")).reset_index()
    pp["display"] = pp["pub_platform"].map(PUB_DISPLAY).fillna(pp["pub_platform"])
    pp = pp.sort_values("count", ascending=False)
    val_col = "reach" if metric == "reach" else "count"
    hover_label = "reach" if metric == "reach" else "reklam"
    fig_plat = px.pie(pp, values=val_col, names="display", hole=0.55,
                      color="display", color_discrete_map=PLAT_COLOR)
    fig_plat.update_traces(textinfo="percent", textposition="inside",
                           insidetextorientation="horizontal",
                           textfont=dict(size=13, color="white"),
                           marker=dict(line=dict(color="white", width=2)),
                           hovertemplate=f"%{{label}}: %{{value:,.0f}} {hover_label} (%{{percent}})<extra></extra>")
    fig_plat.update_layout(legend=dict(orientation="h", y=0, x=0.5, xanchor="center", yanchor="top"))
    styled(fig_plat)

    # Sectors — horizontal bar by reach
    sa = d.groupby(["sector", "sector_display"]).agg(
        count=("ad_id", "size"), reach=("reach_num", "sum")).reset_index()
    sa = sa.sort_values("reach", ascending=True).tail(15)
    sa["reach_label"] = sa["reach"].apply(fmt)
    fig_sec = px.bar(sa, x="reach", y="sector_display", orientation="h",
                     custom_data=["sector", "count", "reach_label"],
                     text="reach_label",
                     labels={"reach": "Celkový reach", "sector_display": ""},
                     color_discrete_sequence=["#6366f1"])
    fig_sec.update_traces(
        hovertemplate="<b>%{y}</b><br>Reach: %{customdata[2]}<br>Reklam: %{customdata[1]:,}<extra></extra>",
        textposition="outside", cliponaxis=False,
        marker=dict(cornerradius=4))
    fig_sec.update_layout(xaxis=dict(visible=False))
    styled(fig_sec)

    return fig_plat, fig_sec


# ── Callback: Přehled — timeline ─────────────────────────────
@callback(
    Output("ch-timeline", "figure"),
    Input("timeline-gran", "value"),
    *FILTER_INPUTS,
)
def update_timeline(gran, sector, platforms, adv, search, date_start, date_end):
    dd = filt_daily(DF_DAILY, sector, platforms, adv, search, date_start, date_end)
    if dd.empty:
        return empty_fig("Denní data nejsou k dispozici")

    # Visualizační okno: posledních 12 měsíců, ale ne dříve než duben 2025 (retention edge effect — pipeline začal sběr jaro 2025)
    if DATE_MAX:
        cutoff_12m = (pd.Timestamp(DATE_MAX) - pd.DateOffset(months=12)).strftime("%Y-%m-%d")
        cutoff_start = max(cutoff_12m, "2025-04-01")
        cutoff_end = _cutoff_end_for_gran(DATE_MAX, gran)
        date_str = dd["date"].astype(str).str[:10]
        dd = dd[(date_str >= cutoff_start) & (date_str <= cutoff_end)]
        if dd.empty:
            return empty_fig("Žádná data v daném okně")

    agg = agg_by_granularity(dd, gran, "sector_display")
    top = agg.groupby("sector_display")["reach"].sum().nlargest(8).index.tolist()
    agg = agg[agg["sector_display"].isin(top)]
    # Period je STRING (week/month/quarter); explicitní převod na datetime, ať Plotly osu vykreslí jako date axis.
    if gran == "month":
        agg["period"] = pd.to_datetime(agg["period"] + "-01")
    else:
        agg["period"] = pd.to_datetime(agg["period"])

    show_markers = gran != "day"
    fig = px.line(agg, x="period", y="reach", color="sector_display",
                  labels={"period": "", "reach": "Dosah", "sector_display": ""},
                  markers=show_markers)
    hover_fmt = "%d.%m.%Y" if gran in ("day", "week") else "%m/%Y"
    fig.update_traces(hovertemplate=f"%{{x|{hover_fmt}}}<br>%{{fullData.name}}: %{{y:,.0f}}<extra></extra>")
    if show_markers:
        fig.update_traces(marker=dict(size=4))
    tick_fmt = "%d.%m" if gran in ("day", "week") else "%m/%Y"
    fig.update_layout(
        legend=dict(orientation="h", y=1.05, x=0.5, xanchor="center"),
        xaxis=dict(tickformat=tick_fmt, dtick="M1", tickangle=-45),
    )
    styled(fig)
    return fig


# ── Callback: Tab SoV — sezónní line chart ───────────────────
@callback(
    Output("ch-season-line", "figure"),
    Input("season-gran", "value"),
    Input("season-metric", "value"),
    *FILTER_INPUTS,
)
def update_season(gran, metric, sector, platforms, adv, search, date_start, date_end):
    dd = filt_daily(DF_DAILY, sector, platforms, adv, search, date_start, date_end)
    if dd.empty:
        return empty_fig("Denní data nejsou k dispozici")

    # Visualizační okno: posledních 12 měsíců, ale ne dříve než duben 2025 (retention edge effect)
    if DATE_MAX:
        cutoff_12m = (pd.Timestamp(DATE_MAX) - pd.DateOffset(months=12)).strftime("%Y-%m-%d")
        cutoff_start = max(cutoff_12m, "2025-04-01")
        cutoff_end = _cutoff_end_for_gran(DATE_MAX, gran)
        date_str = dd["date"].astype(str).str[:10]
        dd = dd[(date_str >= cutoff_start) & (date_str <= cutoff_end)]
        if dd.empty:
            return empty_fig("Žádná data v posledních 12 měsících")

    time_col = GRAN_COL.get(gran, "week")

    if metric == "count":
        agg = dd.groupby([time_col, "advertiser_normalized"]).size().reset_index(name="value")
        y_label = "Počet reklam"
    else:
        agg = dd.groupby([time_col, "advertiser_normalized"])["reach_daily"].sum().reset_index(name="value")
        y_label = "Dosah"

    agg = agg.rename(columns={time_col: "period"}).sort_values("period")
    # Period je STRING (week/month). Plotly potřebuje datetime, jinak osu vykreslí jako kategorickou a dtick ignoruje.
    if gran == "month":
        agg["period"] = pd.to_datetime(agg["period"] + "-01")
    else:
        agg["period"] = pd.to_datetime(agg["period"])
    top_advs = agg.groupby("advertiser_normalized")["value"].sum().nlargest(10).index.tolist()
    agg = agg[agg["advertiser_normalized"].isin(top_advs)]

    show_markers = gran != "day"
    fig_line = px.line(agg, x="period", y="value", color="advertiser_normalized",
                       labels={"period": "", "value": y_label, "advertiser_normalized": ""},
                       markers=show_markers, line_shape="spline")
    hover_fmt = "%d.%m.%Y" if gran in ("day", "week") else "%m/%Y"
    fig_line.update_traces(hovertemplate=f"%{{x|{hover_fmt}}}<br>%{{fullData.name}}: %{{y:,.0f}}<extra></extra>",
                           line=dict(width=2))
    if show_markers:
        fig_line.update_traces(marker=dict(size=4))
    tick_fmt = "%d.%m" if gran in ("day", "week") else "%m/%Y"
    fig_line.update_layout(
        legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
        xaxis=dict(tickformat=tick_fmt, dtick="M1", tickangle=-45),
    )
    styled(fig_line, 600)
    return fig_line


# ── Callback: Tab 2 — Share of Voice ──────────────────────────
@callback(
    Output("ch-sov", "figure"),
    *FILTER_INPUTS,
)
def update_sov(sector, platforms, adv, search, date_start, date_end):
    d = filt(DF, sector, platforms, adv, search, date_start, date_end)
    if d.empty:
        return empty_fig()

    # Agregace per advertiser per platform
    agg_plat = d.groupby(["adv", "platform"]).agg(
        reach=("reach_num", "sum"),
        count=("ad_id", "size"),
    ).reset_index()

    # Top 20 dle celkového reach
    top20 = d.groupby("adv")["reach_num"].sum().nlargest(20).index.tolist()
    agg_plat = agg_plat[agg_plat["adv"].isin(top20)]

    # Celkový součet pro procenta
    total_reach = agg_plat["reach"].sum()
    total_count = agg_plat["count"].sum()
    agg_plat["reach_pct"] = (agg_plat["reach"] / total_reach * 100).round(1)
    agg_plat["count_pct"] = (agg_plat["count"] / total_count * 100).round(1)
    agg_plat["platform_display"] = agg_plat["platform"].map(
        {"meta": "Meta", "tiktok": "TikTok", "linkedin": "LinkedIn"})

    # Řazení dle celkového reach sestupně
    adv_order = (agg_plat.groupby("adv")["reach"].sum()
                 .sort_values(ascending=False).index.tolist())

    # Count per advertiser (pro grouped bar)
    agg_total = agg_plat.groupby("adv").agg(
        total_reach_pct=("reach_pct", "sum"),
        total_count_pct=("count_pct", "sum"),
        total_count=("count", "sum"),
    ).reset_index()
    agg_total = agg_total.set_index("adv").reindex(adv_order).reset_index()

    fig = go.Figure()

    # Reach stacked per platforma (offsetgroup="reach")
    for plat_name, color in PLAT_BAR.items():
        plat_data = agg_plat[agg_plat["platform_display"] == plat_name]
        if plat_data.empty:
            continue
        plat_data = plat_data.set_index("adv").reindex(adv_order).reset_index()
        fig.add_trace(go.Bar(
            x=plat_data["adv"], y=plat_data["reach_pct"],
            name=plat_name,
            marker_color=color, marker_cornerradius=4,
            offsetgroup="reach",
            legendgroup="Reach",
            legendgrouptitle_text="Reach",
            customdata=np.stack([
                plat_data["adv"].fillna(""),
                plat_data["reach"].apply(lambda v: fmt(v) if pd.notna(v) else "0"),
            ], axis=1),
            hovertemplate="<b>%{customdata[0]}</b> — %{fullData.name}<br>"
                          "Reach: %{customdata[1]} (%{y:.1f}%)<extra></extra>",
        ))

    # Count jako jeden bar (offsetgroup="count")
    fig.add_trace(go.Bar(
        x=agg_total["adv"], y=agg_total["total_count_pct"],
        name="Celkem",
        marker_color="#c7d2fe", marker_cornerradius=4,
        offsetgroup="count",
        legendgroup="Počet reklam",
        legendgrouptitle_text="Počet reklam",
        text=agg_total["total_count_pct"].apply(lambda p: f"{p:.1f}%"),
        textposition="outside",
        textfont=dict(size=10, color="#475569"),
        cliponaxis=False,
        customdata=np.stack([
            agg_total["adv"].fillna(""),
            agg_total["total_count"].fillna(0).astype(int).astype(str),
        ], axis=1),
        hovertemplate="<b>%{customdata[0]}</b><br>"
                      "Reklam: %{customdata[1]} (%{y:.1f}%)<extra></extra>",
    ))

    # Invisible bar nad stacked reach pro zobrazení celkového % nahoře
    fig.add_trace(go.Bar(
        x=agg_total["adv"],
        y=[0] * len(agg_total),
        base=agg_total["total_reach_pct"],
        text=agg_total["total_reach_pct"].apply(lambda p: f"{p:.1f}%"),
        textposition="outside",
        textfont=dict(size=10, color="#475569"),
        marker=dict(color="rgba(0,0,0,0)"),
        offsetgroup="reach",
        showlegend=False,
        hoverinfo="skip",
        cliponaxis=False,
    ))

    fig.update_layout(
        barmode="stack",
        legend=dict(orientation="h", y=1.03, x=0.5, xanchor="center"),
        yaxis=dict(title="Podíl z celku (%)", ticksuffix=" %"),
        xaxis=dict(title="", tickangle=-45),
    )
    styled(fig, 600)
    return fig


# ── Callback: Tab 3 — Demografie ──────────────────────────────
@callback(
    Output("ch-demo-age", "figure"),
    Output("ch-demo-gender", "figure"),
    Output("ch-demo-adv", "figure"),
    *FILTER_INPUTS,
)
def update_demo(sector, platforms, adv, search, date_start, date_end):
    dd = filt_demo(DF_DEMO, sector, adv, search, date_start, date_end)
    if dd.empty:
        return (empty_fig("Demografická data pouze pro Meta (CZ)"),
                empty_fig("Demografická data pouze pro Meta (CZ)"),
                empty_fig("Demografická data pouze pro Meta (CZ)"))

    # Age bar — drop ne-standardní buckety (Meta vrací "Unknown") a prázdné (13-17 je v CZ datech vždy 0)
    dd_age = dd[dd["age_range"].isin(AGE_BUCKETS)]
    age_agg = dd_age.groupby("age_range")[["male", "female", "unknown"]].sum().reset_index()
    age_agg["total"] = age_agg["male"] + age_agg["female"] + age_agg["unknown"]
    age_agg = age_agg[age_agg["total"] >= 1000]
    bucket_order = {b: i for i, b in enumerate(AGE_BUCKETS)}
    age_agg["sort"] = age_agg["age_range"].map(bucket_order)
    age_agg = age_agg.sort_values("sort")
    age_long = age_agg.melt(id_vars="age_range", value_vars=["male", "female", "unknown"],
                             var_name="gender", value_name="reach")
    gender_names = {"male": "Muži", "female": "Ženy", "unknown": "Neuvedeno"}
    age_long["gender_cz"] = age_long["gender"].map(gender_names)
    present_buckets = [b for b in AGE_BUCKETS if b in age_agg["age_range"].values]
    fig_age = px.bar(age_long, x="age_range", y="reach", color="gender_cz",
                     barmode="group", color_discrete_map={"Muži": "#6366f1", "Ženy": "#f472b6", "Neuvedeno": "#cbd5e1"},
                     labels={"age_range": "Věková skupina", "reach": "Dosah", "gender_cz": "Pohlaví"},
                     category_orders={"age_range": present_buckets})
    fig_age.update_traces(hovertemplate="%{x}: %{y:,.0f}",
                          marker=dict(cornerradius=3))
    styled(fig_age)

    # Gender doughnut
    gtot = dd[["male", "female", "unknown"]].sum()
    fig_gen = px.pie(values=[gtot["male"], gtot["female"], gtot["unknown"]],
                     names=["Muži", "Ženy", "Neuvedeno"], hole=0.55,
                     color_discrete_map={"Muži": "#6366f1", "Ženy": "#f472b6", "Neuvedeno": "#cbd5e1"})
    fig_gen.update_traces(textinfo="label+percent", hovertemplate="%{label}: %{value:,.0f} (%{percent})",
                          textfont=dict(size=12),
                          marker=dict(line=dict(color="white", width=2)))
    styled(fig_gen)

    # Top advertisers by gender split
    adv_g = dd.groupby("adv")[["male", "female"]].sum().reset_index()
    adv_g["total"] = adv_g["male"] + adv_g["female"]
    adv_g = adv_g.sort_values("total", ascending=True).tail(20)
    fig_adv = go.Figure()
    fig_adv.add_trace(go.Bar(y=adv_g["adv"], x=adv_g["male"], name="Muži",
                             orientation="h", marker_color="#6366f1",
                             marker_cornerradius=4,
                             customdata=adv_g["adv"],
                             hovertemplate="%{y}: %{x:,.0f} muži<extra></extra>"))
    fig_adv.add_trace(go.Bar(y=adv_g["adv"], x=adv_g["female"], name="Ženy",
                             orientation="h", marker_color="#f472b6",
                             marker_cornerradius=4,
                             customdata=adv_g["adv"],
                             hovertemplate="%{y}: %{x:,.0f} ženy<extra></extra>"))
    fig_adv.update_layout(barmode="stack")
    styled(fig_adv)

    return fig_age, fig_gen, fig_adv


# ── Callback: Tab 4 — Strategie ──────────────────────────────
@callback(
    Output("ch-coverage", "figure"),
    Output("ch-cross-plat", "figure"),
    Output("ch-payers", "figure"),
    Output("ch-duration", "figure"),
    *FILTER_INPUTS,
)
def update_strat(sector, platforms, adv, search, date_start, date_end):
    d = filt(DF, sector, platforms, adv, search, date_start, date_end)
    if d.empty:
        return empty_fig(), empty_fig(), empty_fig(), empty_fig()

    # Significance threshold: advertiser counts as "present on a platform"
    # only if it has at least this many ads there. Prevents LinkedIn spillover
    # (single ad categorized as CZ) from inflating cross-platform counts.
    MIN_ADS_PER_PLATFORM = 10

    plat_stats = d.groupby(["adv", "platform"]).agg(
        n_ads=("ad_id", "size"),
        reach=("reach_num", "sum"),
    ).reset_index()
    significant = plat_stats[plat_stats["n_ads"] >= MIN_ADS_PER_PLATFORM]

    # Platform coverage — how many advertisers have significant presence on N platforms
    adv_plats = significant.groupby("adv").size().reset_index(name="n_plat")
    if adv_plats.empty:
        fig_cov = empty_fig("Žádní inzerenti s významnou přítomností")
    else:
        cov = adv_plats.groupby("n_plat").size().reset_index(name="count")
        cov["label"] = cov["n_plat"].astype(str) + " platforma/y"
        fig_cov = px.bar(cov, x="label", y="count", color_discrete_sequence=["#a78bfa"],
                         labels={"label": "Počet platforem", "count": "Inzerentů"})
        fig_cov.update_traces(hovertemplate="%{x}: %{y} inzerentů")
    styled(fig_cov)

    # Cross-platform advertisers — stacked by platform REACH (SoV)
    multi_advs = adv_plats[adv_plats["n_plat"] >= 2]["adv"].tolist()
    if not multi_advs:
        fig_cross = empty_fig("Žádní inzerenti na více platformách")
    else:
        mp = significant[significant["adv"].isin(multi_advs)].copy()
        top_multi = mp.groupby("adv")["reach"].sum().nlargest(20).index.tolist()
        mp = mp[mp["adv"].isin(top_multi)]
        adv_order = mp.groupby("adv")["reach"].sum().sort_values().index.tolist()
        mp["platform_display"] = mp["platform"].map({"meta": "Meta", "tiktok": "TikTok", "linkedin": "LinkedIn"})
        mp["reach_label"] = mp["reach"].apply(fmt)
        fig_cross = px.bar(mp, x="reach", y="adv", color="platform_display", orientation="h",
                           custom_data=["adv", "reach_label"],
                           color_discrete_map=PLAT_BAR,
                           labels={"reach": "Reach (Share of Voice)", "adv": "", "platform_display": ""},
                           category_orders={"adv": adv_order})
        fig_cross.update_layout(barmode="stack",
                                legend=dict(orientation="h", y=1.05, x=0.5, xanchor="center"))
        fig_cross.update_traces(
            hovertemplate="<b>%{y}</b> — %{fullData.name}<br>Reach: %{customdata[1]}<extra></extra>")
    styled(fig_cross)

    # Payers — only genuine third parties (media agencies & external payers)
    pay = d[d["payer_norm"] != ""].copy()
    if pay.empty:
        fig_pay = empty_fig("Žádná data o plátcích")
    else:
        # Aggregate per normalized payer, collect client list
        pay_agg = pay.groupby("payer_norm").agg(
            count=("ad_id", "size"),
            reach=("reach_num", "sum"),
            advertisers=("adv", lambda x: list(x.unique())),
            payer_raw=("payer", lambda x: x.mode().iloc[0] if len(x) > 0 else ""),
        ).reset_index()

        # Filter out self-payers (where payer name overlaps with sole advertiser)
        pay_agg["is_tp"] = pay_agg.apply(
            lambda r: is_third_party(r["payer_raw"], r["advertisers"]), axis=1)
        pay_agg = pay_agg[pay_agg["is_tp"]]

        if pay_agg.empty:
            fig_pay = empty_fig("Žádní plátci třetích stran pro aktuální filtr")
        else:
            # Rank by reach (Share of Voice), not count of ads
            top_payers = pay_agg.sort_values("reach", ascending=False).head(20)
            top_payer_norms = top_payers["payer_norm"].tolist()
            payer_name_map = dict(zip(top_payers["payer_norm"], top_payers["payer_raw"]))
            payer_count_map = dict(zip(top_payers["payer_norm"], top_payers["count"]))

            # Break down reach by (payer, advertiser) for stacked bar
            breakdown = (pay[pay["payer_norm"].isin(top_payer_norms)]
                         .groupby(["payer_norm", "adv"]).agg(
                             reach=("reach_num", "sum"),
                             count=("ad_id", "size"),
                         ).reset_index())
            breakdown["payer_raw"] = breakdown["payer_norm"].map(payer_name_map)
            breakdown["reach_label"] = breakdown["reach"].apply(fmt)

            # Order payers by total reach (ascending for horizontal bar = largest at top)
            payer_order = top_payers.sort_values("reach", ascending=True)["payer_raw"].tolist()

            fig_pay = px.bar(
                breakdown, x="reach", y="payer_raw", color="adv",
                orientation="h",
                custom_data=["adv", "reach_label", "count"],
                labels={"reach": "Reach (Share of Voice)", "payer_raw": "", "adv": "Klient"},
                category_orders={"payer_raw": payer_order},
                color_discrete_sequence=px.colors.qualitative.Bold + px.colors.qualitative.Safe,
            )
            fig_pay.update_layout(
                barmode="stack",
                showlegend=False,  # too many clients for useful legend; info is in hover
            )
            fig_pay.update_traces(
                hovertemplate="<b>%{y}</b><br>Klient: %{customdata[0]}<br>Reach: %{customdata[1]}<br>Reklam: %{customdata[2]}<extra></extra>"
            )

            # Total count per payer jako text vpravo od baru
            totals = top_payers.set_index("payer_raw")["count"].to_dict()
            text_x = top_payers.set_index("payer_raw")["reach"].to_dict()
            fig_pay.add_trace(go.Scatter(
                x=[text_x[p] for p in payer_order],
                y=payer_order,
                mode="text",
                text=[f"{totals[p]:,}".replace(",", " ") + " reklam" for p in payer_order],
                textposition="middle right",
                textfont=dict(size=10, color="#475569"),
                showlegend=False,
                hoverinfo="skip",
                cliponaxis=False,
            ))
    styled(fig_pay)

    # Trvání reklam — top 15 inzerentů (box plot)
    top15 = d.groupby("adv")["reach_num"].sum().nlargest(15).index.tolist()
    dur_data = d[d["adv"].isin(top15)].copy()
    dur_data["_start"] = pd.to_datetime(dur_data["date_start"], errors="coerce")
    dur_data["_stop"] = pd.to_datetime(dur_data["date_stop"], errors="coerce")
    dur_data["duration"] = (dur_data["_stop"] - dur_data["_start"]).dt.days + 1
    dur_data = dur_data[dur_data["duration"].notna() & (dur_data["duration"] > 0)]

    if dur_data.empty:
        fig_dur = empty_fig("Žádná data o trvání reklam")
    else:
        # Buckety: burst → standard → sustained → always-on
        def bucket(d):
            if d <= 3: return "Velmi krátké (≤3 dny)"
            if d <= 14: return "Krátké (4–14 dní)"
            if d <= 60: return "Střední (15–60 dní)"
            return "Dlouhé (60+ dní)"
        dur_data["bucket"] = dur_data["duration"].apply(bucket)
        BUCKET_ORDER = ["Velmi krátké (≤3 dny)", "Krátké (4–14 dní)",
                        "Střední (15–60 dní)", "Dlouhé (60+ dní)"]
        BUCKET_COLORS = {
            "Velmi krátké (≤3 dny)": "#ef4444",
            "Krátké (4–14 dní)": "#f59e0b",
            "Střední (15–60 dní)": "#10b981",
            "Dlouhé (60+ dní)": "#6366f1",
        }

        # % per advertiser per bucket
        counts = dur_data.groupby(["adv", "bucket"]).size().reset_index(name="n")
        totals = counts.groupby("adv")["n"].sum().reset_index(name="total")
        mix = counts.merge(totals, on="adv")
        mix["pct"] = (mix["n"] / mix["total"] * 100).round(1)

        # Order: největší reach nahoře (lídři sektoru), nejmenší dole
        adv_order = dur_data.groupby("adv")["reach_num"].sum().sort_values(ascending=False).index.tolist()

        fig_dur = px.bar(
            mix, x="pct", y="adv", color="bucket", orientation="h",
            category_orders={"adv": adv_order, "bucket": BUCKET_ORDER},
            color_discrete_map=BUCKET_COLORS,
            labels={"pct": "Podíl reklam (%)", "adv": "", "bucket": ""},
        )
        fig_dur.update_layout(
            barmode="stack",
            legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center", yanchor="bottom"),
            margin=dict(l=10, r=20, t=60, b=40),
            xaxis=dict(ticksuffix=" %", range=[0, 100]),
        )
        fig_dur.update_traces(
            hovertemplate="<b>%{y}</b> — %{fullData.name}<br>%{x:.1f} %<extra></extra>",
        )
    styled(fig_dur, 600)

    return fig_cov, fig_cross, fig_pay, fig_dur


# ── Callback: Tab 5 — Cílení ─────────────────────────────────
@callback(
    Output("ch-tgt-age-d", "figure"),
    Output("ch-tgt-gen-d", "figure"),
    Output("ch-tgt-gen-bar", "figure"),
    Output("ch-tgt-int", "figure"),
    *FILTER_INPUTS,
)
def update_targeting(sector, platforms, adv, search, date_start, date_end):
    d = filt(DF, sector, platforms, adv, search, date_start, date_end)
    if d.empty:
        return empty_fig(), empty_fig(), empty_fig(), empty_fig()

    # Exclude linkedin from targeting analysis (no useful targeting data)
    dt = d[d["platform"].isin(["meta", "tiktok"])]
    if dt.empty:
        return empty_fig(), empty_fig(), empty_fig(), empty_fig()

    # 1. Age doughnut — broad vs narrow
    n_narrow = dt["tgt_narrow"].sum()
    n_broad = len(dt) - n_narrow
    fig_age = px.pie(values=[n_broad, n_narrow],
                     names=["Široké cílení (všechny věky)", "Úzké cílení (≤3 skupiny)"],
                     hole=0.55, color_discrete_sequence=["#cbd5e1", "#a78bfa"])
    fig_age.update_traces(textinfo="label+percent",
                          hovertemplate="%{label}: %{value:,} reklam (%{percent})")
    styled(fig_age)

    # 2. Gender doughnut
    gc = dt["tgt_gender"].value_counts()
    n_all = int(gc.get("all", 0))
    n_f = int(gc.get("female", 0))
    n_m = int(gc.get("male", 0))
    fig_gen = px.pie(values=[n_all, n_m, n_f],
                     names=["Bez omezení pohlaví", "Muži", "Ženy"],
                     hole=0.55,
                     color_discrete_map={"Bez omezení pohlaví": "#94a3b8", "Muži": "#6366f1", "Ženy": "#f472b6"})
    fig_gen.update_traces(textinfo="label+percent",
                          hovertemplate="%{label}: %{value:,} reklam (%{percent})")
    styled(fig_gen)

    # 3. Gender-specific advertisers (>= 5% of their ads)
    gs = dt[dt["tgt_gender"] != "all"].copy()
    if gs.empty:
        fig_gbar = empty_fig("Žádné cílení dle pohlaví")
    else:
        adv_total = dt.groupby("adv").size().reset_index(name="total")
        gs_agg = gs.groupby(["adv", "tgt_gender"]).size().reset_index(name="count")
        gs_adv = gs_agg.groupby("adv")["count"].sum().reset_index(name="gs_total")
        gs_adv = gs_adv.merge(adv_total, on="adv")
        gs_adv["pct"] = gs_adv["gs_total"] / gs_adv["total"] * 100
        # Filter: >= 5% unless single advertiser selected
        if not adv:
            gs_adv = gs_adv[gs_adv["pct"] >= 5]
        keep_advs = set(gs_adv.sort_values("gs_total", ascending=False).head(15)["adv"])
        gs_plot = gs_agg[gs_agg["adv"].isin(keep_advs)].copy()
        gs_plot["gender_cz"] = gs_plot["tgt_gender"].map({"male": "Muži", "female": "Ženy"})
        # Sort by total
        adv_order = gs_plot.groupby("adv")["count"].sum().sort_values().index.tolist()
        fig_gbar = px.bar(gs_plot, x="count", y="adv", color="gender_cz", orientation="h",
                          custom_data=["adv"],
                          color_discrete_map={"Muži": "#6366f1", "Ženy": "#f472b6"},
                          labels={"count": "Počet reklam", "adv": "", "gender_cz": ""},
                          category_orders={"adv": adv_order})
        fig_gbar.update_layout(barmode="stack")
        fig_gbar.update_traces(hovertemplate="%{y}: %{x:,} reklam<extra></extra>")
    styled(fig_gbar)

    # 4. TikTok interests
    int_data = dt[dt["tgt_interests"] != ""]["tgt_interests"]
    if int_data.empty:
        fig_int = empty_fig("Žádná data o zájmech (pouze TikTok)")
    else:
        all_interests = []
        for s in int_data:
            all_interests.extend([i.strip() for i in str(s).split(",") if i.strip()])
        int_counts = pd.Series(all_interests).value_counts().head(15).reset_index()
        int_counts.columns = ["interest", "count"]
        int_counts = int_counts.sort_values("count", ascending=True)
        fig_int = px.bar(int_counts, x="count", y="interest", orientation="h",
                         color_discrete_sequence=["#1a1a1a"],
                         labels={"count": "Počet reklam", "interest": ""})
        fig_int.update_traces(hovertemplate="%{y}: %{x:,} reklam<extra></extra>")
    styled(fig_int)

    return fig_age, fig_gen, fig_gbar, fig_int


# ── Callback: Tab 6 — Kreativa ──────────────────────────────
@callback(
    Output("creative-stats", "children"),
    Output("tbl-creative", "data"),
    *FILTER_INPUTS,
)
def update_creative(sector, platforms, adv, search, date_start, date_end):
    d = filt(DF, sector, platforms, adv, search, date_start, date_end)
    if d.empty:
        return "Žádná data pro aktuální filtr.", []

    total = len(d)
    d = d.sort_values("reach_num", ascending=False).head(2000).copy()

    d["rank"] = range(1, len(d) + 1)
    d["reach_fmt"] = d["reach_num"].apply(fmt)
    d["date_short"] = d["date_start"].astype(str).str[:10]

    has_url = d["creative_url"].notna() & d["creative_url"].astype(str).ne("") & d["creative_url"].astype(str).ne("nan")
    d["link"] = "—"
    d.loc[has_url, "link"] = "[Zobrazit](" + d.loc[has_url, "creative_url"].astype(str) + ")"

    shown = len(d)
    n_with_url = has_url.sum()
    stats = f"Top {shown:,} reklam dle dosahu" + (f" (z celkem {total:,})" if total > shown else "") + f" · {n_with_url:,} s odkazem na kreativu"

    records = d[["rank", "adv", "pub_platform", "reach_fmt", "date_short", "sector_display", "link"]].rename(
        columns={"date_short": "date_start"}
    ).to_dict("records")

    return stats, records


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    _ensure_data()
    port = int(os.environ.get("PORT", 8050))
    app.run(debug=True, host="0.0.0.0", port=port)
