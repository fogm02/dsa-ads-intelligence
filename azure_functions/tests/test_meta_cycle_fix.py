"""
Testy fix-u cycle restart logiky v bp_meta_collect.py.

Pokrývají:
  1. Force restart cyklu při timeout (stary cycle_id)
  2. Normální restart při all complete
  3. Žádný restart když fresh cycle a něco pending
  4. Žádný restart když fresh cycle a in_progress sektor
  5. Po restartu: completed_pages a page_cursors vyčištěné
  6. reset_complete_sectors_with_new_pages funguje pro legitimní case
  7. Pořadí v meta_collect — cycle check první, pak reset_complete

Spustit z azure_functions/:
    python3 tests/test_meta_cycle_fix.py
"""

import os
import sys
import json
from datetime import datetime, timezone, timedelta

# Setup path
HERE = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(HERE)
sys.path.insert(0, PARENT)

# Load env vars (potřebné pro shared imports — nepoužijeme reálné Blob volání)
with open(os.path.join(PARENT, "local.settings.json")) as f:
    settings = json.load(f)
for k, v in settings.get("Values", {}).items():
    os.environ.setdefault(k, v)

import logging
logging.basicConfig(level=logging.WARNING, format="%(message)s")

from blueprints.bp_meta_collect import (
    check_and_start_new_cycle,
    reset_complete_sectors_with_new_pages,
    get_sector_state,
    MAX_CYCLE_AGE_DAYS,
)


# ──── Helpers ────────────────────────────────────────────────
def make_state(cycle_id, sectors_dict):
    """Vytvoří fake state. sectors_dict = {sector_key: status_string}."""
    return {
        "cycle_id": cycle_id,
        "version": 1,
        "sectors": {
            sk: {
                "status": status,
                "completed_pages": ["p1", "p2"],
                "page_cursors": {"p3": "cursor_data"},
                "ads_collected": 100,
                "last_run_at": "2026-04-01T00:00:00",
            }
            for sk, status in sectors_dict.items()
        },
    }


def days_ago(n):
    return (datetime.now(timezone.utc) - timedelta(days=n)).strftime("%Y%m%d_%H%M%S")


# ──── Tests ──────────────────────────────────────────────────
PASS = 0
FAIL = 0


def test(name):
    """Decorator pro přehledný report."""
    def decorator(func):
        global PASS, FAIL
        try:
            func()
            print(f"  ✅ {name}")
            PASS += 1
        except AssertionError as e:
            print(f"  ❌ {name}")
            print(f"     {e}")
            FAIL += 1
        except Exception as e:
            print(f"  ❌ {name} (unexpected: {type(e).__name__})")
            print(f"     {e}")
            FAIL += 1
        return func
    return decorator


print("=" * 70)
print("TEST: cycle restart fix v bp_meta_collect.py")
print("=" * 70)
print()


print("--- 1. Force restart při timeout ---")

@test("Old cycle (37 dní) + all complete → restart=True")
def t1():
    state = make_state(days_ago(37), {"s1": "complete", "s2": "complete"})
    result = check_and_start_new_cycle(state, {"s1": {}, "s2": {}})
    assert result is True, f"očekáváno True, dostal {result}"
    assert state["cycle_id"] != days_ago(37), "cycle_id se nezměnil"


@test("Old cycle (37 dní) + jeden pending → restart=True (force timeout)")
def t2():
    state = make_state(days_ago(37), {"s1": "pending", "s2": "complete"})
    result = check_and_start_new_cycle(state, {"s1": {}, "s2": {}})
    assert result is True, "force timeout měl restartovat"


@test("Old cycle (8 dní = právě nad limit 7) → restart=True")
def t3():
    state = make_state(days_ago(8), {"s1": "complete"})
    result = check_and_start_new_cycle(state, {"s1": {}})
    assert result is True, f"timeout 8>{MAX_CYCLE_AGE_DAYS} měl trigger"


@test("Old cycle (6 dní = pod limit) + jeden pending → restart=False")
def t4():
    state = make_state(days_ago(6), {"s1": "pending", "s2": "complete"})
    result = check_and_start_new_cycle(state, {"s1": {}, "s2": {}})
    assert result is False, "bez timeoutu a s pending NEMĚL restartovat"


print()
print("--- 2. Normální all-complete restart ---")

@test("Recent cycle (1 den) + all complete → restart=True")
def t5():
    state = make_state(days_ago(1), {"s1": "complete", "s2": "complete", "s3": "complete"})
    result = check_and_start_new_cycle(state, {"s1": {}, "s2": {}, "s3": {}})
    assert result is True, "all complete měl trigger"


@test("Recent cycle + in_progress sektor → restart=False")
def t6():
    state = make_state(days_ago(2), {"s1": "complete", "s2": "in_progress"})
    result = check_and_start_new_cycle(state, {"s1": {}, "s2": {}})
    assert result is False, "in_progress není complete → no restart"


print()
print("--- 3. Stav po restartu ---")

@test("Po restartu: všechny sektory pending")
def t7():
    state = make_state(days_ago(37), {"s1": "complete", "s2": "complete"})
    check_and_start_new_cycle(state, {"s1": {}, "s2": {}})
    for sk in ("s1", "s2"):
        assert state["sectors"][sk]["status"] == "pending", f"{sk} není pending"


@test("Po restartu: completed_pages vyčištěné (=[])")
def t8():
    state = make_state(days_ago(37), {"s1": "complete"})
    check_and_start_new_cycle(state, {"s1": {}})
    assert state["sectors"]["s1"]["completed_pages"] == [], "completed_pages není prázdné"


@test("Po restartu: page_cursors vyčištěné (={})")
def t9():
    state = make_state(days_ago(37), {"s1": "complete"})
    check_and_start_new_cycle(state, {"s1": {}})
    assert state["sectors"]["s1"]["page_cursors"] == {}, "page_cursors není prázdný"


@test("Po restartu: ads_collected reset na 0")
def t10():
    state = make_state(days_ago(37), {"s1": "complete"})
    check_and_start_new_cycle(state, {"s1": {}})
    assert state["sectors"]["s1"]["ads_collected"] == 0, "ads_collected není 0"


@test("Po restartu: cycle_id je nový (dnešní)")
def t11():
    state = make_state(days_ago(37), {"s1": "complete"})
    old_cid = state["cycle_id"]
    check_and_start_new_cycle(state, {"s1": {}})
    new_cid = state["cycle_id"]
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    assert old_cid != new_cid, "cycle_id se nezměnil"
    assert new_cid.startswith(today), f"nový cycle_id {new_cid} nezačíná dnešním datem"


print()
print("--- 4. Edge cases ---")

@test("Empty cycle_id (první spuštění) + all pending → restart=True (no cycle yet)")
def t12():
    state = {"cycle_id": "", "version": 1, "sectors": {}}
    result = check_and_start_new_cycle(state, {"s1": {}})
    # Empty state + meta_sectors {s1} — sektor s1 neexistuje ve state
    # get_sector_state ho vytvoří s default status. Měl by být pending.
    # all() na prázdné iter vrací True, ale pokud meta_sectors není prázdné, projde get_sector_state
    # → status=pending → not complete → return False
    # Nicméně cycle_id je prázdný → cycle_too_old=False (kód neumí parsovat "")
    # Takže result by měl být False (čeká na complete)
    assert result is False, "Empty cycle_id + pending sektor → no force restart"


@test("Invalid cycle_id format → no crash, treats as no-timeout")
def t13():
    state = make_state("invalid_format", {"s1": "complete"})
    # Měl by zpracovat všechno complete → restart=True
    result = check_and_start_new_cycle(state, {"s1": {}})
    assert result is True, "invalid cycle_id + all complete měl restartovat"


print()
print("--- 5. reset_complete_sectors_with_new_pages funguje samostatně ---")

@test("Sektor s novými pages v configu → reset na pending")
def t14():
    state = make_state(days_ago(2), {"s1": "complete"})
    state["sectors"]["s1"]["completed_pages"] = ["p1"]
    meta_sectors = {"s1": {"pages": {"p1": "page1", "p2_NEW": "page2"}}}
    reset_complete_sectors_with_new_pages(state, meta_sectors)
    assert state["sectors"]["s1"]["status"] == "pending", "sektor s novou page nepushnul na pending"


@test("Sektor bez nových pages → status zůstane complete")
def t15():
    state = make_state(days_ago(2), {"s1": "complete"})
    state["sectors"]["s1"]["completed_pages"] = ["p1", "p2"]
    meta_sectors = {"s1": {"pages": {"p1": "page1", "p2": "page2"}}}
    reset_complete_sectors_with_new_pages(state, meta_sectors)
    assert state["sectors"]["s1"]["status"] == "complete", "sektor BEZ nových pages neměl měnit status"


@test("Sektor in_progress → reset_complete ho nesahá")
def t16():
    state = make_state(days_ago(2), {"s1": "in_progress"})
    state["sectors"]["s1"]["completed_pages"] = ["p1"]
    meta_sectors = {"s1": {"pages": {"p1": "page1", "p_NEW": "page_new"}}}
    reset_complete_sectors_with_new_pages(state, meta_sectors)
    assert state["sectors"]["s1"]["status"] == "in_progress", "in_progress nesměl být změněn"


print()
print("--- 6. Integration: simulate full meta_collect order ---")

@test("Bug scenario: stale cycle (37d) + mid-cycle config change → cycle reset PŘEDtím")
def t17():
    """
    Simuluje původní bug:
    - Cycle 37 dní starý
    - User přidal nový advertiser → consulting má novou page v configu
    - Reset_complete by změnil consulting na pending
    - Bez fix-u: check_and_start_new_cycle vrátí False (jeden pending)
    - S fix-em: cycle_too_old=True → force restart napříč všemi sektory
    """
    state = make_state(days_ago(37), {
        "consulting": "complete",   # all completed_pages = ["p1", "p2"], v configu bude "p_new" navíc
        "banking": "complete",
        "ecommerce": "complete",
    })
    meta_sectors = {
        "consulting": {"pages": {"p1": "x", "p2": "y", "p_new": "z"}},
        "banking": {"pages": {}},
        "ecommerce": {"pages": {}},
    }

    # Pořadí dle nového kódu: cycle check FIRST
    result = check_and_start_new_cycle(state, meta_sectors)
    assert result is True, "stale cycle měl trigger force restart"

    # Po restartu by reset_complete neměl mít co dělat (všechny pending)
    # (volání kvůli completness)
    reset_complete_sectors_with_new_pages(state, meta_sectors)

    # Ověř že všichni jsou pending (ne consulting přepnutý ZNOVU na cokoliv)
    for sk in ("consulting", "banking", "ecommerce"):
        assert state["sectors"][sk]["status"] == "pending", \
            f"{sk} není pending po cycle restart"
        assert state["sectors"][sk]["completed_pages"] == [], \
            f"{sk} má stále completed_pages po restartu"


print()
print("=" * 70)
print(f"VÝSLEDEK: {PASS} passed, {FAIL} failed (z {PASS+FAIL} testů)")
print("=" * 70)
sys.exit(0 if FAIL == 0 else 1)
