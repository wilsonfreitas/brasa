"""Unit tests for the symbol-change detection templates (WIL-12 / WIL-130).

Each test loads the template's sql_query and runs it against synthetic tables
registered exactly as the sql_query ETL step does.
"""

import datetime as dt
from pathlib import Path

import duckdb
import pandas as pd
import yaml

COTAHIST = "staging.b3-cotahist"
RETURNS = "staging.b3-equities-returns"
CHANGES = "staging.b3-equities-symbol-changes"


def _sessions(n: int) -> list[dt.date]:
    return [dt.date(2018, 1, 1) + dt.timedelta(days=i) for i in range(n)]


def _synthetic_cotahist() -> pd.DataFrame:
    """20 sessions; src symbols trade sessions 1..8, dests start at session 9.

    Expected final pairs (rule: ret_match OR (class_ok AND same_suffix),
    two-round 1:1 matching, no price ceiling):
      KROT3->COGN3   round 1 via ret_match (+1%)
      DROP3->FALL3   round 1 via ret_match (-50%, proves signed change)
      UNTA11->UNTB11 round 1 via structure (UNT class, no returns coverage)
      BIGG3->HUGE3   round 2 via structure (+233% gap proves no price gate;
                     loses COGN3 to KROT3 in round 1, recovers HUGE3)
    Excluded:
      SRCA3 (ON) -> DSTA3 (PN): ON->PN blocked by share-class continuity
      DELS3: delisting at 5.0 — its gap to every remaining dest is the worst
             in both rounds, so it never wins a 1:1 slot
      ANCR3: anchors the session bounds, never flagged
    """
    s = _sessions(20)
    rows: list[dict] = []

    def add(symbol, isin, sess_idx, close):
        for i in sess_idx:
            rows.append(
                {
                    "refdate": s[i],
                    "symbol": symbol,
                    "close": close,
                    "isin": isin,
                    "instrument_market": 10,
                    "corporation_name": symbol[:4],
                }
            )

    early = range(0, 8)
    late = range(8, 20)
    add("ANCR3", "BRANCRACNOR0", range(0, 20), 5.0)
    add("KROT3", "BRKROTACNOR2", early, 10.0)
    add("COGN3", "BRCOGNACNOR0", late, 10.1)
    add("DROP3", "BRDROPACNOR1", early, 20.0)
    add("FALL3", "BRFALLACNOR9", late, 10.0)
    add("BIGG3", "BRBIGGACNOR3", early, 12.0)
    add("HUGE3", "BRHUGEACNOR7", late, 40.0)
    add("UNTA11", "BRUNTAUNT0R5", early, 20.0)
    add("UNTB11", "BRUNTBUNT0R6", late, 20.2)
    add("SRCA3", "BRSRCAACNOR4", early, 7.0)
    add("DSTA3", "BRDSTAACNPA5", late, 7.05)
    add("DELS3", "BRDELSACNOR8", early, 5.0)
    return pd.DataFrame(rows)


def _synthetic_returns() -> pd.DataFrame:
    """Adjusted returns covering only the two ret_match pairs' change date."""
    change_date = _sessions(20)[8]
    return pd.DataFrame(
        [
            {
                "refdate": change_date,
                "symbol": "COGN3",
                "pct_return": 0.01,
                "log_return": 0.00995,
            },
            {
                "refdate": change_date,
                "symbol": "FALL3",
                "pct_return": -0.50,
                "log_return": -0.69315,
            },
        ]
    )


def _load_query(template_path: str) -> str:
    doc = yaml.safe_load(Path(template_path).read_text())
    return doc["etl"]["pipeline"][0]["query"]


def _run_changes() -> pd.DataFrame:
    query = _load_query(
        "brasa/files/templates/b3/equities/b3-equities-symbol-changes.yaml"
    )
    conn = duckdb.connect(":memory:")
    conn.register(COTAHIST, _synthetic_cotahist())
    conn.register(RETURNS, _synthetic_returns())
    try:
        return conn.execute(query).fetch_df()
    finally:
        conn.close()


def test_symbol_changes_confidence_rule_and_two_rounds():
    df = _run_changes()
    pairs = set(zip(df["src_symbol"], df["dest_symbol"], strict=True))
    assert pairs == {
        ("KROT3", "COGN3"),
        ("DROP3", "FALL3"),
        ("UNTA11", "UNTB11"),
        ("BIGG3", "HUGE3"),
    }
    # ON->PN blocked entirely: DSTA3 never appears as a dest
    assert "DSTA3" not in df["dest_symbol"].values
    # delisting and blocked src excluded
    assert "DELS3" not in df["src_symbol"].values
    assert "SRCA3" not in df["src_symbol"].values


def test_symbol_changes_signed_change_and_ret_match():
    df = _run_changes().set_index("src_symbol")
    # signed: falling price is negative
    assert abs(df.loc["DROP3", "close_change_pct"] - (-0.50)) < 1e-12
    assert abs(df.loc["BIGG3", "close_change_pct"] - 28 / 12) < 1e-12
    # ret_match: True where covered, NULL (NaN/None) where not
    assert bool(df.loc["KROT3", "ret_match"]) is True
    assert bool(df.loc["DROP3", "ret_match"]) is True
    assert pd.isna(df.loc["UNTA11", "ret_match"])
    assert pd.isna(df.loc["BIGG3", "ret_match"])


def test_symbol_spans_analysis_flags_and_marks_matched():
    query = _load_query(
        "brasa/files/templates/b3/equities/b3-equities-symbol-spans-analysis.yaml"
    )
    changes = pd.DataFrame(
        [
            {"src_symbol": "KROT3", "dest_symbol": "COGN3"},
            {"src_symbol": "UNTA11", "dest_symbol": "UNTB11"},
        ]
    )
    conn = duckdb.connect(":memory:")
    conn.register(COTAHIST, _synthetic_cotahist())
    conn.register(CHANGES, changes)
    try:
        df = conn.execute(query).fetch_df()
    finally:
        conn.close()
    matched = {
        (r.symbol, r.event_type): bool(r.matched) for r in df.itertuples(index=False)
    }
    assert matched[("KROT3", "HARD_STOP")] is True
    assert matched[("UNTA11", "HARD_STOP")] is True
    assert matched[("DELS3", "HARD_STOP")] is False
    assert matched[("COGN3", "SUDDEN_START")] is True
    assert matched[("UNTB11", "SUDDEN_START")] is True
    assert matched[("HUGE3", "SUDDEN_START")] is False
    assert "ANCR3" not in df["symbol"].values


def test_returns_symbols_changes_resolves_chains():
    query = _load_query(
        "brasa/files/templates/b3/equities/b3-equities-returns-symbols-changes.yaml"
    )
    s = _sessions(6)
    changes = pd.DataFrame(
        [
            {"src_symbol": "AAAA3", "dest_symbol": "BBBB3"},
            {"src_symbol": "BBBB3", "dest_symbol": "CCCC3"},
        ]
    )
    returns = pd.DataFrame(
        [
            {
                "refdate": s[0],
                "symbol": "AAAA3",
                "pct_return": 0.01,
                "log_return": 0.00995,
            },
            {
                "refdate": s[1],
                "symbol": "AAAA3",
                "pct_return": 0.02,
                "log_return": 0.01980,
            },
            {
                "refdate": s[2],
                "symbol": "BBBB3",
                "pct_return": 0.03,
                "log_return": 0.02956,
            },
            {
                "refdate": s[3],
                "symbol": "CCCC3",
                "pct_return": 0.04,
                "log_return": 0.03922,
            },
        ]
    )
    conn = duckdb.connect(":memory:")
    conn.register(CHANGES, changes)
    conn.register(RETURNS, returns)
    try:
        df = conn.execute(query).fetch_df()
    finally:
        conn.close()
    # both hops relabel to the terminal ticker; CCCC3's own rows are NOT
    # duplicated here (they already exist in b3-equities-returns)
    assert set(df["symbol"]) == {"CCCC3"}
    assert len(df) == 3
    assert sorted(df["pct_return"]) == [0.01, 0.02, 0.03]
