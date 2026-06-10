"""Unit tests for the WIL-12 symbol-change detection templates.

Each test loads the template's sql_query and runs it against a synthetic
in-memory cotahist registered exactly as the sql_query ETL step does.
"""

import datetime as dt
from pathlib import Path

import duckdb
import pandas as pd
import yaml

TABLE = "staging.b3-cotahist"


def _sessions(n: int) -> list[dt.date]:
    return [dt.date(2018, 1, 1) + dt.timedelta(days=i) for i in range(n)]


def _synthetic_cotahist() -> pd.DataFrame:
    """20 sessions. src symbols trade sessions 1..8, dest start at session 9.

    Expected matches: KROT3->COGN3 (ACN, ~10) and UNTA11->UNTB11 (UNT, ~20).
    Non-matches: XPTO3/YPTO3 (price jump), DELS3 (delisting, no successor),
    MISM3 (ACN at 20 but only adjacent same-price start is UNT class).
    ANCR3 spans all 20 sessions (anchors the bounds; never flagged).
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

    early = range(0, 8)  # sessions 1..8 (rn 1..8)
    late = range(8, 20)  # sessions 9..20 (rn 9..20)
    add("ANCR3", "BRANCRACNOR0", range(0, 20), 5.0)
    add("KROT3", "BRKROTACNOR2", early, 10.0)
    add("COGN3", "BRCOGNACNOR0", late, 10.1)
    add("UNTA11", "BRUNTAUNT0R5", early, 20.0)
    add("UNTB11", "BRUNTBUNT0R6", late, 20.2)
    add("XPTO3", "BRXPTOACNOR0", early, 30.0)
    add("YPTO3", "BRYPTOACNOR0", late, 90.0)
    add("DELS3", "BRDELSACNOR0", early, 200.0)
    add("MISM3", "BRMISMACNOR0", early, 20.0)
    return pd.DataFrame(rows)


def _run_template_query(template_path: str) -> pd.DataFrame:
    doc = yaml.safe_load(Path(template_path).read_text())
    query = doc["etl"]["pipeline"][0]["query"]
    conn = duckdb.connect(":memory:")
    conn.register(TABLE, _synthetic_cotahist())
    try:
        return conn.execute(query).fetch_df()
    finally:
        conn.close()


def test_symbol_changes_detects_renames_only():
    df = _run_template_query("templates/brasa/brasa-symbol-changes.yaml")
    pairs = set(zip(df["src_symbol"], df["dest_symbol"], strict=True))
    assert pairs == {("KROT3", "COGN3"), ("UNTA11", "UNTB11")}
    # ISIN class carried correctly
    by_src = df.set_index("src_symbol")["isin_class"].to_dict()
    assert by_src["KROT3"] == "ACN"
    assert by_src["UNTA11"] == "UNT"
    # price-jump, delisting, and class-mismatch are NOT emitted
    assert "XPTO3" not in df["src_symbol"].values
    assert "DELS3" not in df["src_symbol"].values
    assert "MISM3" not in df["src_symbol"].values


def test_symbol_spans_analysis_flags_and_marks_matched():
    df = _run_template_query("templates/brasa/brasa-symbol-spans-analysis.yaml")
    matched = {
        (r.symbol, r.event_type): bool(r.matched) for r in df.itertuples(index=False)
    }
    # hard-stops
    assert matched[("KROT3", "HARD_STOP")] is True
    assert matched[("UNTA11", "HARD_STOP")] is True
    assert matched[("XPTO3", "HARD_STOP")] is False
    assert matched[("DELS3", "HARD_STOP")] is False
    assert matched[("MISM3", "HARD_STOP")] is False
    # sudden-starts
    assert matched[("COGN3", "SUDDEN_START")] is True
    assert matched[("UNTB11", "SUDDEN_START")] is True
    assert matched[("YPTO3", "SUDDEN_START")] is False
    # the anchor symbol is neither a hard-stop nor a sudden-start
    assert "ANCR3" not in df["symbol"].values
