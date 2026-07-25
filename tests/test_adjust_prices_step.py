"""Tests for the adjust_prices_by_returns transform and step."""

import numpy as np
import pandas as pd

from brasa.engine.pipeline.steps.shared_transforms import adjust_prices_by_returns


def make_frame():
    """One symbol, 3 consecutive B3 business days, returns consistent with closes."""
    closes = [100.0, 110.0, 121.0]
    return pd.DataFrame(
        {
            "refdate": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "symbol": ["IDX1"] * 3,
            "open": [99.0, 109.0, 120.0],
            "high": [101.0, 111.0, 122.0],
            "low": [98.0, 108.0, 119.0],
            "close": closes,
            "returns": [np.nan, np.log(110 / 100), np.log(121 / 110)],
        }
    )


def test_consistent_returns_reproduce_raw_prices():
    # When returns equal the actual close-to-close log changes, the
    # back-adjustment is the identity: adjusted == raw for every column.
    out = adjust_prices_by_returns(make_frame()).sort_values("refdate")
    expected = make_frame().sort_values("refdate")
    for col in ["open", "high", "low", "close"]:
        np.testing.assert_allclose(out[col].to_numpy(), expected[col].to_numpy())


def test_anchor_is_latest_close():
    # With returns that disagree with raw prices, the LATEST close must stay
    # equal to the raw latest close (the series is back-adjusted, not forward).
    df = make_frame()
    df.loc[1, "returns"] = np.log(105 / 100)  # inconsistent middle return
    out = adjust_prices_by_returns(df).sort_values("refdate")
    assert out["close"].iloc[-1] == 121.0
    # And the earliest close is latest / exp(sum of later returns):
    expected_first = 121.0 / np.exp(np.log(121 / 110) + np.log(105 / 100))
    np.testing.assert_allclose(out["close"].iloc[0], expected_first)


def test_no_gap_fill_keeps_source_rows_only():
    df = make_frame().drop(index=1).reset_index(drop=True)  # drop 2024-01-03
    out = adjust_prices_by_returns(df, fill_calendar_gaps=False)
    assert len(out) == 2
    assert pd.Timestamp("2024-01-03") not in set(out["refdate"])


def test_gap_fill_emits_synthetic_row():
    df = make_frame().drop(index=1).reset_index(drop=True)  # drop 2024-01-03
    out = adjust_prices_by_returns(df, fill_calendar_gaps=True)
    assert len(out) == 3
    assert pd.Timestamp("2024-01-03") in set(out["refdate"])
