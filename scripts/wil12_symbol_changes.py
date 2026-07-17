"""WIL-12 — validate ticker-change detection over B3 cotahist.

Builds the two detection queries (symbol changes + hard-stop/sudden-start
analysis), runs them against staging.b3-cotahist in an in-memory DuckDB
(mirroring the sql_query ETL step), writes both tables to CSV, and scores the
detected changes against the known renames in
templates/brasa/brasa-returns-symbols-changes.yaml.

Each candidate is also corroborated against staging.b3-equities-returns, the
adjusted daily-return series. For a genuine rename the dest symbol's
`pct_return` on the change date equals the signed close-to-close change from the
src's last close to the dest's first close (verified on KROT3->COGN3: +2.60% in
both). This adjusted-return continuity is a far stronger signal than the raw
close ratio — a coincidental hard-stop/sudden-start adjacency of two unrelated
tickers has no prior-day link, so its first-day return is absent or unrelated.
The returns series starts 2016-02-01, so pre-2016 changes get a NULL cross-check.

Usage:
    uv run python scripts/wil12_symbol_changes.py

Pairing also enforces share-class continuity derived from the ISIN: ON->ON and
PN->PN always continue, a PN line may be discontinued and reappear as a new ON
(PN->ON), but an ON never becomes a PN (blocks e.g. ELET3 -> AXIA6).

Tunable knobs: PRICE_TOL, RECENCY, RET_MATCH_TOL (see constants below).
Read-only.
"""

from pathlib import Path

import duckdb
import yaml

from brasa.queries import get_dataset

# --- tunable parameters -------------------------------------------------
PRICE_TOL = 0.10  # max |B.first_close - A.last_close| / A.last_close
RECENCY = 5  # sessions from the universe edge to count as stop/start
RET_MATCH_TOL = 0.005  # max |dest_pct_return - close_change_pct| to corroborate
TABLE = "staging.b3-cotahist"
TABLE_RETURNS = "staging.b3-equities-returns"
OUT_DIR = Path("scripts/out")


def _share_class_expr(isin: str = "isin") -> str:
    """SQL deriving the share class from the ISIN (authoritative, not the
    ticker digit): 'OR' security token -> ON (ordinárias), any 'P*' token
    (PA/PB/.../PR) -> PN (preferenciais), otherwise the ISIN security-class
    token (units/BDRs keep UNT/CDA and only ever pair within their own class).
    """
    return (
        f"CASE WHEN substr({isin}, 10, 2) = 'OR' THEN 'ON' "
        f"WHEN substr({isin}, 10, 1) = 'P' THEN 'PN' "
        f"ELSE substr({isin}, 7, 3) END"
    )


# Share-class continuity (a = hard-stop, b = sudden-start): ON->ON and PN->PN
# always continue; a PN line may be discontinued and reappear as a new ON
# (PN->ON); an ON never becomes a PN (blocks e.g. ELET3 -> AXIA6).
CLASS_COMPAT = (
    "(a.share_class = b.share_class OR (a.share_class = 'PN' AND b.share_class = 'ON'))"
)

# Ranking tie-break: prefer same share class (ON->ON / PN->PN) over the PN->ON
# conversion, then the exact symbol suffix, then the smallest price gap.
_RANK_ORDER = (
    "(src_share_class = dest_share_class) DESC,"
    " (substr(src_symbol, 5) = substr(dest_symbol, 5)) DESC,"
    " price_diff_pct"
)


def _base_ctes() -> str:
    """Shared CTE prefix (universe -> flagged) for both detection queries.

    Carries `isin_class` (ISIN token 7-9: ACN/UNT/CDA) and the finer
    `share_class` (ON/PN/…) used for the share-class continuity rule.
    """
    return f"""
    universe AS (
        SELECT refdate, symbol, close, isin,
               substr(isin, 7, 3) AS isin_class,
               {_share_class_expr("isin")} AS share_class,
               corporation_name
        FROM '{TABLE}'
        WHERE instrument_market = 10
          AND substr(isin, 7, 3) IN ('ACN', 'UNT', 'CDA')
          AND close IS NOT NULL
    ),
    session_rank AS (
        SELECT refdate, row_number() OVER (ORDER BY refdate) AS rn
        FROM (SELECT DISTINCT refdate FROM universe)
    ),
    bounds AS (SELECT min(rn) AS min_rn, max(rn) AS max_rn FROM session_rank),
    spans AS (
        SELECT symbol,
               min(refdate) AS first_date,
               max(refdate) AS last_date,
               arg_min(close, refdate) AS first_close,
               arg_max(close, refdate) AS last_close,
               arg_max(isin, refdate) AS isin,
               any_value(isin_class) AS isin_class,
               any_value(share_class) AS share_class,
               arg_max(corporation_name, refdate) AS corporation_name
        FROM universe GROUP BY symbol
    ),
    spans_ranked AS (
        SELECT s.*, fr.rn AS first_rn, lr.rn AS last_rn
        FROM spans s
        JOIN session_rank fr ON fr.refdate = s.first_date
        JOIN session_rank lr ON lr.refdate = s.last_date
    ),
    flagged AS (
        SELECT sr.*,
               sr.last_rn  < b.max_rn - {RECENCY} AS is_hard_stop,
               sr.first_rn > b.min_rn + {RECENCY} AS is_sudden_start
        FROM spans_ranked sr CROSS JOIN bounds b
    )"""


def changes_sql() -> str:
    return f"""
    WITH {_base_ctes()},
    candidate_pairs AS (
        SELECT a.symbol AS src_symbol,
               b.symbol AS dest_symbol,
               b.first_date AS change_date,
               a.last_close AS src_last_close,
               b.first_close AS dest_first_close,
               abs(b.first_close - a.last_close) / a.last_close AS price_diff_pct,
               a.isin_class AS isin_class,
               a.share_class AS src_share_class,
               b.share_class AS dest_share_class,
               a.isin AS src_isin,
               b.isin AS dest_isin,
               a.corporation_name AS corporation_name_src,
               b.corporation_name AS corporation_name_dest
        FROM flagged a
        JOIN flagged b
          ON b.first_rn = a.last_rn + 1
         AND a.isin_class = b.isin_class
         AND {CLASS_COMPAT}
        WHERE a.is_hard_stop AND b.is_sudden_start
          AND a.last_close > 0
          AND abs(b.first_close - a.last_close) / a.last_close <= {PRICE_TOL}
    ),
    ranked AS (
        SELECT *,
               row_number() OVER (
                   PARTITION BY src_symbol ORDER BY {_RANK_ORDER}
               ) AS src_rk,
               row_number() OVER (
                   PARTITION BY dest_symbol ORDER BY {_RANK_ORDER}
               ) AS dest_rk
        FROM candidate_pairs
    ),
    final AS (
        SELECT src_symbol, dest_symbol, change_date,
               src_last_close, dest_first_close, price_diff_pct,
               (dest_first_close - src_last_close) / src_last_close
                   AS close_change_pct,
               isin_class, src_share_class, dest_share_class,
               src_isin, dest_isin,
               corporation_name_src, corporation_name_dest
        FROM ranked
        WHERE src_rk = 1 AND dest_rk = 1
    )
    -- corroborate against the adjusted return series: the dest symbol's
    -- pct_return on the change date should equal the close-to-close change.
    SELECT f.*,
           r.pct_return AS dest_pct_return,
           CASE WHEN r.pct_return IS NULL THEN NULL
                ELSE abs(r.pct_return - f.close_change_pct) <= {RET_MATCH_TOL}
           END AS ret_match
    FROM final f
    LEFT JOIN '{TABLE_RETURNS}' r
      ON r.symbol = f.dest_symbol AND r.refdate = f.change_date
    ORDER BY f.change_date, f.src_symbol
    """


def spans_sql() -> str:
    return f"""
    WITH {_base_ctes()},
    candidate_pairs AS (
        SELECT a.symbol AS src_symbol, b.symbol AS dest_symbol,
               a.share_class AS src_share_class,
               b.share_class AS dest_share_class,
               abs(b.first_close - a.last_close) / a.last_close AS price_diff_pct
        FROM flagged a
        JOIN flagged b
          ON b.first_rn = a.last_rn + 1
         AND a.isin_class = b.isin_class
         AND {CLASS_COMPAT}
        WHERE a.is_hard_stop AND b.is_sudden_start
          AND a.last_close > 0
          AND abs(b.first_close - a.last_close) / a.last_close <= {PRICE_TOL}
    ),
    ranked AS (
        SELECT *,
               row_number() OVER (
                   PARTITION BY src_symbol ORDER BY {_RANK_ORDER}
               ) AS src_rk,
               row_number() OVER (
                   PARTITION BY dest_symbol ORDER BY {_RANK_ORDER}
               ) AS dest_rk
        FROM candidate_pairs
    ),
    changes AS (
        SELECT src_symbol, dest_symbol FROM ranked WHERE src_rk = 1 AND dest_rk = 1
    )
    -- event_pct_return: adjusted return on the event date (from the returns
    -- series). For a SUDDEN_START it is the first-day return that should match
    -- the close-to-close change if the start continues a renamed series.
    SELECT f.symbol, 'HARD_STOP' AS event_type, f.last_date AS event_date,
           f.last_close AS close, f.isin, f.isin_class, f.share_class,
           f.corporation_name,
           f.symbol IN (SELECT src_symbol FROM changes) AS matched,
           r.pct_return AS event_pct_return
    FROM flagged f
    LEFT JOIN '{TABLE_RETURNS}' r
      ON r.symbol = f.symbol AND r.refdate = f.last_date
    WHERE f.is_hard_stop
    UNION ALL
    SELECT f.symbol, 'SUDDEN_START' AS event_type, f.first_date AS event_date,
           f.first_close AS close, f.isin, f.isin_class, f.share_class,
           f.corporation_name,
           f.symbol IN (SELECT dest_symbol FROM changes) AS matched,
           r.pct_return AS event_pct_return
    FROM flagged f
    LEFT JOIN '{TABLE_RETURNS}' r
      ON r.symbol = f.symbol AND r.refdate = f.first_date
    WHERE f.is_sudden_start
    ORDER BY event_date, symbol
    """


def known_pairs() -> set[tuple[str, str]]:
    path = Path("brasa/files/templates/brasa/brasa-returns-symbols-changes.yaml")
    doc = yaml.safe_load(path.read_text())
    return {(row["src"], row["dest"]) for row in doc["etl"]["symbols"]}


def main() -> None:
    dataset = get_dataset(
        "b3-cotahist",
        layer="staging",
        use_template_schema=False,
        use_catalog_schema=True,
    )
    returns = get_dataset(
        "b3-equities-returns",
        layer="staging",
        use_template_schema=False,
        use_catalog_schema=True,
    )
    conn = duckdb.connect(":memory:")
    conn.register(TABLE, dataset)
    conn.register(TABLE_RETURNS, returns)
    changes = conn.execute(changes_sql()).fetch_df()
    spans = conn.execute(spans_sql()).fetch_df()
    conn.close()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    changes.to_csv(OUT_DIR / "symbol_changes.csv", index=False)
    spans.to_csv(OUT_DIR / "symbol_spans_analysis.csv", index=False)

    detected = set(zip(changes["src_symbol"], changes["dest_symbol"], strict=True))
    known = known_pairs()
    recovered = known & detected
    print(f"detected changes: {len(detected)}")
    print(f"known renames: {len(known)}")
    print(f"recovered: {len(recovered)}/{len(known)}")
    print(f"missing: {sorted(known - detected)}")
    print(f"extra (not in known list): {sorted(detected - known)}")

    # adjusted-return corroboration (staging.b3-equities-returns)
    covered = changes["dest_pct_return"].notna()
    confirmed = changes["ret_match"].fillna(False).astype(bool)
    print(
        f"returns cross-check: {int(confirmed.sum())} confirmed / "
        f"{int(covered.sum())} with returns coverage / {len(changes)} total "
        f"(pre-2016 changes have no returns to check)"
    )
    contradicted = changes[covered & ~confirmed]
    if not contradicted.empty:
        pairs = list(
            zip(
                contradicted["src_symbol"],
                contradicted["dest_symbol"],
                strict=True,
            )
        )
        print(f"returns-contradicted pairs (inspect): {pairs}")
    print(f"CSVs written to {OUT_DIR}/")


if __name__ == "__main__":
    main()
