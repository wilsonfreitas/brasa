"""WIL-12 — validate ticker-change detection over B3 cotahist.

Builds the two detection queries (symbol changes + hard-stop/sudden-start
analysis), runs them against staging.b3-cotahist in an in-memory DuckDB
(mirroring the sql_query ETL step), writes both tables to CSV, and scores the
detected changes against the known renames in
templates/brasa/brasa-returns-symbols-changes.yaml.

Usage:
    uv run python scripts/wil12_symbol_changes.py

Tunable knobs: PRICE_TOL, RECENCY (see constants below). Read-only.
"""

from pathlib import Path

import duckdb
import yaml

from brasa.queries import get_dataset

# --- tunable parameters -------------------------------------------------
PRICE_TOL = 0.10  # max |B.first_close - A.last_close| / A.last_close
RECENCY = 5  # sessions from the universe edge to count as stop/start
TABLE = "staging.b3-cotahist"
OUT_DIR = Path("scripts/out")


def changes_sql() -> str:
    return f"""
    WITH universe AS (
        SELECT refdate, symbol, close, isin,
               substr(isin, 7, 3) AS isin_class,
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
    ),
    candidate_pairs AS (
        SELECT a.symbol AS src_symbol,
               b.symbol AS dest_symbol,
               b.first_date AS change_date,
               a.last_close AS src_last_close,
               b.first_close AS dest_first_close,
               abs(b.first_close - a.last_close) / a.last_close AS price_diff_pct,
               a.isin_class AS isin_class,
               a.isin AS src_isin,
               b.isin AS dest_isin,
               a.corporation_name AS corporation_name_src,
               b.corporation_name AS corporation_name_dest
        FROM flagged a
        JOIN flagged b
          ON b.first_rn = a.last_rn + 1
         AND a.isin_class = b.isin_class
        WHERE a.is_hard_stop AND b.is_sudden_start
          AND a.last_close > 0
          AND abs(b.first_close - a.last_close) / a.last_close <= {PRICE_TOL}
    ),
    ranked AS (
        -- prefer pairs that keep the symbol suffix (share-class digits), then
        -- smallest price gap; avoids cross-pairing e.g. FJTA4->TASA3
        SELECT *,
               row_number() OVER (
                   PARTITION BY src_symbol
                   ORDER BY (substr(src_symbol, 5) = substr(dest_symbol, 5)) DESC,
                            price_diff_pct
               ) AS src_rk,
               row_number() OVER (
                   PARTITION BY dest_symbol
                   ORDER BY (substr(src_symbol, 5) = substr(dest_symbol, 5)) DESC,
                            price_diff_pct
               ) AS dest_rk
        FROM candidate_pairs
    )
    SELECT src_symbol, dest_symbol, change_date,
           src_last_close, dest_first_close, price_diff_pct,
           isin_class, src_isin, dest_isin,
           corporation_name_src, corporation_name_dest
    FROM ranked
    WHERE src_rk = 1 AND dest_rk = 1
    ORDER BY change_date, src_symbol
    """


def spans_sql() -> str:
    return f"""
    WITH universe AS (
        SELECT refdate, symbol, close, isin,
               substr(isin, 7, 3) AS isin_class,
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
    ),
    candidate_pairs AS (
        SELECT a.symbol AS src_symbol, b.symbol AS dest_symbol,
               abs(b.first_close - a.last_close) / a.last_close AS price_diff_pct
        FROM flagged a
        JOIN flagged b
          ON b.first_rn = a.last_rn + 1
         AND a.isin_class = b.isin_class
        WHERE a.is_hard_stop AND b.is_sudden_start
          AND a.last_close > 0
          AND abs(b.first_close - a.last_close) / a.last_close <= {PRICE_TOL}
    ),
    ranked AS (
        -- prefer pairs that keep the symbol suffix (share-class digits), then
        -- smallest price gap; avoids cross-pairing e.g. FJTA4->TASA3
        SELECT *,
               row_number() OVER (
                   PARTITION BY src_symbol
                   ORDER BY (substr(src_symbol, 5) = substr(dest_symbol, 5)) DESC,
                            price_diff_pct
               ) AS src_rk,
               row_number() OVER (
                   PARTITION BY dest_symbol
                   ORDER BY (substr(src_symbol, 5) = substr(dest_symbol, 5)) DESC,
                            price_diff_pct
               ) AS dest_rk
        FROM candidate_pairs
    ),
    changes AS (
        SELECT src_symbol, dest_symbol FROM ranked WHERE src_rk = 1 AND dest_rk = 1
    )
    SELECT symbol, 'HARD_STOP' AS event_type, last_date AS event_date,
           last_close AS close, isin, isin_class, corporation_name,
           symbol IN (SELECT src_symbol FROM changes) AS matched
    FROM flagged WHERE is_hard_stop
    UNION ALL
    SELECT symbol, 'SUDDEN_START' AS event_type, first_date AS event_date,
           first_close AS close, isin, isin_class, corporation_name,
           symbol IN (SELECT dest_symbol FROM changes) AS matched
    FROM flagged WHERE is_sudden_start
    ORDER BY event_date, symbol
    """


def known_pairs() -> set[tuple[str, str]]:
    path = Path("templates/brasa/brasa-returns-symbols-changes.yaml")
    doc = yaml.safe_load(path.read_text())
    return {(row["src"], row["dest"]) for row in doc["etl"]["symbols"]}


def main() -> None:
    dataset = get_dataset(
        "b3-cotahist",
        layer="staging",
        use_template_schema=False,
        use_catalog_schema=True,
    )
    conn = duckdb.connect(":memory:")
    conn.register(TABLE, dataset)
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
    print(f"CSVs written to {OUT_DIR}/")


if __name__ == "__main__":
    main()
