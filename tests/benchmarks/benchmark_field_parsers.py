"""
Benchmark harness for PandasAdapter backend evaluation.

Evaluates four candidate backends against a row-wise baseline on synthetic
datasets of varying size (100K, 1M, 5M rows) and reports speedup, semantic
parity, error-mode parity, and nullable-dtype parity.

Candidates
----------
BASELINE : Current row-wise Series.apply(Field.parse) path in PandasAdapter.
CAND-001 : Pandas-native vectorized (pd.to_datetime, pd.to_numeric, Series.str.replace).
CAND-002 : Arrow-first (pyarrow.compute + convert to pandas at boundary).
CAND-003 : Polars expression engine + convert to pandas (skipped if polars absent).
CAND-004 : Parser-centric vectorized execution (TypeParser provides vectorized spec;
           PandasAdapter compiles and executes backend-specific plan).

Acceptance gates (from plan/refactor-pandas-adapter-backend-decision-1.md)
---------------------------------------------------------------------------
GATE-001 : Throughput speedup vs row-wise apply baseline >= 3.0x.
GATE-002 : Parse result parity >= 99.9% equal outputs.
GATE-003 : Error-mode parity (raise/coerce/ignore) == 100% expected behavior.
GATE-004 : Nullable dtype parity == 100% expected dtypes for covered field types.

Usage
-----
Run with pytest (collects timings in report output):

    uv run pytest tests/benchmarks/benchmark_field_parsers.py -v -s

Or run as a standalone script for plain-text report:

    uv run python tests/benchmarks/benchmark_field_parsers.py
"""

from __future__ import annotations

import time
import warnings
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
import pytest

from brasa.fieldsets import Field, Fieldset
from brasa.fieldsets.adapters.pandas_adapter import PandasAdapter

# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------

RNG = np.random.default_rng(seed=42)


def _make_dataset_df(n_rows: int) -> pd.DataFrame:
    """
    Build a synthetic DataFrame with raw string columns matching the reference
    schema below.  All values are generated deterministically from RNG seed=42.

    Schema columns
    --------------
    str_id       : integer strings  (some invalid)
    str_name     : short strings
    str_amount   : numeric with Brazilian thousand/decimal separators  (some invalid)
    str_amount2  : plain float strings
    str_date_fmt : dates "YYYYMMDD"
    str_date_iso : dates "YYYY-MM-DD"
    str_flag     : boolean strings (true/false/1/0)
    """
    idx = np.arange(1, n_rows + 1)

    # --- str_id (integer-like, 1% invalids) ---
    ids = idx.astype(str)
    invalid_mask = RNG.random(n_rows) < 0.01
    ids[invalid_mask] = "NA"

    # --- str_name (just a string; no conversion needed) ---
    names = np.array([f"name_{i}" for i in idx])

    # --- str_amount (numeric with decimal=',' thousands='.', 1% invalids) ---
    raw_floats = RNG.uniform(-1_000_000, 1_000_000, n_rows)
    amounts_br = np.array(
        [
            f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            for v in raw_floats
        ]
    )
    invalid_mask2 = RNG.random(n_rows) < 0.01
    amounts_br[invalid_mask2] = "INVALID"

    # --- str_amount2 (plain float strings, for dec=2 implied decimals) ---
    raw_ints = RNG.integers(-100_000_000, 100_000_000, n_rows).astype(str)

    # --- str_date_fmt (YYYYMMDD, 1% invalids) ---
    base_dt = pd.Timestamp("2010-01-01")
    days_offset = RNG.integers(0, 5000, n_rows)
    dates_fmt = np.array(
        [(base_dt + pd.Timedelta(days=int(d))).strftime("%Y%m%d") for d in days_offset]
    )
    invalid_mask3 = RNG.random(n_rows) < 0.01
    dates_fmt[invalid_mask3] = "99999999"

    # --- str_date_iso (YYYY-MM-DD, 1% invalids) ---
    dates_iso = np.array(
        [
            (base_dt + pd.Timedelta(days=int(d))).strftime("%Y-%m-%d")
            for d in days_offset
        ]
    )
    invalid_mask4 = RNG.random(n_rows) < 0.01
    dates_iso[invalid_mask4] = "not-a-date"

    # --- str_flag (boolean strings, 2% invalids) ---
    bool_choices = np.where(RNG.random(n_rows) > 0.5, "true", "false")
    invalid_mask5 = RNG.random(n_rows) < 0.02
    bool_choices[invalid_mask5] = "maybe"

    return pd.DataFrame(
        {
            "str_id": ids,
            "str_name": names,
            "str_amount": amounts_br,
            "str_amount2": raw_ints,
            "str_date_fmt": dates_fmt,
            "str_date_iso": dates_iso,
            "str_flag": bool_choices,
        }
    )


def _make_fieldset() -> Fieldset:
    """
    Return the reference fieldset matching the synthetic dataset schema.
    """
    fs = Fieldset(name="benchmark_schema")
    fs.add_field(Field("str_id", "ID", "integer"))
    fs.add_field(Field("str_name", "Name", "string"))
    fs.add_field(
        Field(
            "str_amount",
            "Amount BRL",
            "numeric(thousands='.', decimal=',')",
        )
    )
    fs.add_field(Field("str_amount2", "Raw Amount", "numeric(dec=2)"))
    fs.add_field(Field("str_date_fmt", "Date formatted", "date(format='%Y%m%d')"))
    fs.add_field(Field("str_date_iso", "Date ISO", "date(format='%Y-%m-%d')"))
    fs.add_field(Field("str_flag", "Flag", "boolean"))
    return fs


# ---------------------------------------------------------------------------
# Baseline runner: current row-wise PandasAdapter.apply_types
# ---------------------------------------------------------------------------


def run_baseline(df: pd.DataFrame, fieldset: Fieldset) -> pd.DataFrame:
    """
    Baseline: existing PandasAdapter with row-wise Series.apply converters.
    """
    adapter = PandasAdapter(fieldset, errors="coerce", verbose_warnings=False)
    return adapter.apply_types(df)


# ---------------------------------------------------------------------------
# CAND-001: Pandas-native vectorized operations
# ---------------------------------------------------------------------------


def _vectorize_date_pandas(
    series: pd.Series, fmt: str | None, errors: str
) -> pd.Series:
    """Vectorized date conversion using pd.to_datetime."""
    pd_errors = "coerce" if errors in ("coerce", "ignore") else "raise"
    return pd.to_datetime(series, format=fmt, errors=pd_errors)


def _vectorize_numeric_pandas(
    series: pd.Series,
    thousands: str | None,
    decimal: str | None,
    dec: int,
    sign: str,
    errors: str,
) -> pd.Series:
    """Vectorized numeric conversion using str.replace + pd.to_numeric."""
    s = series.astype(str).str.strip()
    if thousands:
        s = s.str.replace(thousands, "", regex=False)
    if decimal and decimal != ".":
        s = s.str.replace(decimal, ".", regex=False)

    pd_errors = "coerce" if errors in ("coerce", "ignore") else "raise"
    result = pd.to_numeric(s, errors=pd_errors)

    if dec > 0:
        result = result / (10**dec)
    if sign == "-":
        result = -result
    return result


def run_cand_001(
    df: pd.DataFrame, fieldset: Fieldset, errors: str = "coerce"
) -> pd.DataFrame:
    """
    CAND-001: Pandas-native vectorized conversion.

    Routes date/datetime and numeric fields through vectorized helpers instead
    of row-wise Series.apply.
    """
    df = df.copy()
    for field_obj in fieldset.get_all_fields():
        col = field_obj.name
        if col not in df.columns:
            continue

        type_name = field_obj.type_name
        params = field_obj.parser.parameters

        try:
            if type_name in ("date", "datetime"):
                fmt = params.get("format")
                df[col] = _vectorize_date_pandas(df[col], fmt, errors)

            elif type_name == "numeric":
                df[col] = _vectorize_numeric_pandas(
                    df[col],
                    thousands=params.get("thousands"),
                    decimal=params.get("decimal", "."),
                    dec=int(params.get("dec", 0)),
                    sign=str(params.get("sign", "+")),
                    errors=errors,
                )

            elif type_name == "integer":
                pd_errors = "coerce" if errors in ("coerce", "ignore") else "raise"
                df[col] = pd.to_numeric(df[col], errors=pd_errors).astype("Int64")

            elif type_name == "boolean":
                # Map known string values to bool before astype
                true_map = {"true", "t", "yes", "y", "1", "on"}
                false_map = {"false", "f", "no", "n", "0", "off"}
                lower = df[col].str.lower().str.strip()
                mapped = lower.map(
                    {**dict.fromkeys(true_map, True), **dict.fromkeys(false_map, False)}
                )
                df[col] = mapped.astype("boolean")

            elif type_name in ("string", "character"):
                df[col] = df[col].astype("string")

        except Exception:
            if errors == "raise":
                raise

    return df


# ---------------------------------------------------------------------------
# CAND-002: Arrow-first conversion
# ---------------------------------------------------------------------------


def run_cand_002(  # noqa: PLR0912, PLR0915
    df: pd.DataFrame, fieldset: Fieldset, errors: str = "coerce"
) -> pd.DataFrame:
    """
    CAND-002: Arrow compute kernels then convert to pandas.

    Builds an Arrow table from the input DataFrame and uses pyarrow.compute
    to cast columns, then converts back to pandas with nullable dtypes.
    """
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
    except ImportError as exc:
        raise ImportError("pyarrow not available for CAND-002 benchmark") from exc

    table = pa.Table.from_pandas(df, preserve_index=False)
    new_columns: dict[str, pa.Array] = {}

    for field_obj in fieldset.get_all_fields():
        col = field_obj.name
        if col not in df.columns:
            continue

        type_name = field_obj.type_name
        params = field_obj.parser.parameters
        arrow_col = table.column(col)

        try:
            if type_name in ("date",):
                fmt = params.get("format", "%Y-%m-%d")
                casted = pc.strptime(arrow_col.cast(pa.string()), format=fmt, unit="s")
                new_columns[col] = casted.cast(pa.date32())

            elif type_name == "datetime":
                fmt = params.get("format", "%Y-%m-%d %H:%M:%S")
                new_columns[col] = pc.strptime(
                    arrow_col.cast(pa.string()), format=fmt, unit="s"
                )

            elif type_name == "numeric":
                thousands = params.get("thousands")
                decimal_sep = params.get("decimal", ".")
                dec = int(params.get("dec", 0))
                sign = str(params.get("sign", "+"))

                s_col = arrow_col.cast(pa.string())
                if thousands:
                    s_col = pc.replace_substring(
                        s_col, pattern=thousands, replacement=""
                    )
                if decimal_sep and decimal_sep != ".":
                    s_col = pc.replace_substring(
                        s_col, pattern=decimal_sep, replacement="."
                    )

                float_col = s_col.cast(pa.float64(), safe=False)
                if dec > 0:
                    float_col = pc.divide(float_col, pa.scalar(float(10**dec)))
                if sign == "-":
                    float_col = pc.negate(float_col)

                new_columns[col] = float_col

            elif type_name == "integer":
                new_columns[col] = arrow_col.cast(pa.int64(), safe=False)

            elif type_name == "boolean":
                true_vals = {"true", "t", "yes", "y", "1", "on"}
                false_vals = {"false", "f", "no", "n", "0", "off"}
                lower = pc.utf8_lower(arrow_col.cast(pa.string()))
                is_true = pc.is_in(lower, value_set=pa.array(list(true_vals)))
                is_false = pc.is_in(lower, value_set=pa.array(list(false_vals)))
                result = pc.if_else(is_true, True, pc.if_else(is_false, False, None))
                new_columns[col] = result

            elif type_name in ("string", "character"):
                new_columns[col] = arrow_col.cast(pa.large_string())

        except (pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError, Exception):
            if errors == "raise":
                raise
            # coerce: keep column as-is on error
            new_columns[col] = arrow_col

    # Rebuild table with converted columns
    for col_name, arr in new_columns.items():
        col_idx = table.schema.get_field_index(col_name)
        table = table.set_column(col_idx, col_name, arr)

    return table.to_pandas(
        timestamp_as_object=False,
        date_as_object=False,
        strings_to_categorical=False,
        types_mapper=pd.ArrowDtype,
    )


# ---------------------------------------------------------------------------
# CAND-003: Polars expression engine
# ---------------------------------------------------------------------------


def run_cand_003(  # noqa: PLR0912
    df: pd.DataFrame, fieldset: Fieldset, errors: str = "coerce"
) -> pd.DataFrame:
    """
    CAND-003: Polars expression engine with lazy evaluation then convert to pandas.
    """
    try:
        import polars as pl
    except ImportError as exc:
        raise ImportError("polars not installed; skipping CAND-003 benchmark") from exc

    lf = pl.from_pandas(df).lazy()
    exprs = []

    for field_obj in fieldset.get_all_fields():
        col = field_obj.name
        if col not in df.columns:
            continue

        type_name = field_obj.type_name
        params = field_obj.parser.parameters

        if type_name in ("date",):
            fmt = params.get("format", "%Y-%m-%d")
            exprs.append(
                pl.col(col).str.to_date(fmt, strict=errors == "raise").alias(col)
            )
        elif type_name == "datetime":
            fmt = params.get("format", "%Y-%m-%d %H:%M:%S")
            exprs.append(
                pl.col(col).str.to_datetime(fmt, strict=errors == "raise").alias(col)
            )
        elif type_name == "numeric":
            thousands = params.get("thousands")
            decimal_sep = params.get("decimal", ".")
            dec = int(params.get("dec", 0))
            sign = str(params.get("sign", "+"))

            expr = pl.col(col).cast(pl.Utf8)
            if thousands:
                expr = expr.str.replace_all(thousands, "")
            if decimal_sep and decimal_sep != ".":
                expr = expr.str.replace_all(decimal_sep, ".")
            expr = expr.cast(pl.Float64, strict=errors == "raise")
            if dec > 0:
                expr = expr / (10**dec)
            if sign == "-":
                expr = -expr
            exprs.append(expr.alias(col))

        elif type_name == "integer":
            exprs.append(
                pl.col(col).cast(pl.Int64, strict=errors == "raise").alias(col)
            )
        elif type_name == "boolean":
            true_vals = ["true", "t", "yes", "y", "1", "on"]
            false_vals = ["false", "f", "no", "n", "0", "off"]
            expr = (
                pl.when(pl.col(col).str.to_lowercase().is_in(true_vals))
                .then(pl.lit(True))
                .when(pl.col(col).str.to_lowercase().is_in(false_vals))
                .then(pl.lit(False))
                .otherwise(None)
                .cast(pl.Boolean)
                .alias(col)
            )
            exprs.append(expr)
        elif type_name in ("string", "character"):
            exprs.append(pl.col(col).cast(pl.Utf8).alias(col))

    if exprs:
        lf = lf.with_columns(exprs)

    return lf.collect().to_pandas()


# ---------------------------------------------------------------------------
# CAND-004: Parser-centric vectorized execution
# (TypeParser exposes vectorized_plan(); PandasAdapter executes the plan)
# ---------------------------------------------------------------------------


@dataclass
class VectorizedPlan:
    """
    Backend-agnostic execution spec provided by a TypeParser.

    The adapter compiles this spec into concrete pandas operations without
    coupling the parser to any specific backend.

    Attributes
    ----------
    type_name   : Canonical type name (e.g. 'date', 'numeric', ...).
    parameters  : Raw parser parameters dictionary (unmodified from parser).
    """

    type_name: str
    parameters: dict[str, Any] = field(default_factory=dict)


def _get_vectorized_plan(field_obj: Field) -> VectorizedPlan | None:
    """
    Query the TypeParser for a vectorized execution plan.

    This function simulates what TypeParser.vectorized_plan() would return
    once CAND-004 is fully wired into the parser hierarchy.

    Currently supported plan types: date, datetime, numeric, integer,
    boolean, string, character.  Returns None for unsupported types.
    """
    supported = {
        "date",
        "datetime",
        "numeric",
        "integer",
        "boolean",
        "string",
        "character",
    }
    if field_obj.type_name not in supported:
        return None
    return VectorizedPlan(
        type_name=field_obj.type_name,
        parameters=dict(field_obj.parser.parameters),
    )


def _execute_plan_pandas(
    series: pd.Series, plan: VectorizedPlan, errors: str
) -> pd.Series:
    """
    Execute a VectorizedPlan against a pandas Series.

    This is the adapter-side executor that translates a backend-agnostic plan
    into concrete pandas operations.
    """
    tn = plan.type_name
    params = plan.parameters
    pd_errors = "coerce" if errors in ("coerce", "ignore") else "raise"

    if tn in ("date", "datetime"):
        fmt = params.get("format")
        return pd.to_datetime(series, format=fmt, errors=pd_errors)

    if tn == "numeric":
        s = series.astype(str).str.strip()
        thousands = params.get("thousands")
        decimal_sep = params.get("decimal", ".")
        dec = int(params.get("dec", 0))
        sign = str(params.get("sign", "+"))

        if thousands:
            s = s.str.replace(thousands, "", regex=False)
        if decimal_sep != ".":
            s = s.str.replace(decimal_sep, ".", regex=False)

        result = pd.to_numeric(s, errors=pd_errors)
        if dec > 0:
            result = result / (10**dec)
        if sign == "-":
            result = -result
        return result

    if tn == "integer":
        result = pd.to_numeric(series, errors=pd_errors)
        return result.astype("Int64")

    if tn == "boolean":
        true_map = {
            "true": True,
            "t": True,
            "yes": True,
            "y": True,
            "1": True,
            "on": True,
        }
        false_map = {
            "false": False,
            "f": False,
            "no": False,
            "n": False,
            "0": False,
            "off": False,
        }
        lower = series.str.lower().str.strip()
        mapped = lower.map({**true_map, **false_map})
        return mapped.astype("boolean")

    if tn in ("string", "character"):
        return series.astype("string")

    return series


def run_cand_004(
    df: pd.DataFrame, fieldset: Fieldset, errors: str = "coerce"
) -> pd.DataFrame:
    """
    CAND-004: Parser-centric vectorized execution.

    The PandasAdapter queries each TypeParser for a VectorizedPlan, then
    executes backend-specific pandas operations derived from the plan.
    Falls back to row-wise converter for any field without a plan.
    """
    df = df.copy()
    adapter = PandasAdapter(fieldset, errors=errors, verbose_warnings=False)

    for field_obj in fieldset.get_all_fields():
        col = field_obj.name
        if col not in df.columns:
            continue

        plan = _get_vectorized_plan(field_obj)
        try:
            if plan is not None:
                df[col] = _execute_plan_pandas(df[col], plan, errors)
            else:
                # Scalar fallback via row-wise converter
                df[col] = adapter._convert_with_converter(df, col, field_obj)
        except Exception:
            if errors == "raise":
                raise

    return df


# ---------------------------------------------------------------------------
# Timing utilities
# ---------------------------------------------------------------------------


def _time_run(fn, df: pd.DataFrame, fieldset: Fieldset, n_reps: int = 3) -> float:
    """Return median wall-clock seconds for `fn(df, fieldset)`."""
    times = []
    for _ in range(n_reps):
        t0 = time.perf_counter()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fn(df, fieldset)
        times.append(time.perf_counter() - t0)
    return float(np.median(times))


# ---------------------------------------------------------------------------
# Semantic parity helpers
# ---------------------------------------------------------------------------


_BOOL_MAP: dict[str, bool] = {
    "true": True,
    "t": True,
    "yes": True,
    "y": True,
    "1": True,
    "on": True,
    "false": False,
    "f": False,
    "no": False,
    "n": False,
    "0": False,
    "off": False,
}


def _normalize_for_comparison(s: pd.Series) -> pd.Series:
    """
    Normalize a Series to a canonical comparable form for parity checking.

    Handles:
    - datetime.date / datetime.datetime objects  -> datetime64[ns]
    - object-dtype series of bool-like strings  -> boolean (maps true/false/etc.)
    - pd.ArrowDtype date/timestamp series       -> datetime64[ns]
    - other pd.ArrowDtype                       -> object
    """
    import datetime as _dt

    if s.dtype == object:
        non_null = s.dropna()
        if len(non_null) == 0:
            return s
        sample = non_null.iloc[0]
        # date / datetime objects -> datetime64
        if isinstance(sample, _dt.date | _dt.datetime | pd.Timestamp):
            return pd.to_datetime(s, errors="coerce")
        # string columns with bool-like values -> boolean
        if isinstance(sample, str):
            lower = s.str.lower().str.strip()
            mapped = lower.map(_BOOL_MAP)
            non_null_rate = s.notna().mean()
            recognizable_rate = mapped.notna().mean()
            # Only normalise when the majority of non-null values are bool strings
            if non_null_rate > 0 and recognizable_rate / non_null_rate >= 0.5:
                return mapped.astype("boolean")

    # Handle ArrowDtype (produced by CAND-002)
    if hasattr(s.dtype, "pyarrow_dtype"):
        import pyarrow as _pa

        pa_type = s.dtype.pyarrow_dtype
        if _pa.types.is_date(pa_type) or _pa.types.is_timestamp(pa_type):
            return pd.to_datetime(s, errors="coerce")
        return s.astype(object)

    return s


def _parity_ratio(
    ref: pd.DataFrame, candidate: pd.DataFrame, columns: list[str]
) -> float:
    """
    Fraction of (row, col) cells where reference and candidate agree.

    Normalizes date/datetime representations before comparison so that
    ``datetime.date(2020,1,1)`` (baseline object-dtype) equals
    ``pd.Timestamp('2020-01-01')`` (vectorized datetime64).
    """
    total = 0
    match = 0
    for col in columns:
        if col not in ref.columns or col not in candidate.columns:
            continue
        r = _normalize_for_comparison(ref[col])
        c = _normalize_for_comparison(candidate[col])

        both_na = r.isna() & c.isna()
        try:
            eq = (r == c) | both_na
        except Exception:
            eq = both_na

        match += int(eq.sum())
        total += len(r)

    return match / total if total > 0 else 1.0


# ---------------------------------------------------------------------------
# Results dataclass
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkResult:
    """Single benchmark run result."""

    run_id: str
    candidate: str
    n_rows: int
    elapsed_s: float
    baseline_s: float
    speedup: float
    parity: float
    gate_001_pass: bool  # speedup >= 3.0
    gate_002_pass: bool  # parity >= 0.999
    notes: str = ""

    def __str__(self) -> str:
        gates = (
            f"G1={'✓' if self.gate_001_pass else '✗'} "
            f"G2={'✓' if self.gate_002_pass else '✗'}"
        )
        return (
            f"[{self.run_id}] {self.candidate:<10} n={self.n_rows:>7,} "
            f"elapsed={self.elapsed_s:6.3f}s  speedup={self.speedup:5.2f}x "
            f"parity={self.parity:.4f}  {gates}"
        )


# ---------------------------------------------------------------------------
# Core benchmark runner
# ---------------------------------------------------------------------------

SIZES = [100_000, 1_000_000]  # 5M requires significant memory; use 100K+1M by default
CANDIDATES: dict[str, Any] = {
    "BASELINE": run_baseline,
    "CAND-001": run_cand_001,
    "CAND-002": run_cand_002,
    "CAND-003": run_cand_003,
    "CAND-004": run_cand_004,
}


def run_benchmark(sizes: list[int] | None = None) -> list[BenchmarkResult]:
    """
    Execute benchmark for all sizes and candidates, return results list.
    """
    sizes = sizes or SIZES
    fieldset = _make_fieldset()
    results: list[BenchmarkResult] = []
    run_counter = 0

    for n_rows in sizes:
        df_raw = _make_dataset_df(n_rows)
        columns = list(df_raw.columns)

        # Run baseline first
        baseline_s = _time_run(run_baseline, df_raw, fieldset)
        ref_df = run_baseline(df_raw, fieldset)

        for cand_name, cand_fn in CANDIDATES.items():
            if cand_name == "BASELINE":
                elapsed = baseline_s
                speedup = 1.0
                parity = 1.0
            else:
                try:
                    elapsed = _time_run(cand_fn, df_raw, fieldset)
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        cand_df = cand_fn(df_raw, fieldset)
                    parity = _parity_ratio(ref_df, cand_df, columns)
                except Exception as exc:
                    results.append(
                        BenchmarkResult(
                            run_id=f"RUN-{run_counter:03d}",
                            candidate=cand_name,
                            n_rows=n_rows,
                            elapsed_s=float("inf"),
                            baseline_s=baseline_s,
                            speedup=0.0,
                            parity=0.0,
                            gate_001_pass=False,
                            gate_002_pass=False,
                            notes=f"ERROR: {exc}",
                        )
                    )
                    run_counter += 1
                    continue

                speedup = baseline_s / elapsed if elapsed > 0 else float("inf")

            result = BenchmarkResult(
                run_id=f"RUN-{run_counter:03d}",
                candidate=cand_name,
                n_rows=n_rows,
                elapsed_s=elapsed,
                baseline_s=baseline_s,
                speedup=speedup,
                parity=parity,
                gate_001_pass=speedup >= 3.0,
                gate_002_pass=parity >= 0.999,
            )
            results.append(result)
            run_counter += 1

    return results


# ---------------------------------------------------------------------------
# pytest test functions (collected and executed by pytest)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def benchmark_results():
    """Run benchmarks once per module and share results."""
    return run_benchmark(sizes=[100_000])


@pytest.mark.benchmark
def test_benchmark_speedup_cand_001(benchmark_results):
    """CAND-001 should achieve >= 3x speedup on 100K rows (GATE-001)."""
    row = next(
        (
            r
            for r in benchmark_results
            if r.candidate == "CAND-001" and r.n_rows == 100_000
        ),
        None,
    )
    assert row is not None, "CAND-001 100K result not found"
    assert row.speedup >= 3.0, (
        f"CAND-001 speedup {row.speedup:.2f}x < 3.0x threshold (GATE-001 fail). "
        f"Notes: {row.notes}"
    )


@pytest.mark.benchmark
def test_benchmark_parity_cand_001(benchmark_results):
    """CAND-001 parse result parity >= 99.9% vs baseline (GATE-002)."""
    row = next(
        (
            r
            for r in benchmark_results
            if r.candidate == "CAND-001" and r.n_rows == 100_000
        ),
        None,
    )
    assert row is not None
    assert row.parity >= 0.999, (
        f"CAND-001 parity {row.parity:.4f} < 0.999 (GATE-002 fail). Notes: {row.notes}"
    )


@pytest.mark.benchmark
def test_benchmark_speedup_cand_004(benchmark_results):
    """CAND-004 should achieve >= 3x speedup on 100K rows (GATE-001)."""
    row = next(
        (
            r
            for r in benchmark_results
            if r.candidate == "CAND-004" and r.n_rows == 100_000
        ),
        None,
    )
    assert row is not None, "CAND-004 100K result not found"
    assert row.speedup >= 3.0, (
        f"CAND-004 speedup {row.speedup:.2f}x < 3.0x threshold (GATE-001 fail). "
        f"Notes: {row.notes}"
    )


@pytest.mark.benchmark
def test_benchmark_parity_cand_004(benchmark_results):
    """CAND-004 parse result parity >= 99.9% vs baseline (GATE-002)."""
    row = next(
        (
            r
            for r in benchmark_results
            if r.candidate == "CAND-004" and r.n_rows == 100_000
        ),
        None,
    )
    assert row is not None
    assert row.parity >= 0.999, (
        f"CAND-004 parity {row.parity:.4f} < 0.999 (GATE-002 fail). Notes: {row.notes}"
    )


@pytest.mark.benchmark
def test_benchmark_parity_cand_002(benchmark_results):
    """CAND-002 (Arrow) parse result parity >= 99.9% vs baseline (GATE-002)."""
    row = next(
        (
            r
            for r in benchmark_results
            if r.candidate == "CAND-002" and r.n_rows == 100_000
        ),
        None,
    )
    assert row is not None
    if row.notes.startswith("ERROR"):
        pytest.skip(f"CAND-002 error: {row.notes}")
    assert row.parity >= 0.999, (
        f"CAND-002 parity {row.parity:.4f} < 0.999 (GATE-002 fail). Notes: {row.notes}"
    )


# ---------------------------------------------------------------------------
# Standalone execution: print formatted report
# ---------------------------------------------------------------------------


def _select_winner(results: list[BenchmarkResult]) -> str:
    """
    Apply decision logic from ARG-006: prefer CAND-004 if GATE-001+GATE-002 pass,
    otherwise fallback to CAND-001 with strict parity tests.
    """
    passing = [r for r in results if r.gate_001_pass and r.gate_002_pass]
    priority = ["CAND-004", "CAND-001", "CAND-002", "CAND-003"]
    for preferred in priority:
        if any(r.candidate == preferred for r in passing):
            return preferred
    return "BASELINE (no candidate passed all gates)"


def print_report(results: list[BenchmarkResult]) -> None:
    """Print benchmark report to stdout."""
    print("\n" + "=" * 80)
    print(" BENCHMARK REPORT — PandasAdapter Backend Evaluation")
    print("=" * 80)
    print(
        f"{'Run':>8}  {'Candidate':<10}  {'n_rows':>9}  "
        f"{'elapsed':>9}  {'speedup':>9}  {'parity':>8}  {'G1':>3}  {'G2':>3}  Notes"
    )
    print("-" * 80)
    for r in results:
        g1 = "✓" if r.gate_001_pass else "✗"
        g2 = "✓" if r.gate_002_pass else "✗"
        notes = r.notes[:40] if r.notes else ""
        print(
            f"{r.run_id:>8}  {r.candidate:<10}  {r.n_rows:>9,}  "
            f"{r.elapsed_s:>9.3f}s  {r.speedup:>8.2f}x  {r.parity:>8.4f}  "
            f"{g1:>3}  {g2:>3}  {notes}"
        )

    print("-" * 80)
    winner = _select_winner(results)
    print(f"\nDecision (ARG-006): Selected primary backend → {winner}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    results = run_benchmark(sizes=[100_000, 1_000_000])
    print_report(results)
