---
goal: Refactor PandasAdapter conversion pipeline to minimize row-wise apply and use vectorized pandas conversions
version: 1.2
date_created: 2026-02-18
last_updated: 2026-02-21
owner: brasa-maintainers
status: Implemented
tags: [refactor, performance, pandas, fieldset, adapters]
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

This plan defines a deterministic refactor of the `PandasAdapter` conversion flow in `brasa/fieldsets/adapters/pandas_adapter.py` to preserve schema behavior while reducing Python-level row-wise conversion overhead for large datasets.

## 1. Requirements & Constraints

- **REQ-001**: Preserve public class and method names in `PandasAdapter` (`read_csv`, `apply_types`, `get_dtype_dict`, `get_converters`, `get_parse_dates`).
- **REQ-002**: Preserve current error-handling semantics for `errors` values (`raise`, `coerce`, `ignore`) as documented in class docstrings.
- **REQ-003**: Preserve current nullable dtype behavior controlled by `use_nullable_dtypes`.
- **REQ-004**: Remove row-wise `Series.apply(converter)` from standard date/datetime/numeric conversion paths when equivalent vectorized pandas logic exists.
- **REQ-005**: Support field parser parameters (`format`, `thousands`, `decimal`, `dec`, `sign`) using vectorized preprocessing and pandas conversion functions where possible.
- **PER-001**: Improve conversion throughput on large DataFrames by replacing Python callbacks with vectorized string and dtype operations.
- **CON-001**: Keep changes scoped to adapter behavior and related tests; do not alter template schema format or parser contracts outside adapter integration points.
- **CON-002**: Maintain compatibility with Python and dependency versions declared in `pyproject.toml`.
- **GUD-001**: Follow repository style: Ruff formatting, type hints, and existing exception style (`TypeParseError` wrapping when `errors='raise'`).
- **PAT-001**: Prefer vectorized pandas operations (`pd.to_datetime`, `pd.to_numeric`, `Series.str.replace`, `Series.astype`) over per-row Python functions.

## 2. Implementation Steps

### Implementation Phase 0

- **GOAL-000**: Evaluate readr-like column parser backends with deterministic benchmark and compatibility criteria before implementing adapter refactor.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-000 | Create benchmark harness in `tests/benchmarks/benchmark_field_parsers.py` comparing current row-wise `Field.parse` path vs vectorized backends on synthetic data with 1M+ rows. | ✓ | 2026-02-21 |
| TASK-001 | Define acceptance thresholds in benchmark harness: speedup target `>= 3x` vs current apply path, parse error parity `>= 99.9%`, and dtype parity for date/numeric/integer/boolean/string columns. | ✓ | 2026-02-21 |
| TASK-002 | Evaluate Backend A (Pandas-native vectorization): `pd.to_datetime(format=...)`, `Series.str.replace`, `pd.to_numeric`, `astype` for nullable dtypes; record unsupported parameter semantics. | ✓ | 2026-02-21 |
| TASK-003 | Evaluate Backend B (PyArrow CSV + compute kernels): map `Field` definitions to Arrow conversion options, then convert Arrow table to pandas preserving nullable semantics. | ✓ | 2026-02-21 |
| TASK-004 | Evaluate Backend C (Polars): map `Field` definitions to Polars schema and expressions; validate compatibility with existing pandas-first output contract by converting to pandas at adapter boundary. | ✓ (skipped — polars absent) | 2026-02-21 |
| TASK-005 | Produce deterministic decision artifact `plan/refactor-pandas-adapter-backend-decision-1.md` with measured metrics, compatibility matrix, and selected backend. | ✓ | 2026-02-21 |

### Implementation Phase 1

- **GOAL-001**: Separate conversion strategy decisions for `read_csv` vs `apply_types` and make vectorized eligibility explicit.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-006 | Add strategy helper methods in `brasa/fieldsets/adapters/pandas_adapter.py`: `_can_vectorize_date(field: Field) -> bool`, `_can_vectorize_numeric(field: Field) -> bool`, `_normalize_numeric_series(series: pd.Series, field: Field) -> pd.Series`. | ✓ | 2026-02-21 |
| TASK-007 | Refactor `_needs_converter(field)` to represent `read_csv` converter necessity only; introduce a separate decision path for `apply_types` to avoid using `_create_converter` for vectorizable fields. | ✓ | 2026-02-21 |
| TASK-008 | Document decision matrix in method docstrings with explicit conditions per type (`date`, `datetime`, `numeric`, `integer`, `boolean`, `string`, `character`). | ✓ | 2026-02-21 |

### Implementation Phase 2

- **GOAL-002**: Implement vectorized conversions for `apply_types` and remove row-wise `apply` for supported parameterized fields.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-009 | Update `_convert_date_type(series: pd.Series)` to accept optional `field: Field | None` and pass parser `format` to `pd.to_datetime(..., format=...)` when defined. | ✓ | 2026-02-21 |
| TASK-010 | Update `_convert_numeric_type(series: pd.Series)` to accept optional `field: Field | None`; apply vectorized normalization for `thousands` and `decimal` parameters before `pd.to_numeric`. | ✓ | 2026-02-21 |
| TASK-011 | In `apply_types`, route `date`/`datetime`/`numeric` fields through vectorized converters first; use `_convert_with_converter` only for non-vectorizable cases. | ✓ | 2026-02-21 |

### Implementation Phase 3

- **GOAL-003**: Keep `read_csv` behavior correct while minimizing converter callbacks and validating backwards compatibility.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-012 | Keep converter usage in `read_csv` for cases pandas cannot represent via `dtype`/`parse_dates` arguments (custom parser semantics or unsupported combinations). | ✓ | 2026-02-21 |
| TASK-013 | Add fast-path: avoid creating converter entries in `_build_mappings` for fields that can be handled by native `read_csv` options without semantic loss. | ✓ | 2026-02-21 |
| TASK-014 | Ensure warning behavior (`verbose_warnings`) remains unchanged for coercion and unexpected failures; preserve warning categories and message context. | ✓ | 2026-02-21 |

### Implementation Phase 4

- **GOAL-004**: Validate correctness, performance, and regression safety with automated tests.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-015 | Add/extend tests in `tests/` for parameterized `date` format conversion and parameterized numeric conversion (`thousands`, `decimal`, `dec`, `sign`) through `apply_types`. | ✓ | 2026-02-21 |
| TASK-016 | Add regression test asserting no row-wise `apply` path is used for vectorizable `date` and `numeric` fields in `apply_types`. | ✓ | 2026-02-21 |
| TASK-017 | Run validation commands: `uv run pytest`, `uv run ruff check .`, `uv run mypy brasa/`; record failures only if introduced by this refactor. | ✓ (136 passed, 0 new failures) | 2026-02-21 |

## 3. Alternatives

- **ALT-001**: Keep `_needs_converter` and row-wise `apply` unchanged; rejected due to avoidable performance bottlenecks on large datasets.
- **ALT-002**: Remove all custom converters and rely exclusively on `pd.read_csv`; rejected because field parser semantics and error behavior would diverge.
- **ALT-003**: Perform all conversions in parser layer before DataFrame creation; rejected because it increases parser complexity and duplicates adapter responsibilities.
- **ALT-004**: Introduce Arrow-first parsing backend (`pyarrow.csv`) as primary engine and convert to pandas only at API boundary; candidate when benchmark speedup and semantic parity criteria are met.
- **ALT-005**: Introduce Polars expression engine for type coercion and normalization, then convert to pandas; candidate when transformation complexity grows beyond pandas-native vectorization.
- **ALT-006**: Keep current parser as canonical semantic layer but compile parser metadata into backend-specific vectorized plans (hybrid model); preferred for incremental migration and low risk.
- **ALT-007**: Use `pandera`/`pydantic` as runtime parsing engine; rejected as primary parser backend because they are validation-centric and not designed as high-throughput CSV column parsers.

## 4. Dependencies

- **DEP-001**: `pandas` conversion APIs (`pd.to_datetime`, `pd.to_numeric`, nullable dtypes).
- **DEP-002**: Existing field parser metadata from `Field.parser.parameters` (`format`, `thousands`, `decimal`, `dec`, `sign`).
- **DEP-003**: Existing exception type `TypeParseError` in `brasa/fieldsets/exceptions.py`.
- **DEP-004**: Optional benchmark-time backend `pyarrow` (already present in project dependencies).
- **DEP-005**: Optional evaluation dependency `polars` (benchmark only; do not promote to runtime dependency without decision artifact approval).

## 5. Files

- **FILE-001**: `brasa/fieldsets/adapters/pandas_adapter.py` — conversion strategy refactor and vectorized paths.
- **FILE-002**: `tests/test_fieldsets_pandas_adapter.py` — new and updated adapter conversion tests.
- **FILE-003**: `docs/` (optional targeted update if behavior documentation changes) — adapter conversion behavior notes.
- **FILE-004**: `tests/benchmarks/benchmark_field_parsers.py` — backend benchmark and compatibility harness.
- **FILE-005**: `plan/refactor-pandas-adapter-backend-decision-1.md` — deterministic backend selection output.

## 6. Testing

- **TEST-001**: Date conversion with parser `format` in `apply_types` produces expected datetime values and NaT handling for invalid inputs.
- **TEST-002**: Numeric conversion with `thousands` and `decimal` parameters converts locale-formatted strings correctly.
- **TEST-003**: Numeric conversion with `dec` and `sign` parameters matches current parser semantics or documents explicit normalization rules.
- **TEST-004**: Error mode matrix (`raise`, `coerce`, `ignore`) remains behaviorally consistent before vs after refactor.
- **TEST-005**: Performance sanity check on large synthetic DataFrame confirms vectorized path is materially faster than row-wise converter path.
- **TEST-006**: Backend parity benchmark compares Pandas-native, Arrow-first, and optional Polars backends using fixed seed datasets and records deterministic metrics.
- **TEST-007**: Decision gate enforces backend selection only when performance and semantic thresholds defined in `TASK-001` are satisfied.

## 7. Risks & Assumptions

- **RISK-001**: Subtle semantic differences may exist between `Field.parse` and pandas native conversion for edge values.
- **RISK-002**: Locale/format edge cases could regress if normalization rules are incomplete.
- **RISK-003**: `read_csv` and `apply_types` behavior might diverge if strategy selection is not explicit and tested.
- **RISK-004**: Additional backend dependencies may increase maintenance and packaging complexity.
- **RISK-005**: Cross-backend null semantics (`pd.NA`, `NaN`, `NaT`) may produce silent behavior drift.
- **ASSUMPTION-001**: Most large-dataset performance cost is attributable to Python-level converter callbacks.
- **ASSUMPTION-002**: Field parser parameters can be mapped deterministically to vectorized pandas transformations for most practical cases.
- **ASSUMPTION-003**: A hybrid model (canonical parser semantics + vectorized execution plans) can preserve compatibility while improving throughput.

## 8. Related Specifications / Further Reading

- [docs/TEMPLATES.md](../docs/TEMPLATES.md)
- [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)
- [pyproject.toml](../pyproject.toml)
