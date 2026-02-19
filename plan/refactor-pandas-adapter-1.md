---
goal: Refactor PandasAdapter conversion pipeline to minimize row-wise apply and use vectorized pandas conversions
version: 1.0
date_created: 2026-02-18
last_updated: 2026-02-18
owner: brasa-maintainers
status: Planned
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

### Implementation Phase 1

- **GOAL-001**: Separate conversion strategy decisions for `read_csv` vs `apply_types` and make vectorized eligibility explicit.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-001 | Add strategy helper methods in `brasa/fieldsets/adapters/pandas_adapter.py`: `_can_vectorize_date(field: Field) -> bool`, `_can_vectorize_numeric(field: Field) -> bool`, `_normalize_numeric_series(series: pd.Series, field: Field) -> pd.Series`. |  |  |
| TASK-002 | Refactor `_needs_converter(field)` to represent `read_csv` converter necessity only; introduce a separate decision path for `apply_types` to avoid using `_create_converter` for vectorizable fields. |  |  |
| TASK-003 | Document decision matrix in method docstrings with explicit conditions per type (`date`, `datetime`, `numeric`, `integer`, `boolean`, `string`, `character`). |  |  |

### Implementation Phase 2

- **GOAL-002**: Implement vectorized conversions for `apply_types` and remove row-wise `apply` for supported parameterized fields.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-004 | Update `_convert_date_type(series: pd.Series)` to accept optional `field: Field | None` and pass parser `format` to `pd.to_datetime(..., format=...)` when defined. |  |  |
| TASK-005 | Update `_convert_numeric_type(series: pd.Series)` to accept optional `field: Field | None`; apply vectorized normalization for `thousands` and `decimal` parameters before `pd.to_numeric`. |  |  |
| TASK-006 | In `apply_types`, route `date`/`datetime`/`numeric` fields through vectorized converters first; use `_convert_with_converter` only for non-vectorizable cases. |  |  |

### Implementation Phase 3

- **GOAL-003**: Keep `read_csv` behavior correct while minimizing converter callbacks and validating backwards compatibility.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-007 | Keep converter usage in `read_csv` for cases pandas cannot represent via `dtype`/`parse_dates` arguments (custom parser semantics or unsupported combinations). |  |  |
| TASK-008 | Add fast-path: avoid creating converter entries in `_build_mappings` for fields that can be handled by native `read_csv` options without semantic loss. |  |  |
| TASK-009 | Ensure warning behavior (`verbose_warnings`) remains unchanged for coercion and unexpected failures; preserve warning categories and message context. |  |  |

### Implementation Phase 4

- **GOAL-004**: Validate correctness, performance, and regression safety with automated tests.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-010 | Add/extend tests in `tests/` for parameterized `date` format conversion and parameterized numeric conversion (`thousands`, `decimal`, `dec`, `sign`) through `apply_types`. |  |  |
| TASK-011 | Add regression test asserting no row-wise `apply` path is used for vectorizable `date` and `numeric` fields in `apply_types`. |  |  |
| TASK-012 | Run validation commands: `poetry run pytest`, `poetry run ruff check .`, `poetry run mypy brasa/`; record failures only if introduced by this refactor. |  |  |

## 3. Alternatives

- **ALT-001**: Keep `_needs_converter` and row-wise `apply` unchanged; rejected due to avoidable performance bottlenecks on large datasets.
- **ALT-002**: Remove all custom converters and rely exclusively on `pd.read_csv`; rejected because field parser semantics and error behavior would diverge.
- **ALT-003**: Perform all conversions in parser layer before DataFrame creation; rejected because it increases parser complexity and duplicates adapter responsibilities.

## 4. Dependencies

- **DEP-001**: `pandas` conversion APIs (`pd.to_datetime`, `pd.to_numeric`, nullable dtypes).
- **DEP-002**: Existing field parser metadata from `Field.parser.parameters` (`format`, `thousands`, `decimal`, `dec`, `sign`).
- **DEP-003**: Existing exception type `TypeParseError` in `brasa/fieldsets/exceptions.py`.

## 5. Files

- **FILE-001**: `brasa/fieldsets/adapters/pandas_adapter.py` — conversion strategy refactor and vectorized paths.
- **FILE-002**: `tests/test_fieldsets_pandas_adapter.py` — new and updated adapter conversion tests.
- **FILE-003**: `docs/` (optional targeted update if behavior documentation changes) — adapter conversion behavior notes.

## 6. Testing

- **TEST-001**: Date conversion with parser `format` in `apply_types` produces expected datetime values and NaT handling for invalid inputs.
- **TEST-002**: Numeric conversion with `thousands` and `decimal` parameters converts locale-formatted strings correctly.
- **TEST-003**: Numeric conversion with `dec` and `sign` parameters matches current parser semantics or documents explicit normalization rules.
- **TEST-004**: Error mode matrix (`raise`, `coerce`, `ignore`) remains behaviorally consistent before vs after refactor.
- **TEST-005**: Performance sanity check on large synthetic DataFrame confirms vectorized path is materially faster than row-wise converter path.

## 7. Risks & Assumptions

- **RISK-001**: Subtle semantic differences may exist between `Field.parse` and pandas native conversion for edge values.
- **RISK-002**: Locale/format edge cases could regress if normalization rules are incomplete.
- **RISK-003**: `read_csv` and `apply_types` behavior might diverge if strategy selection is not explicit and tested.
- **ASSUMPTION-001**: Most large-dataset performance cost is attributable to Python-level converter callbacks.
- **ASSUMPTION-002**: Field parser parameters can be mapped deterministically to vectorized pandas transformations for most practical cases.

## 8. Related Specifications / Further Reading

- [docs/TEMPLATES.md](../docs/TEMPLATES.md)
- [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)
- [pyproject.toml](../pyproject.toml)
