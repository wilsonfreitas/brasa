---
goal: Build brasa-industry-sectors lookup dataset from brasa-companies sector hierarchy with GICS, ICB, and normalized mappings
version: 1.0
date_created: 2026-02-20
last_updated: 2026-02-20
owner: brasa-team
status: 'Implemented'
tags: ['data', 'etl', 'templates', 'industry-classification', 'staging']
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

This plan defines the implementation of a new ETL dataset named `brasa-industry-sectors` in the `staging` layer. The dataset is a deterministic lookup table generated from `staging.brasa-companies` using `sector` and `subsector`, with mappings to standard taxonomies (GICS and ICB) and a custom normalized sector/subsector model based on the mapping logic already drafted in the `7. Industry Sector Classification Mapping` section of `notebooks/datalake-companies-investigation.ipynb`.

## 1. Requirements & Constraints

- **REQ-001**: Create a new template file `templates/brasa-industry-sectors.yaml` with `id: brasa-industry-sectors`.
- **REQ-002**: Implement as an ETL pipeline template (`etl.pipeline`) and not as `etl.function`.
- **REQ-003**: Use `staging.brasa-companies` as the only source dataset.
- **REQ-004**: Build output keys from `sector` and `subsector` with `DISTINCT` semantics.
- **REQ-005**: Output must include at minimum: `sector`, `subsector`, `gics_sector`, `icb_sector`, `normalized_sector`, `normalized_subsector`.
- **REQ-006**: Mapping values must be copied exactly from the notebook section `7. Industry Sector Classification Mapping` for overlapping tuples.
- **REQ-007**: Keep explicit fallback value `Other` for unmapped `(sector, subsector)` tuples.
- **REQ-008**: Persist output in `staging` layer using dataset name `brasa-industry-sectors`.
- **REQ-009**: Ensure template loads through `MarketDataTemplate` and is discoverable via `retrieve_template`.
- **REQ-010**: Add automated test coverage for template loading and expected field set.
- **SEC-001**: SQL must not execute dynamic string interpolation from runtime user input.
- **DAT-001**: Exclude rows where `sector` is null or empty after trim.
- **DAT-002**: Trim whitespace in `sector` and `subsector` before mapping.
- **CON-001**: Do not modify `templates/brasa-companies.yaml` schema in this scope.
- **CON-002**: Do not introduce new Python ETL functions unless pipeline steps are insufficient.
- **CON-003**: Keep compatibility with Python `^3.10` and current dependencies in `pyproject.toml`.
- **GUD-001**: Follow template conventions documented in `docs/TEMPLATES.md` and `.github/instructions/templates.instructions.md`.
- **PAT-001**: Reuse the existing `sql_query` ETL step pattern already used by `templates/brasa-companies.yaml`.

## 2. Implementation Steps

### Implementation Phase 1: Define Mapping Contract and Source Semantics

- GOAL-001: Freeze deterministic mapping rules and source-field contract before template creation.
- EXIT-001: A written mapping matrix exists in the plan and can be translated 1:1 into SQL `CASE` logic.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-001 | Extract canonical mapping tuples from `notebooks/datalake-companies-investigation.ipynb` section `7. Industry Sector Classification Mapping`: `gics_mapping`, `icb_mapping`, and `subsector_mapping` keyed by `(sector, subsector)`. | ✅ | 2026-02-20 |
| TASK-002 | Normalize source-column assumptions from `templates/brasa-companies.yaml`: `sector` and `subsector` are already generated from `industry_classification` split logic. | ✅ | 2026-02-20 |
| TASK-003 | Define fallback policy: unmapped `sector` -> `gics_sector='Unclassified'` and `icb_sector='Unclassified'`; unmapped tuple -> `normalized_subsector='Other'`. | ✅ | 2026-02-20 |
| TASK-004 | Define deterministic sort order for output rows: `ORDER BY sector, subsector`. | ✅ | 2026-02-20 |

### Implementation Phase 2: Create New ETL Template

- GOAL-002: Implement pipeline template producing `staging.brasa-industry-sectors`.
- EXIT-002: `templates/brasa-industry-sectors.yaml` is valid, loadable, and writes to `staging` with required fields.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-005 | Add file `templates/brasa-industry-sectors.yaml` with metadata sections: `id`, `description`, `etl.pipeline`, `writer`, and `fields`. | ✅ | 2026-02-20 |
| TASK-006 | In `etl.pipeline`, add `step: sql_query` with `datasets: [staging.brasa-companies]`. | ✅ | 2026-02-20 |
| TASK-007 | Implement SQL CTE `base` selecting distinct trimmed `sector`, `subsector` from `'staging.brasa-companies'`, filtering null/blank `sector`. | ✅ | 2026-02-20 |
| TASK-008 | Implement SQL mapping using deterministic `CASE` expressions for `gics_sector` and `icb_sector` based on `sector`. | ✅ | 2026-02-20 |
| TASK-009 | Implement SQL mapping for `normalized_sector` (English normalized level-1) and `normalized_subsector` based on `(sector, subsector)` with `Other` fallback. | ✅ | 2026-02-20 |
| TASK-010 | Set `writer.layer: staging` and `writer.dataset: brasa-industry-sectors`. | ✅ | 2026-02-20 |
| TASK-011 | Define explicit `fields` entries with `type: string` for all output columns. | ✅ | 2026-02-20 |

### Implementation Phase 3: Automated Validation and Regression Guard

- GOAL-003: Add tests ensuring template validity and schema contract stability.
- EXIT-003: Tests confirm template loads as ETL pipeline and contains mandatory output fields.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-012 | Update `tests/test_templates.py` with `test_brasa_industry_sectors_template_loads()` following existing pattern used by `test_brasa_companies_template_loads()`. | ✅ | 2026-02-20 |
| TASK-013 | Assert: template id is `brasa-industry-sectors`, `is_etl=True`, `etl.is_pipeline=True`, and expected fields are present. | ✅ | 2026-02-20 |
| TASK-014 | Add optional skipped integration test `test_brasa_industry_sectors_pipeline_execution()` with `@pytest.mark.skip` reason requiring local datalake in `staging.brasa-companies`. | ✅ | 2026-02-20 |
| TASK-015 | Run validation commands: `uv run pytest tests/test_templates.py -k industry_sectors`, `uv run ruff check .`. | ✅ | 2026-02-20 |

### Implementation Phase 4: Documentation and Operational Usage

- GOAL-004: Document execution path and lookup-table purpose for future consumers.
- EXIT-004: Repository docs mention the new dataset, source, and usage intent.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-016 | Update `docs/TEMPLATES.md` with a short ETL-single-dataset example reference for `brasa-industry-sectors` and source `staging.brasa-companies`. | ✅ | 2026-02-20 |
| TASK-017 | Update `README.md` or `docs/README.md` dataset section with one-line purpose: lookup table for sector taxonomy normalization. | ✅ | 2026-02-20 |
| TASK-018 | Add runbook snippet: `uv run brasa etl brasa-industry-sectors` (or repository-standard command equivalent) and expected output path in staging DB layer. | ✅ | 2026-02-20 |

## 3. Alternatives

- **ALT-001**: Keep mapping only in notebook code cells. Rejected because notebooks are not deterministic production ETL artifacts.
- **ALT-002**: Implement mapping in `brasa/etl.py` Python function. Rejected because pipeline YAML + `sql_query` is simpler, declarative, and aligned with current template standards.
- **ALT-003**: Join against an external CSV mapping file. Rejected to avoid extra artifact lifecycle and synchronization risk.
- **ALT-004**: Derive `normalized_subsector` only from `sector`. Rejected because objective requires `sector` + `subsector` granular lookup.

## 4. Dependencies

- **DEP-001**: Existing dataset `staging.brasa-companies` produced by `templates/brasa-companies.yaml`.
- **DEP-002**: ETL `sql_query` pipeline step registered in `brasa.engine.pipeline.registry.StepRegistry`.
- **DEP-003**: DuckDB SQL functions used in existing templates (`TRIM`, `CASE`, `ORDER BY`, `DISTINCT`).
- **DEP-004**: Test infrastructure in `tests/test_templates.py` and `MarketDataTemplate` loader.

## 5. Files

- **FILE-001**: `templates/brasa-industry-sectors.yaml` - New ETL template definition.
- **FILE-002**: `tests/test_templates.py` - Add template-load and optional skipped execution tests.
- **FILE-003**: `docs/TEMPLATES.md` - Add template reference and behavior summary.
- **FILE-004**: `README.md` or `docs/README.md` - Add dataset lookup-table mention.
- **FILE-005**: `plan/data-industry-sectors-1.md` - This implementation plan.

## 6. Testing

- **TEST-001**: Verify `MarketDataTemplate("templates/brasa-industry-sectors.yaml")` loads without errors.
- **TEST-002**: Verify template metadata (`id`, ETL pipeline flags) and mandatory field names.
- **TEST-003**: Verify SQL output schema contract via parser/load-level checks (column presence and types).
- **TEST-004**: Verify linter and style checks pass for modified files.
- **TEST-005**: (Skipped integration) Execute ETL pipeline when `staging.brasa-companies` exists and assert output dataset path is created.

## 7. Risks & Assumptions

- **RISK-001**: Notebook mapping and production SQL mapping can diverge over time. Mitigation: include exact tuple mapping extraction in `TASK-001` and keep mapping centralized in one template.
- **RISK-002**: New sectors can appear in source data and remain unmapped. Mitigation: explicit fallback values and periodic review process.
- **RISK-003**: If source dataset schema changes (`sector/2` renamed), ETL fails. Mitigation: load tests and template contract checks.
- **ASSUMPTION-001**: `staging.brasa-companies` remains available before this ETL is executed.
- **ASSUMPTION-002**: Sector hierarchy values in source remain Portuguese labels as in current notebook mapping.
- **ASSUMPTION-003**: `sql_query` step supports all required SQL constructs without custom Python extension.

## 8. Related Specifications / Further Reading

- `docs/TEMPLATES.md`
- `.github/instructions/templates.instructions.md`
- `templates/brasa-companies.yaml`
- `tests/test_templates.py`
- `notebooks/datalake-companies-investigation.ipynb` (section `7. Industry Sector Classification Mapping`)
