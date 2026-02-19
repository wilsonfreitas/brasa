---
goal: Implement deterministic Download Status classification for every download attempt
version: 1.0
date_created: 2026-02-18
last_updated: 2026-02-18
owner: brasa-team
status: 'Planned'
tags: ['feature', 'download', 'status', 'reporting', 'cache']
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

This plan defines and implements a single, deterministic download status model for all download attempts in brasa. The objective is to classify every attempt with one explicit code, persist it, and expose it in reports/CLI without ambiguity.

## 1. Requirements & Constraints

- **REQ-001**: Every download attempt must produce exactly one status code.
- **REQ-002**: Status mapping must be deterministic from current execution paths in `brasa/engine/cache.py::download_marketdata` and `brasa/engine/api.py::download_marketdata`.
- **REQ-003**: Statuses must be persisted per attempt in the `download_trials` table.
- **REQ-004**: CLI/report output must display both aggregate counts and per-task status symbols.
- **REQ-005**: Existing behavior for cache reuse, reprocess, and invalid download skip must remain backward compatible.
- **REQ-006**: Existing `TaskStatus` behavior must continue to work for downstream code that consumes `TaskReport`.
- **SEC-001**: Do not persist sensitive payload data in status fields; store only status code, reason, and optional HTTP/status metadata.
- **CON-001**: Use SQLite metadata database (`meta.db`) and existing CacheManager persistence patterns.
- **CON-002**: Python and dependencies must remain compatible with versions in `pyproject.toml`.
- **CON-003**: Migration must preserve compatibility with existing `download_trials(downloaded)` rows.
- **GUD-001**: Keep status symbols single-character and non-conflicting.
- **GUD-002**: Use explicit enum constants instead of string literals in control flow.
- **PAT-001**: Follow existing exception taxonomy: `DownloadException`, `InvalidContentException`, `DuplicatedFolderException`, generic `Exception`.

### Status Code Specification

| Code | Name | Deterministic Trigger | Source Path |
|------|------|------------------------|-------------|
| `.` | PASSED | Successful `_download_marketdata` completion | `cache.py::download_marketdata` success branch |
| `F` | FAILED | Expected download failure (`DownloadException`), including HTTP non-2xx raised by downloaders | `cache.py` + `downloaders/*.py` |
| `E` | ERROR | Unexpected unhandled exception path (`except Exception`) | `cache.py::download_marketdata` generic exception branch |
| `S` | SKIPPED | `_should_download(...) == False` in API workflow | `api.py::download_marketdata` skip branch |
| `D` | DUPLICATED | `DuplicatedFolderException` raised because target raw folder already exists | `download.py::_download_marketdata` + `cache.py` |
| `I` | INVALID | Downloaded file fails one or more template validation rules, causing `InvalidContentException` in post-download validation | `download.py::_validate_downloaded_files` + `cache.py` |
| `W` | WARNING | Optional non-terminal warning classification for successful attempt with warnings (no exception) | `reporting.py` warning capture |

### Deterministic Symbol Rule

- **REQ-007**: Symbol `S` is reserved for SKIPPED only.
- **REQ-008**: Success symbol is `.` only (no `S` alias) to avoid collision.

## 2. Implementation Steps

### Implementation Phase 1: Define Status Model

- GOAL-001: Introduce a canonical download status model and symbol mapping with compile-time discoverability and explicit completion criteria.
- GOAL-001-Criteria: Phase completes when all status codes (`.,F,E,S,D,I,W`) are represented in code by constants/enums and unit tests verify symbol uniqueness.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-001 | Add `DownloadAttemptStatus` enum in `brasa/engine/reporting.py` with values `PASSED, FAILED, ERROR, SKIPPED, DUPLICATED, INVALID, WARNING` and `symbol` property returning `.,F,E,S,D,I,W`. |  |  |
| TASK-002 | Add deterministic mapper function `map_exception_to_download_status(ex: Exception) -> DownloadAttemptStatus` in `brasa/engine/reporting.py`. |  |  |
| TASK-003 | Keep existing `TaskStatus` API compatibility; add explicit conversion helper `to_task_status(download_status)` for report integration. |  |  |
| TASK-004 | Add unit tests in `tests/test_reporting.py` asserting all symbol values are unique and stable. |  |  |

### Implementation Phase 2: Persist Attempt Status in Metadata DB

- GOAL-002: Persist one status per attempt in `download_trials` without breaking old rows.
- GOAL-002-Criteria: Phase completes when `save_trial` writes status fields, existing reads remain valid, and migration path is covered by tests.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-005 | Update `sql/create-meta-db.sql` to include new `download_trials` columns: `status_code TEXT`, `status_name TEXT`, `reason TEXT`, `http_status INTEGER`. |  |  |
| TASK-006 | Add migration logic in `brasa/engine/cache.py::init` to `ALTER TABLE download_trials` for missing columns (idempotent checks via `PRAGMA table_info`). |  |  |
| TASK-007 | Refactor `CacheManager.save_trial(meta, downloaded)` to `save_trial(meta, status_code, status_name, reason="", http_status=None)` and keep backward wrapper for internal compatibility during transition. |  |  |
| TASK-008 | Add `CacheManager.get_last_download_status(meta)` returning structured dict with code/name/reason/http status for CLI/report usage. |  |  |
| TASK-009 | Add tests in `tests/test_cache.py` validating legacy and migrated schemas, plus persisted status retrieval. |  |  |

### Implementation Phase 3: Classify All Download Outcomes in Runtime Flow

- GOAL-003: Ensure every execution branch in download flow records one deterministic status.
- GOAL-003-Criteria: Phase completes when all branches in `cache.py::download_marketdata` and `api.py::download_marketdata` set status explicitly and branch coverage tests pass.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-010 | In `brasa/engine/cache.py::download_marketdata`, replace boolean `save_trial` calls with explicit statuses: success `.`/PASSED, duplicated `D`, invalid `I`, expected failure `F`, unexpected error `E`. |  |  |
| TASK-011 | In `brasa/engine/api.py::download_marketdata`, record skip `S` when `_should_download(...)` returns `False` (without invoking downloader). |  |  |
| TASK-012 | Extend `DownloadException` creation points in `brasa/downloaders/downloaders.py` to include optional normalized HTTP status extraction for persistence (`http_status`). |  |  |
| TASK-013 | Ensure invalid-download short-circuit in `_should_download` remains SKIPPED (`S`) and reason includes `invalid_download_reason` that references validation-rule failure context. |  |  |
| TASK-014 | Add integration tests in `tests/test_invalid_downloads.py` and new `tests/test_download_status.py` for all 6 core status outcomes (`.,F,E,S,D,I`). |  |  |

### Implementation Phase 4: Surface Statuses in Reports and CLI

- GOAL-004: Expose status taxonomy in outputs used by operators and CI.
- GOAL-004-Criteria: Phase completes when report text/JSON contains status code+name per task and summary counts for all status types.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-015 | Update `TaskResult.extra_info` population in `brasa/engine/api.py` to include `download_status_code`, `download_status_name`, `download_status_reason`, `http_status`. |  |  |
| TASK-016 | Update summary rendering in `brasa/engine/reporting.py` to show counts for `D` and `I` distinctly (not merged into generic failed/error). |  |  |
| TASK-017 | Update CLI output in `brasa/cli.py` download command to print status legend: `.(passed) F(failed) E(error) S(skipped) D(duplicated) I(invalid)`. |  |  |
| TASK-018 | Add/extend report serialization tests ensuring JSON report contains deterministic status fields for each task. |  |  |

### Implementation Phase 5: Documentation and Rollout

- GOAL-005: Document the status model and migration behavior for maintainers and users.
- GOAL-005-Criteria: Phase completes when docs include status definitions, migration notes, and troubleshooting guidance.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-019 | Add section "Download Status Codes" to `docs/USER_GUIDE.md` with complete code table and branch mapping. |  |  |
| TASK-020 | Update `docs/IMPLEMENTATION_INVALID_DOWNLOADS.md` to reference `I` (invalid) vs `S` (skipped) behavior. |  |  |
| TASK-021 | Add changelog entry to `README.md` or release notes section describing new deterministic statuses and DB migration impact. |  |  |

## 3. Alternatives

- **ALT-001**: Reuse only existing `TaskStatus` (`passed/failed/error/skipped/warning`) without adding `D` and `I` - Rejected because duplicated and invalid outcomes are operationally distinct and currently conflated.
- **ALT-002**: Keep only boolean `downloaded` in `download_trials` - Rejected because it cannot represent skip, duplicate, invalid, and unexpected error separately.
- **ALT-003**: Encode status only in log text, not DB - Rejected because logs are not queryable/reliable for programmatic auditing.
- **ALT-004**: Use `S` for both success and skipped - Rejected due deterministic symbol collision.

## 4. Dependencies

- **DEP-001**: Existing exception classes in `brasa/engine/exceptions.py`.
- **DEP-002**: Existing reporting framework in `brasa/engine/reporting.py`.
- **DEP-003**: SQLite schema bootstrap in `sql/create-meta-db.sql` and cache initialization in `brasa/engine/cache.py`.
- **DEP-004**: pytest test framework and fixtures in `tests/conftest.py`.

## 5. Files

- **FILE-001**: `brasa/engine/reporting.py` - Add canonical download status enum/mappers and summary handling.
- **FILE-002**: `brasa/engine/cache.py` - Persist explicit attempt status and add migration/retrieval helpers.
- **FILE-003**: `brasa/engine/api.py` - Record skip and attach status details to task results.
- **FILE-004**: `brasa/downloaders/downloaders.py` - Normalize and propagate HTTP status for expected failures.
- **FILE-005**: `sql/create-meta-db.sql` - Extend `download_trials` schema for status fields.
- **FILE-006**: `brasa/cli.py` - Display status legend and extended status summaries.
- **FILE-007**: `tests/test_reporting.py` - Add symbol uniqueness and mapping tests.
- **FILE-008**: `tests/test_cache.py` - Add persistence/migration tests for download status columns.
- **FILE-009**: `tests/test_download_status.py` - Add end-to-end status outcome tests.
- **FILE-010**: `tests/test_invalid_downloads.py` - Extend tests for invalid vs skipped semantics.
- **FILE-011**: `docs/USER_GUIDE.md` - Document status taxonomy and interpretation.
- **FILE-012**: `docs/IMPLEMENTATION_INVALID_DOWNLOADS.md` - Align invalid download behavior with new status model.

## 6. Testing

- **TEST-001**: Verify status symbol set is exactly `{.,F,E,S,D,I,W}` and contains no duplicates.
- **TEST-002**: Verify `DuplicatedFolderException` maps to `D` and persists `status_code='D'`.
- **TEST-003**: Verify validation-rule failure (`template.downloader.validate`) raises `InvalidContentException`, maps to `I`, and metadata flag behavior remains unchanged.
- **TEST-004**: Verify `DownloadException` maps to `F`, including optional HTTP status persistence.
- **TEST-005**: Verify generic `Exception` maps to `E`.
- **TEST-006**: Verify `_should_download == False` path records/returns `S` in report output.
- **TEST-007**: Verify successful downloads record `.` with non-empty downloaded file list.
- **TEST-008**: Verify migration of existing `download_trials` schema does not break old data reads.
- **TEST-009**: Verify report JSON contains `download_status_code`, `download_status_name`, `download_status_reason`, and `http_status` when available.

## 7. Risks & Assumptions

- **RISK-001**: Status duplication between `TaskStatus` and new download status model may cause drift - Mitigate with one canonical mapper and unit tests.
- **RISK-002**: SQLite migration can fail on locked database files - Mitigate with explicit error handling and startup warnings.
- **RISK-003**: Some downloaders may not expose HTTP status consistently - Mitigate by allowing `http_status=NULL` and preserving reason text.
- **RISK-004**: Legacy tooling may assume boolean `downloaded` semantics only - Mitigate by preserving legacy column and derived compatibility.
- **ASSUMPTION-001**: All download attempts continue to pass through `CacheManager.download_marketdata` and `api.download_marketdata`.
- **ASSUMPTION-002**: Existing tests can be expanded without changing external network dependencies.

## 8. Related Specifications / Further Reading

- [docs/IMPLEMENTATION_INVALID_DOWNLOADS.md](../docs/IMPLEMENTATION_INVALID_DOWNLOADS.md)
- [docs/INVALID_DOWNLOADS_QUICK_REFERENCE.md](../docs/INVALID_DOWNLOADS_QUICK_REFERENCE.md)
- [docs/USER_GUIDE.md](../docs/USER_GUIDE.md)
- [brasa/engine/cache.py](../brasa/engine/cache.py)
- [brasa/engine/api.py](../brasa/engine/api.py)
- [brasa/engine/reporting.py](../brasa/engine/reporting.py)
