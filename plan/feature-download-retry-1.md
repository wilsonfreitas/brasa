---
goal: Implement deterministic retry policy for unstable downloader operations
version: 1.0
date_created: 2026-02-18
last_updated: 2026-02-18
owner: brasa-team
status: 'Planned'
tags: ['feature', 'download', 'retry', 'resilience', 'templates']
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

This plan defines a template-driven retry mechanism for unstable downloads, with configuration inside `downloader` (same scope as `download_delay`) and deterministic behavior across all templates and execution paths.

## 1. Requirements & Constraints

- **REQ-001**: Retry configuration must be declared under `downloader` in template YAML.
- **REQ-002**: Existing templates without retry keys must preserve current behavior (single attempt).
- **REQ-003**: Retry behavior must be deterministic and centrally implemented in one code path.
- **REQ-004**: Retry must apply to download failures raised before file validation completes.
- **REQ-005**: Validation failures (`InvalidContentException`) must not be retried by default.
- **REQ-006**: Retry configuration must support unstable B3 endpoints such as `b3-company-details`.
- **REQ-007**: Retry behavior must be visible in logs and task reports with attempt count.
- **SEC-001**: Retry logs must not include sensitive request payloads beyond current logging scope.
- **CON-001**: Keep Python/dependency compatibility defined in `pyproject.toml`.
- **CON-002**: Keep current public API signatures backward compatible.
- **CON-003**: Preserve current `download_delay` semantics between task iterations in `download_marketdata`.
- **GUD-001**: New configuration names must be snake_case and explicit.
- **GUD-002**: Use existing exception taxonomy (`DownloadException`, `InvalidContentException`, `DuplicatedFolderException`).
- **PAT-001**: Keep retry orchestration in engine layer; keep downloader classes focused on one HTTP attempt.

### Retry Configuration Specification

| Key | Type | Default | Scope | Behavior |
|-----|------|---------|-------|----------|
| `retry_attempts` | `int` | `0` | `downloader` | Number of additional attempts after the first failure. Total attempts = `1 + retry_attempts`. |
| `retry_delay` | `float` | `0.0` | `downloader` | Initial delay in seconds before retry #1. |
| `retry_backoff` | `float` | `1.0` | `downloader` | Multiplier applied after each failed retry (`delay = delay * retry_backoff`). |
| `retry_on_status_codes` | `list[int]` | `[408, 425, 429, 500, 502, 503, 504]` | `downloader` | HTTP status codes considered transient. |
| `retry_on_download_exception` | `bool` | `true` | `downloader` | Retry when a `DownloadException` occurs without explicit status extraction. |

### Deterministic Retry Rules

| Rule ID | Rule |
|---------|------|
| `RTRY-001` | Attempt 1 always executes immediately with current downloader function. |
| `RTRY-002` | Retry executes only when failure is classified transient by status code or by `retry_on_download_exception=true`. |
| `RTRY-003` | `InvalidContentException` and `DuplicatedFolderException` are non-retriable by default. |
| `RTRY-004` | On final failure, original exception chain is preserved (`raise ... from ...`). |
| `RTRY-005` | Retry sleeps are independent of `download_delay` (which remains inter-task pacing). |

## 2. Implementation Steps

### Implementation Phase 1

- GOAL-001: Add retry configuration fields to template downloader model with strict defaults and validation.
- GOAL-001-Criteria: `MarketDataDownloader` exposes all retry attributes with defaults; existing templates load without changes.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-001 | Update `brasa/engine/template.py::MarketDataDownloader.__init__` to parse `retry_attempts`, `retry_delay`, `retry_backoff`, `retry_on_status_codes`, `retry_on_download_exception`. |  |  |
| TASK-002 | Add private validation method in `brasa/engine/template.py` to enforce: `retry_attempts >= 0`, `retry_delay >= 0`, `retry_backoff >= 1`, status codes in `[100, 599]`. |  |  |
| TASK-003 | Add docstring attributes in `MarketDataDownloader` describing retry keys and defaults. |  |  |

### Implementation Phase 2

- GOAL-002: Implement centralized retry executor around downloader function invocation.
- GOAL-002-Criteria: One reusable retry path is used for all templates; no duplication in downloader classes.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-004 | Refactor `brasa/engine/template.py::MarketDataDownloader.download` to wrap `self.download_function(self, **args)` in retry loop controlled by parsed retry config. |  |  |
| TASK-005 | Add helper `brasa/engine/template.py::_extract_status_code_from_exception(err: Exception) -> int | None` to parse status information from nested exceptions/messages. |  |  |
| TASK-006 | Add helper `brasa/engine/template.py::_is_retriable_failure(err: Exception, status_code: int | None) -> bool` implementing `RTRY-002` and `RTRY-003`. |  |  |
| TASK-007 | Ensure each failed retriable attempt logs `template id`, `attempt`, `max_attempts`, `status_code`, and next delay via `logging`. |  |  |

### Implementation Phase 3

- GOAL-003: Preserve engine behavior and expose retry telemetry in report output.
- GOAL-003-Criteria: task results include retry metadata without breaking existing consumers.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-008 | Update `brasa/engine/download.py::_download_marketdata` to store retry metadata in `meta.response` keys: `retry_attempts_used`, `retry_attempts_configured`, `retry_success_on_attempt`. |  |  |
| TASK-009 | Update `brasa/engine/api.py::download_marketdata` to include retry metadata in `TaskResult.extra_info` when available. |  |  |
| TASK-010 | Keep `download_delay` usage unchanged in `brasa/engine/api.py` (between task iterations only). |  |  |

### Implementation Phase 4

- GOAL-004: Add template-level adoption for unstable endpoint and test all retry outcomes.
- GOAL-004-Criteria: unstable template has retry config and tests validate success/failure paths.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-011 | Update `templates/b3-company-details.yaml` downloader block with explicit retry keys (`retry_attempts`, `retry_delay`, `retry_backoff`). |  |  |
| TASK-012 | Add unit tests in `tests/test_template.py` covering retry config parsing defaults and validation errors. |  |  |
| TASK-013 | Add unit tests in `tests/test_download_retry.py` with mocked downloader function for: immediate success, success after transient failure, final failure after max retries, and non-retriable invalid-content failure. |  |  |
| TASK-014 | Add regression test in `tests/test_api.py` verifying `download_delay` semantics remain unchanged when retry is enabled. |  |  |

### Implementation Phase 5

- GOAL-005: Document retry policy and usage for operators.
- GOAL-005-Criteria: docs include config keys, defaults, behavior matrix, and B3 example.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-015 | Add section "Downloader Retry Policy" to `docs/TEMPLATES.md` with key definitions and deterministic rules table. |  |  |
| TASK-016 | Add operator troubleshooting section to `docs/USER_GUIDE.md` showing how to tune `retry_attempts` and `retry_delay` for unstable endpoints. |  |  |

## 3. Alternatives

- **ALT-001**: Implement retry centrally in `brasa/engine/template.py::MarketDataDownloader.download` (chosen) because it applies to all downloader functions with one deterministic policy and minimal code duplication.
- **ALT-002**: Implement retry inside each class in `brasa/downloaders/downloaders.py` (rejected) because behavior diverges by class and maintenance cost grows with each new downloader.
- **ALT-003**: Use `requests` adapter-level retry (`urllib3.Retry`) globally (rejected for now) because not all flows are plain `requests.get`, and current architecture wraps failures via helper functions; mixed behavior would be harder to keep deterministic.
- **ALT-004**: Retry entire `cache.download_marketdata` operation including validation and file system steps (rejected) because it risks repeated folder/metadata side effects and complicates cleanup semantics.

## 4. Dependencies

- **DEP-001**: Template parsing and downloader execution in `brasa/engine/template.py`.
- **DEP-002**: Download orchestration in `brasa/engine/download.py` and `brasa/engine/api.py`.
- **DEP-003**: Existing downloader helpers/classes in `brasa/downloaders/helpers.py` and `brasa/downloaders/downloaders.py`.
- **DEP-004**: Existing exception classes in `brasa/engine/exceptions.py`.
- **DEP-005**: pytest framework and existing fixtures in `tests/`.

## 5. Files

- **FILE-001**: `brasa/engine/template.py` - Add retry config parsing, validation, and retry loop execution.
- **FILE-002**: `brasa/engine/download.py` - Persist retry metadata in response/meta context.
- **FILE-003**: `brasa/engine/api.py` - Surface retry metadata in task results.
- **FILE-004**: `templates/b3-company-details.yaml` - Enable retry configuration for unstable endpoint.
- **FILE-005**: `tests/test_template.py` - Add parsing/validation tests for retry keys.
- **FILE-006**: `tests/test_download_retry.py` - Add retry behavior unit tests.
- **FILE-007**: `tests/test_api.py` - Add regression test for `download_delay` interaction.
- **FILE-008**: `docs/TEMPLATES.md` - Document retry keys and deterministic rules.
- **FILE-009**: `docs/USER_GUIDE.md` - Add operator tuning guidance.

## 6. Testing

- **TEST-001**: Verify templates without retry keys still execute one attempt only.
- **TEST-002**: Verify `retry_attempts=2` produces maximum 3 total attempts.
- **TEST-003**: Verify backoff delays follow deterministic sequence (`retry_delay`, `retry_delay*retry_backoff`, ...).
- **TEST-004**: Verify status code `503` triggers retry when included in `retry_on_status_codes`.
- **TEST-005**: Verify non-retriable exception (`InvalidContentException`) fails immediately.
- **TEST-006**: Verify final exception preserves original cause chain.
- **TEST-007**: Verify `TaskResult.extra_info` includes retry metadata fields.
- **TEST-008**: Verify `download_delay` between tasks remains unchanged by retry policy.

## 7. Risks & Assumptions

- **RISK-001**: Current `DownloadException` messages may not always expose HTTP status code; fallback classification could over-retry. Mitigation: support configurable `retry_on_download_exception` and explicit status extraction helper.
- **RISK-002**: Aggressive retry defaults may increase API throttling on B3 endpoints. Mitigation: conservative defaults and template-level tuning.
- **RISK-003**: Sleep-based retries may increase total batch runtime. Mitigation: bounded `retry_attempts` and documented tuning guidance.
- **ASSUMPTION-001**: Most unstable failures are transient HTTP/network errors rather than deterministic payload validation failures.
- **ASSUMPTION-002**: Retry policy should be template-specific, not global.

## 8. Related Specifications / Further Reading

- [docs/TEMPLATES.md](../docs/TEMPLATES.md)
- [docs/ETL_PIPELINE_DESIGN.md](../docs/ETL_PIPELINE_DESIGN.md)
- [docs/IMPLEMENTATION_INVALID_DOWNLOADS.md](../docs/IMPLEMENTATION_INVALID_DOWNLOADS.md)
- [brasa/engine/template.py](../brasa/engine/template.py)
- [brasa/engine/download.py](../brasa/engine/download.py)
- [brasa/engine/api.py](../brasa/engine/api.py)
- [templates/b3-company-details.yaml](../templates/b3-company-details.yaml)
