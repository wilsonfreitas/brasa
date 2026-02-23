---
goal: Fix SQLite Connection Leaks
version: 1.0
date_created: 2026-02-22
last_updated: 2026-02-22
owner: AI Assistant
status: 'Planned'
tags: [refactor, bug, database, architecture]
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

This plan addresses the "database could not be opened" error encountered during parallel processing, which is caused by SQLite file descriptor leaks. The goal is to properly close database connections using `contextlib.closing` while maintaining transaction management via nested `with conn:` blocks, eliminating the need for explicit `conn.commit()` calls.

## 1. Requirements & Constraints

- **REQ-001**: Must resolve "database could not be opened" error during parallel processing.
- **REQ-002**: Must use `contextlib.closing` to wrap connection properties.
- **REQ-003**: Must use nested `with conn:` blocks for transaction management.
- **REQ-004**: Must remove explicit `conn.commit()` calls where nested `with conn:` is used.
- **CON-001**: Must preserve backward compatibility for external code accessing the connection property directly.
- **PAT-001**: Follow existing codebase patterns for imports and formatting (Ruff, line length 88).

## 2. Implementation Steps

### Implementation Phase 1

- GOAL-001: Update CacheManager to properly close connections and remove explicit commits.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-001 | In `brasa/engine/cache.py`, import `closing` from `contextlib`. | | |
| TASK-002 | In `brasa/engine/cache.py`, replace `with self.meta_db_connection as conn:` with `with closing(self.meta_db_connection) as conn:` and nest `with conn:`. | | |
| TASK-003 | In `brasa/engine/cache.py`, remove `conn.commit()` in `save_meta`, `clean_meta_db`, and `save_trial`. | | |

### Implementation Phase 2

- GOAL-002: Update DatasetCatalog to properly close connections and remove explicit commits.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-004 | In `brasa/engine/catalog.py`, import `closing` from `contextlib`. | | |
| TASK-005 | In `brasa/engine/catalog.py`, replace `with self._connection as conn:` with `with closing(self._connection) as conn:` and nest `with conn:`. | | |
| TASK-006 | In `brasa/engine/catalog.py`, remove `conn.commit()` in `register_dataset` and `remove_dataset`. | | |

### Implementation Phase 3

- GOAL-003: Update API and Tests to properly close connections.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-007 | In `brasa/engine/api.py`, update `process_marketdata` to use `closing` and `with conn:` when querying `cache_metadata`. | | |
| TASK-008 | In `tests/conftest.py`, update database queries to use `closing` and `with conn:`. | | |
| TASK-009 | In `tests/test_download_status.py`, update database queries to use `closing` and `with conn:`. | | |

## 3. Alternatives

- **ALT-001**: Changing the connection property to a context manager. Rejected to preserve backward compatibility with external code accessing the connection property directly.

## 4. Dependencies

- **DEP-001**: `contextlib` (Python standard library).
- **DEP-002**: `sqlite3` (Python standard library).

## 5. Files

- **FILE-001**: `brasa/engine/cache.py` - CacheManager connection handling.
- **FILE-002**: `brasa/engine/catalog.py` - DatasetCatalog connection handling.
- **FILE-003**: `brasa/engine/api.py` - API queries connection handling.
- **FILE-004**: `tests/conftest.py` - Test fixtures connection handling.
- **FILE-005**: `tests/test_download_status.py` - Test queries connection handling.

## 6. Testing

- **TEST-001**: Run `poetry run pytest` to ensure all tests pass and database operations function correctly.
- **TEST-002**: Run `process_marketdata` with multiple workers to verify the "database could not be opened" error no longer occurs.

## 7. Risks & Assumptions

- **RISK-001**: Potential missed connection closures if not all instances of `with connection as conn:` are identified and updated.
- **ASSUMPTION-001**: The file descriptor leak is solely caused by the lack of explicit connection closures in the identified files.

## 8. Related Specifications / Further Reading

- [Python sqlite3 documentation](https://docs.python.org/3/library/sqlite3.html)
- [Python contextlib documentation](https://docs.python.org/3/library/contextlib.html)
