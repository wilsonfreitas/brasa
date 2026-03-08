# Plan: `brasa doctor` Command

## Context

The brasa cache accumulates state over time: downloaded raw files, parsed parquet datasets, SQLite metadata, and YAML templates. Drift can occur — orphan files, broken references, stale ETL outputs, date gaps, schema mismatches. The `brasa doctor` command gives users a single diagnostic tool to surface and optionally fix these issues, similar to `brew doctor` or `cargo doctor`.

---

## Architecture

### New files
- `brasa/engine/doctor.py` — all check logic and data types

### Modified files
- `brasa/cli.py` — add `doctor` subcommand + `_COMMAND_GROUPS` entry

---

## Data Types (`doctor.py`)

```python
@dataclass
class Issue:
    category: str         # e.g. "Raw Files", "DB / Parquet"
    code: str             # e.g. "orphan-raw", "missing-raw"
    severity: str         # "error" | "warning" | "info"
    description: str      # human-readable message
    details: list[str]    # list of affected paths/ids
    fixable: bool         # whether --fix can address it
    fix_fn: Callable | None  # called when --fix is active

@dataclass
class DoctorReport:
    issues: list[Issue]
    def errors(self) -> list[Issue]: ...
    def warnings(self) -> list[Issue]: ...
    def fixable(self) -> list[Issue]: ...
    def summary(self) -> str: ...
```

---

## Checks

### Category: Raw Files
| Code | Description | Fixable |
|------|-------------|---------|
| `orphan-raw` | Checksum folders in `raw/` not referenced in any `cache_metadata.downloaded_files` | Yes — delete folder |
| `missing-raw` | `cache_metadata.downloaded_files` entries pointing to non-existent paths | Yes — mark metadata as invalid |

### Category: DB / Parquet
| Code | Description | Fixable |
|------|-------------|---------|
| `orphan-db` | Folders in `db/{layer}/` with no matching template or `dataset_catalog` entry | Yes — delete folder |
| `missing-db` | `cache_metadata.processed_files` entries pointing to non-existent parquet paths | No — requires re-processing |
| `empty-parquet` | Parquet partition directories that contain no `.parquet` files | Yes — delete empty dir |
| `corrupted-parquet` | Parquet files that raise an error when read by PyArrow | No — requires re-processing |
| `schema-drift` | `dataset_catalog` schema differs from current template `fields` definition | No — informational |

### Category: Metadata
| Code | Description | Fixable |
|------|-------------|---------|
| `unresolved-errors` | `cache_metadata` rows with non-empty `processing_errors` | No — informational |
| `invalid-downloads` | `cache_metadata` rows marked `is_invalid_download='1'` | Yes — delete raw folder + metadata row |

### Category: Template Consistency
| Code | Description | Fixable |
|------|-------------|---------|
| `stale-etl` | Staging/curated dataset whose `updated_at` is older than its source input dataset's `updated_at` | No — run `brasa process <template>` |
| `missing-etl-source` | ETL template references an upstream dataset with no data on disk | No — informational |

### Category: Date Gaps
| Code | Description | Fixable |
|------|-------------|---------|
| `date-gaps` | Time-series datasets with missing business days (B3 calendar) between first and last available date | No — run `brasa download` for missing dates |

---

## CLI Interface

```
brasa doctor [--fix] [--category CATEGORY ...] [--template TEMPLATE ...]
             [--since DAYS]
```

- `--fix`: apply all auto-fixable issues (with confirmation prompt unless `--yes`)
- `--yes`: skip confirmation when using `--fix`
- `--category`: run only specific categories (`raw`, `db`, `meta`, `templates`, `gaps`)
- `--template`: restrict `date-gaps` and `stale-etl` checks to specific templates
- `--since N`: for date-gap checks, only look back N days (default: 30)

**Exit code:** 0 if no errors, 1 if any error-severity issues found.

---

## Output Format (rich)

Uses `rich` (already a dependency) for colored terminal output:

```
Brasa Doctor
════════════

Raw Files
  ✓  No orphan raw files
  ✗  2 metadata entries reference missing raw files
       raw/b3-cotahist-daily/abc123/COTAHIST_D01012024.ZIP
       raw/b3-bvbg028/def456/BVBG028.xml
     → fixable: run with --fix to mark entries as invalid

DB / Parquet
  ✓  All DB folders have a matching template
  ⚠  1 corrupted parquet file (manual re-process required)
       db/input/b3-cotahist-daily/refdate=2024-01-15/part-0.parquet

Template Consistency
  ⚠  b3-equities-returns ETL is stale (input updated 2 days ago)
     → run: brasa process b3-equities-returns

Date Gaps (last 30 days)
  ✗  b3-cotahist-daily: 3 missing business days
       2024-03-05, 2024-03-06, 2024-03-07
     → run: brasa download b3-cotahist-daily -d 2024-03-05 2024-03-07

────────────────────────────────────
Summary: 2 errors · 2 warnings · 1 fixable
Run `brasa doctor --fix` to apply auto-fixes.
```

---

## Implementation Notes

- Access `CacheManager()` singleton for raw/meta paths
- Query SQLite directly via `CacheManager()._conn` (or through existing engine methods) for metadata checks
- Use `pyarrow.parquet.read_schema()` for non-destructive parquet validation (schema check + corrupted check)
- For date-gap detection: get the `dataset_catalog`, read partition values (folders named `refdate=YYYY-MM-DD`), compare against `bizdays` B3 calendar
- Schema drift: compare `dataset_catalog.schema_json` vs. field definitions from `retrieve_template()`
- `stale-etl`: compare `dataset_catalog.updated_at` for staging vs. its source input dataset

---

## Verification

```bash
# Smoke test (no issues expected on fresh cache)
uv run python -m brasa doctor

# Test --fix flow (with --yes to skip prompt)
uv run python -m brasa doctor --fix --yes

# Category filter
uv run python -m brasa doctor --category raw db

# Run test suite
uv run pytest tests/test_doctor.py

# Linting
uv run ruff check brasa/engine/doctor.py brasa/cli.py
uv run ruff format --check .
```

Tests should cover each check function independently using a temporary cache directory (follow the existing `conftest.py` pattern with `BRASA_DATA_PATH` set to a temp dir).
