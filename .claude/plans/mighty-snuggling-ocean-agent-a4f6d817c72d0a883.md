# Brasa CLI & Download Plan Exploration Report

## Overview
This report explores the brasa project's CLI structure, download plan mechanism, and related functionality to support planning a "smart update" feature.

---

## 1. CLI Structure

### Entry Point
- **File**: `/home/wilson/dev/python/brasa/brasa/cli.py`
- **Parser Type**: `argparse.ArgumentParser` with custom `_GroupedHelpFormatter`
- **Main subcommands**: `download`, `process`, `create-views`, `query`, `head`, `list-datasets`, etc.

### Download Command (`brasa download`)

**Location**: Lines 166-199 and 725-775 in `cli.py`

**Arguments**:
```
download <template_name>... [options]
```

**Options**:
- `--plan FILE`: Path to YAML download plan file (mutually exclusive with template names)
- `--arg KEY=VALUE`: Repeatable flag for download arguments
  - Supports DSL: `@` for dates, `$` for symbols, commas for lists
  - Examples: `@2026-01`, `@2026-01-01:2026-01-31`, `$index`, `IBOV,BOVA11`
- `--calendar {B3|ANBIMA}`: Business calendar for date parsing (default: B3)
- `--force`: Force re-download even if cached
- `-v/--verbose`, `-q/--quiet`: Verbosity control

**Implementation Flow**:
1. Parse arguments and validate mutual exclusivity (plan vs. templates)
2. If `--plan`: Load and execute plan via `execute_download_plan()`
3. Else: Loop over templates and call `download_marketdata()` for each

**Key Code** (lines 725-775):
```python
if plan_file:
    plan = DownloadPlan.from_file(plan_file)
    execute_download_plan(
        plan,
        refdate_override=refdate_override,
        verbosity=verbosity,
        report_file=report_file,
    )
else:
    for template in templates:
        download_marketdata(
            template,
            force=args.force,
            verbosity=verbosity,
            **download_kwargs,
        )
```

### Process Command (`brasa process`)

**Location**: Lines 201-210 and 776-793 in `cli.py`

**Arguments**:
```
process <template_name>... [options]
```

**Options**:
- `--reprocess`: Reprocess even if already processed
- `-v/--verbose`, `-q/--quiet`: Verbosity control

**Implementation**:
- Checks if template is ETL or market data
- Calls `process_etl()` or `process_marketdata()` with `reprocess` flag

---

## 2. Download Plan System

### Structure & Files

**Location**: `/home/wilson/dev/python/brasa/brasa/engine/download_plan.py`

**Core Classes**:

1. **DownloadPlanDefaults** (lines 43-54)
   - `refdate`: DateRangeParser string (e.g., "2026-01-01:", "2026")
   - `calendar`: Business calendar name (default: "B3")
   - `reprocess`: Default reprocess flag (default: False)

2. **DownloadPlanTask** (lines 58-69)
   - `template`: Template name to download
   - `args`: Per-task argument overrides
   - `reprocess`: Task-level reprocess flag

3. **DownloadPlan** (lines 73-167)
   - `name`: Plan name
   - `description`: Optional description
   - `defaults`: Applied to all tasks
   - `tasks`: Ordered list of DownloadPlanTask objects

4. **DownloadPlanReport** (lines 176-315)
   - `plan_name`: Name of executed plan
   - `task_reports`: Dict mapping template name → TaskReport
   - `implicit_task_reports`: Auto-executed dependencies
   - Properties: `success`, `total_duration`
   - Methods: `summary()`, `save_report()`

### YAML Plan Format

**Example** (`daily-b3.yaml`):
```yaml
name: daily-b3
description: Daily B3 market data download
defaults:
  refdate: "2026-03-23:"  # open-ended → up to yesterday
  calendar: B3
  force: false

tasks:
  - template: b3-bvbg087
  - template: b3-bvbg086
  - template: b3-cotahist-daily

  # Per-task arg override with integer range
  - template: b3-indexes-historical-prices
    args:
      year: "2026"
    force: false  # override default reprocess setting
```

### Argument Resolution

**Function**: `resolve_plan_args()` (lines 323-370)

Supports:
1. **Passthrough**: Non-string values pass unchanged
2. **Integer ranges**: `"2020:2023"` → `[2020, 2021, 2022, 2023]`
3. **Symbol resolution**: `"symbols:index"` → calls `get_symbols("index")`
4. **DSL resolution**: Same as CLI `--arg` (dates, symbols, lists)

**Note**: `refdate` is handled separately; not included in `resolve_plan_args()`

### Refdate Smart Injection

**Function**: `_template_requires_refdate()` (lines 373-386)

**Purpose**: Only inject `refdate` into templates that declare it in their downloader args

**Implementation**:
```python
template = retrieve_template(template_name)
return "refdate" in template.downloader.args
```

**Priority Order** (in `_resolve_task_refdate()`, lines 413-440):
1. `refdate_override` (CLI `--date` flag) — highest
2. Task-level `args.refdate`
3. Plan defaults `refdate`
4. `None` if not specified

### Execution Flow

**Function**: `execute_download_plan()` (lines 500-582)

**For each task**:
1. Merge defaults + task args (task wins on conflict)
2. Resolve refdate with priority ordering
3. Resolve remaining args (symbols, ranges), excluding refdate
4. Smart-inject refdate only if template declares it
5. Call `_execute_task()` which invokes `download_marketdata()`
6. Continue on error (one failure ≠ abort plan)

**Status Legend** (printed to stderr):
```
.(passed) F(failed) E(error) S(skipped) D(duplicated) I(invalid) C(corrupted)
```

**Report Collection**:
- Direct task reports → `plan_report.task_reports`
- Dependency reports (ETL upstream) → `plan_report.implicit_task_reports`
- Deduplication: same dependency only stored once

---

## 3. Download & Process Workflow

### Download Operation

**Function**: `download_marketdata()` (lines 242-356 in `api.py`)

**Key Parameters**:
- `template_name`: Template to download
- `force`: Force re-download, bypass cache checks
- `verbosity`: Output level (QUIET, NORMAL, VERBOSE)
- `report_file`: Optional path to save report
- `**kwargs`: Template-specific args (e.g., `refdate`, `index`)

**Workflow**:
1. **Dependency Resolution**: Inject missing args from upstream (e.g., if a template needs `index` from `b3-indexes-composition`, auto-execute it)
2. **Iteration**: Create cartesian product of all arg combinations via `KwargsIterator`
   - Single template can trigger multiple downloads
3. **Per-iteration**:
   - Check `_should_download()` based on cache state
   - Download and save to metadata
   - Build TaskResult (status, duration, files, warnings)
4. **Status Mapping**:
   - None → PASSED (.)
   - DownloadException → FAILED (F)
   - DuplicatedFolderException → DUPLICATED (D)
   - InvalidContentException → INVALID (I)
   - CorruptedContentException → CORRUPTED (C)
   - Other exceptions → ERROR (E)
   - Skipped (already cached) → SKIPPED (S)

**Skip Logic** (`_should_download()`, lines 39-89):
- If `force=True`: Always download (remove cache entry first)
- If last status `D` (DUPLICATED): Skip unless raw files missing
- If last status `I` (INVALID): Skip (permanent)
- If no successful trial and no cached metadata: Download
- Else: Skip

### Process Operation

**Function**: `process_marketdata()` (lines 377-522 in `api.py`)

**Key Parameters**:
- `template_name`: Template to process
- `reprocess`: Force reprocess even if already processed
- `max_workers`: Parallel workers for file I/O (default: 4)
- `meta_id`: Process only specific cache entry (optional)

**Workflow**:
1. Query metadata DB for all downloads of this template
2. For each metadata entry:
   - Check if already processed (skip unless `reprocess=True`)
   - Read raw files via `_read_marketdata()`
   - Convert to parquet
   - Update metadata with `is_processed=True`
3. Uses ThreadPoolExecutor for parallel I/O, lock for serialized DB writes
4. Touch `.last_processed` marker if any passed

**Status**:
- PASSED: Successfully processed
- SKIPPED: Already processed (unless `reprocess=True`)
- FAILED/ERROR: Processing error

---

## 4. Progress Reporting System

**Location**: `/home/wilson/dev/python/brasa/brasa/engine/reporting.py`

### Status Enums

**DownloadAttemptStatus** (lines 33-96):
- PASSED → "." (green)
- FAILED → "F" (red)
- ERROR → "E" (red bold)
- SKIPPED → "S" (yellow)
- DUPLICATED → "D" (cyan)
- INVALID → "I" (magenta)
- CORRUPTED → "C" (yellow)
- WARNING → "W" (yellow)

**TaskStatus** (lines 168-212):
- Mirror of DownloadAttemptStatus for report integration

### TaskResult (lines 224-274)

Captures single operation outcome:
```python
@dataclass
class TaskResult:
    status: TaskStatus
    operation: str  # "download", "process", "etl"
    template_name: str
    args: dict[str, Any]
    duration_seconds: float
    error_type: str | None
    error_message: str | None
    error_traceback: str | None
    warnings: list[str]
    downloaded_files: list[str]
    is_processed: bool
    extra_info: dict[str, Any]  # download status codes, HTTP status, retry info
    timestamp: datetime
```

### TaskReport (lines 380-706)

Collects and reports results:

**Properties**:
- `success`: True if no ERROR/FAILED results
- `results`: List of TaskResult objects
- `dependency_reports`: Upstream ETL reports

**Methods**:
- `start(total)`: Initialize progress display
- `add_result(result)`: Add result and update display
- `finish()`: Print report if problems/warnings found
- `save_report(filepath, format='json'|'txt')`: Persist report

**Display**:
- **NORMAL**: `"Download template-name .F.SDS [42/100]"` (symbols every 50, counter at end)
- **VERBOSE**: Per-task status with args summary
- **QUIET**: No output unless problems

**Report Format** (JSON):
```json
{
  "template_name": "b3-cotahist",
  "operation": "download",
  "elapsed_seconds": 45.2,
  "summary": {
    "total": 10,
    "passed": 8,
    "failed": 0,
    "errors": 0,
    "skipped": 2,
    "duplicated": 0,
    "invalid": 0,
    "corrupted": 0,
    "warnings": 0
  },
  "results": [...]
}
```

---

## 5. The `force` Argument in Context

### Where `force` is Used

1. **CLI**: `--force` flag in `download` command (lines 195-198)
   - Passed to `download_marketdata(force=args.force, ...)`

2. **Download Plan**: `force` field in task definition (lines 465-466)
   - Overrides plan defaults per-task
   - Mapped to `reprocess` parameter: `force=task.reprocess`
   - Handled in task execution via `_execute_task()`

3. **API Level**: `force` parameter in `download_marketdata()` and `get_marketdata()`

### Behavior When `force=True`

In `_should_download()` (lines 50-54):
```python
if force:
    if cache.has_meta(meta):
        cache.load_meta(meta)
        cache.remove_meta(meta)  # Clear cache entry
    return True  # Always download
```

### Real-World Example: `b3-indexes-historical-prices`

**Template**: `/home/wilson/dev/python/brasa/templates/b3/indexes/b3-indexes-historical-prices.yaml`

**Properties**:
- Downloader: `brasa.downloaders.b3_url_encoded_download`
- Format: JSON
- Retry: 15 attempts, 3s delay, 2x backoff
- Download delay: 1 second between downloads
- Args: `year`, `index` (both required)
- Dependencies: `index` from `staging.b3-indexes-composition`

**In a plan** (daily-b3.yaml):
```yaml
- template: b3-indexes-historical-prices
  args:
    year: "2026"
  force: false  # Don't force re-download for this year
```

---

## 6. Data Flow Summary

### Single `brasa download b3-cotahist`

```
CLI parsing
  ↓
download_marketdata("b3-cotahist", force=False)
  ├─ Resolve dependencies (if declared)
  ├─ Create KwargsIterator (cartesian product)
  ├─ For each arg combination:
  │  ├─ _should_download() check
  │  ├─ If yes: cache.download_marketdata() → DownloadResult
  │  ├─ Build TaskResult from DownloadResult
  │  └─ Add to TaskReport
  └─ Print progress & report
```

### Plan Execution `brasa download --plan daily-b3.yaml`

```
CLI parsing
  ↓
DownloadPlan.from_file("daily-b3.yaml")
  ├─ Parse YAML → DownloadPlan object
  ├─ Validate all template names exist
  └─ execute_download_plan(plan)
      ├─ For each task:
      │  ├─ Merge defaults + task args
      │  ├─ Resolve refdate (override > task > defaults > None)
      │  ├─ Resolve remaining args (symbols, ranges)
      │  ├─ Smart-inject refdate if template needs it
      │  ├─ _execute_task()
      │  │  └─ Call download_marketdata(force=task.reprocess, ...)
      │  └─ Collect dependency reports
      │
      └─ DownloadPlanReport
          ├─ task_reports: explicit downloads
          ├─ implicit_task_reports: upstream ETL (deduplicated)
          ├─ Print summary with auto count
          └─ Save JSON/TXT report if requested
```

---

## 7. Key Functions & Files Reference

### Core Engine Files
| File | Purpose |
|------|---------|
| `/brasa/engine/api.py` | `download_marketdata()`, `process_marketdata()`, `process_etl()` |
| `/brasa/engine/download_plan.py` | Plan loading, validation, execution |
| `/brasa/engine/reporting.py` | Status enums, TaskResult, TaskReport, progress display |
| `/brasa/engine/download.py` | Low-level download handling (format, validation, compression) |
| `/brasa/engine/cache.py` | CacheManager, metadata persistence |
| `/brasa/cli.py` | CLI parser and command dispatch |

### Key Functions

| Function | File | Purpose |
|----------|------|---------|
| `execute_download_plan()` | download_plan.py:500 | Plan execution orchestrator |
| `download_marketdata()` | api.py:242 | Download single/multiple combinations |
| `process_marketdata()` | api.py:377 | Process downloads to parquet |
| `_should_download()` | api.py:39 | Cache skip logic |
| `resolve_plan_args()` | download_plan.py:323 | Resolve symbols/ranges in plan args |
| `_template_requires_refdate()` | download_plan.py:373 | Smart refdate injection check |

### Utility Classes

| Class | File | Purpose |
|-------|------|---------|
| `DownloadPlan` | download_plan.py:73 | Plan structure |
| `DownloadPlanReport` | download_plan.py:176 | Plan execution report |
| `TaskReport` | reporting.py:380 | Operation report (download/process) |
| `TaskResult` | reporting.py:224 | Single operation outcome |
| `KwargsIterator` | util.py:182 | Cartesian product of args |
| `DownloadArgs` | util.py:56 | Canonical download args container |

---

## 8. Smart Update Feature Implications

### For "Smart Update"

**Recommended Integration Points**:

1. **At plan execution level** (`execute_download_plan()`):
   - Intercept task execution in `_execute_task()`
   - Check staleness before invoking `download_marketdata()`
   - Conditionally set `force=True` if update needed

2. **At download level** (`download_marketdata()`):
   - Before `_should_download()` check
   - Calculate if data needs refresh based on:
     - Last successful download timestamp
     - Data source update frequency
     - Staleness threshold

3. **Via plan defaults**:
   - Add `smart_update: true` to plan YAML
   - Pass to execution via new parameter
   - Apply logic uniformly across all tasks

**Key Observation**: The download plan already supports:
- Per-task `force` override
- Task-level argument resolution
- Dependency tracking
- Progress reporting at plan level

→ **Architecture fits naturally with existing plan execution model**

---

## 9. Files Explored

### Main Implementation
- `/home/wilson/dev/python/brasa/brasa/engine/api.py`
- `/home/wilson/dev/python/brasa/brasa/engine/download_plan.py`
- `/home/wilson/dev/python/brasa/brasa/engine/reporting.py`
- `/home/wilson/dev/python/brasa/brasa/engine/download.py`
- `/home/wilson/dev/python/brasa/brasa/cli.py`
- `/home/wilson/dev/python/brasa/brasa/util.py`

### Templates & Examples
- `/home/wilson/dev/python/brasa/daily-b3.yaml`
- `/home/wilson/dev/python/brasa/templates/b3/indexes/b3-indexes-historical-prices.yaml`

### Tests
- `/home/wilson/dev/python/brasa/tests/test_download_plan.py` (comprehensive plan tests)
- `/home/wilson/dev/python/brasa/tests/test_download_args.py`
- `/home/wilson/dev/python/brasa/tests/test_download_status.py`
