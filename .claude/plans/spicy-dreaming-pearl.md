# Download Plan Feature

## Context

The script `cli-random.py` currently serves as an ad-hoc batch download orchestrator — it calls `download_marketdata()` multiple times with different templates and arguments. This approach is fragile, not reusable, and has no aggregate reporting. The "download-plan" feature replaces this with a declarative YAML file that defines all templates to download with their arguments, integrated into the existing `download` CLI command.

## Plan File Format

```yaml
# my-download-plan.yaml
name: daily-b3
description: Daily B3 market data download

defaults:                            # merged into every task (per-task args override)
  refdate: "2026-01-01:today"        # DateRangeParser syntax
  calendar: B3                       # used when parsing refdate strings
  reprocess: false

tasks:
  # refdate comes from defaults
  - template: b3-bvbg087
  - template: b3-bvbg028
  - template: b3-cotahist-daily
  - template: b3-otc-trade-information
  - template: b3-economic-indicators-fwf
  - template: b3-trades-intraday

  # no args needed (defaults.refdate ignored — template doesn't require it)
  - template: b3-listed-fixed-income-etfs
  - template: b3-listed-stock-etfs
  - template: b3-listed-reits
  - template: b3-indexes-composition

  # dynamic symbol resolution
  - template: b3-indexes-theoretical-portfolio
    args:
      index: "symbols:index"

  # per-task args with dynamic symbols and year range
  - template: b3-indexes-historical-prices
    args:
      index: "symbols:index"
      year: "2020:2026"           # expands to [2020, 2021, ..., 2026]
    reprocess: true
```

Key conventions:
- **`defaults` block**: Merged into every task. `refdate` and `calendar` define the date range. Per-task `args` override defaults.
- **`refdate` string parsing**: Uses `DateRangeParser` syntax (`"2026"`, `"2024:2026"`, `"2024-01"`, `"2024-01-01:today"`, etc.). `calendar` controls which business calendar to use (default: `B3`).
- **`symbols:<type>` prefix**: Resolves to `brasa.get_symbols(type)` at runtime.
- **Smart injection**: `refdate` from defaults is only injected into templates that actually require it (i.e., template has `refdate: ~` in `downloader.args`). Templates without `refdate` in their args ignore it.
- **CLI `--date` override**: When provided, CLI `--date` overrides `defaults.refdate` for all tasks.
- **Plan files**: Always require explicit paths (no default search directory).

### Refdate resolution priority (highest to lowest):
1. CLI `--date` (overrides everything)
2. Per-task `args.refdate` in the plan YAML
3. `defaults.refdate` in the plan YAML

## CLI Integration

Add `--plan` flag to the existing `download` command:

```bash
# Use dates from the plan YAML
brasa download --plan my-plan.yaml

# Override plan dates with CLI --date
brasa download --plan my-plan.yaml -d 2026-01 --calendar B3

# With verbosity and report
brasa download --plan my-plan.yaml -v --report report.json
```

### Changes to `brasa/cli.py`

1. Add `--plan` argument to `parser_download` (around line 166-181)
2. Change `template` from `nargs="+"` to `nargs="*"` (make optional when `--plan` is used)
3. Add validation: either `--plan` or template names required, not both
4. Add plan execution branch in handler (around line 612-630)

## New File: `brasa/engine/download_plan.py`

### Data Structures

```python
@dataclass
class DownloadPlanTask:
    template: str
    args: dict[str, Any]       # per-task args (override defaults)
    reprocess: bool

@dataclass
class DownloadPlanDefaults:
    refdate: str | None        # DateRangeParser string, e.g. "2026-01-01:today"
    calendar: str              # business calendar name (default: "B3")
    reprocess: bool            # default reprocess flag (default: False)

@dataclass
class DownloadPlan:
    name: str
    description: str
    defaults: DownloadPlanDefaults
    tasks: list[DownloadPlanTask]

    @classmethod
    def from_file(cls, path: str | Path) -> "DownloadPlan"

    @classmethod
    def from_dict(cls, data: dict) -> "DownloadPlan"

    def validate(self) -> list[str]
        # Validate template names exist, return error messages
```

### Aggregate Report

```python
@dataclass
class DownloadPlanReport:
    plan_name: str
    task_reports: dict[str, TaskReport]  # template -> TaskReport
    success: bool  # True if no ERROR/FAILED in any report
    total_duration: float

    def summary(self) -> str
        # Per-template one-line summaries + overall totals

    def save_report(self, filepath, format="json")
        # JSON: nested {plan_name, tasks: [{template, report}]}
        # TXT: aggregate summary text
```

Follows `OrchestratorReport` pattern from `brasa/engine/orchestrator.py`.

### Execution Function

```python
def execute_download_plan(
    plan: DownloadPlan,
    refdate_override: list[datetime] | DateRange | None = None,  # from CLI --date
    verbosity: Verbosity = Verbosity.NORMAL,
    report_file: str | Path | None = None,
) -> DownloadPlanReport:
```

For each task:
1. **Merge defaults into task args**: `defaults` dict is merged with per-task `args` (task args win on conflict)
2. **Resolve refdate**: Priority: CLI `--date` > task `args.refdate` > `defaults.refdate`. Parse the string via `DateRangeParser(calendar)`.
3. **Resolve `symbols:` args** via `get_symbols()`
4. **Smart injection**: Only inject `refdate` if template actually requires it (check `template.downloader.args` for `refdate: ~`)
5. Call `download_marketdata(template, reprocess=task.reprocess, verbosity=verbosity, **resolved_args)`
6. Collect the returned `TaskReport` into `DownloadPlanReport`
7. Continue on error (tasks are independent, unlike ETL pipelines)

### Argument Resolution

```python
def resolve_plan_args(args: dict, calendar: str = "B3") -> dict:
    # "symbols:index"  -> get_symbols("index")
    # refdate string   -> DateRangeParser(calendar).parse(refdate_str)
    # "2020:2026"      -> [2020, 2021, 2022, 2023, 2024, 2025, 2026]  (integer range)
    # static values    -> pass through unchanged
```

**Resolution rules for string values:**
- `"symbols:<type>"` — resolves via `get_symbols(type)`
- `"<int>:<int>"` (e.g., `"2020:2026"`) — expands to integer list range(start, end+1)
- `refdate` strings — parsed via `DateRangeParser(calendar)` (reuses `brasa/util.py:159-212`)
- Everything else — passed through as-is

Smart injection checks `template.downloader.args` for `refdate: ~` (None = required arg) via `MarketDataDownloader.args` (template.py:362).

## Files to Modify

| File | Change |
|------|--------|
| `brasa/engine/download_plan.py` | **New** — DownloadPlanTask, DownloadPlan, DownloadPlanReport, execute_download_plan, resolve_plan_args |
| `brasa/cli.py` | Add `--plan` arg, make `template` optional, add plan execution branch |
| `brasa/engine/__init__.py` | Export new classes/functions |
| `brasa/__init__.py` | Export to public API |
| `tests/test_download_plan.py` | **New** — unit tests |

## Error Handling

- **Fail fast** (before execution): invalid YAML, missing `name`/`tasks`, unknown template names, template requires `refdate` but no `defaults.refdate` or CLI `--date` provided
- **Continue on error** (during execution): symbol resolution failure or download failure for one task does not stop others
- Validation via `plan.validate()` checks all template names exist

## Aggregate Report Display

```
Status legend: .(passed) F(failed) E(error) S(skipped) D(duplicated) I(invalid) C(corrupted)
Download b3-bvbg087 .......... [10/20]..........  [20/20] (3.2s)
Download b3-cotahist-daily ..................FF [20/20] (12.3s)
Download b3-listed-stock-etfs . [1/1] (0.5s)
...

══════════════════════════ PLAN SUMMARY ═══════════════════════════
Download plan 'daily-b3': 14 templates in 5m 23.4s

  b3-bvbg087                  20 passed
  b3-cotahist-daily           18 passed, 2 failed
  b3-listed-stock-etfs        1 passed
  b3-indexes-historical-prices  45 passed
  ...

Overall: 12 templates ok, 2 with failures
```

Individual template reports (failures, errors, warnings) are displayed by `TaskReport.finish()` as they already are — no changes needed to the existing report system.

## Testing

All tests in `tests/test_download_plan.py`, no network required (mock `download_marketdata` and `get_symbols`):

1. Plan parsing from dict and file (valid/invalid inputs)
2. Plan validation (valid/invalid template names)
3. Argument resolution (`symbols:` prefix, static passthrough)
4. Refdate auto-injection logic
5. Plan execution with mocked downloads (success, partial failure, continue-on-error)
6. Report properties (success, duration, summary text)
7. Report saving (JSON structure, TXT format)
8. CLI argument parsing (`--plan` flag, mutual exclusion with templates)

## Verification

```bash
# Run tests
uv run pytest tests/test_download_plan.py -v

# Lint and format
uv run ruff check brasa/engine/download_plan.py
uv run ruff format --check brasa/engine/download_plan.py

# Full check
uv run pre-commit run --all-files
```
