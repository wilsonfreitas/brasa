# Download Plans

A **download plan** is a YAML file that declares a set of templates to
download in one command. It replaces ad-hoc scripts with a reproducible,
version-controllable configuration.

```bash
# Run a plan
brasa download --plan daily-b3.yaml

# Override dates from the command line
brasa download --plan daily-b3.yaml -d 2026-01 --calendar B3

# Save an aggregate report
brasa download --plan daily-b3.yaml --report report.json
```

---

## Plan file format

```yaml
# daily-b3.yaml
name: daily-b3
description: Daily B3 market data download

defaults:                         # merged into every task
  refdate: "2026-01-01:"          # open-ended → up to yesterday
  calendar: B3
  reprocess: false

tasks:
  # refdate comes from defaults
  - template: b3-bvbg087
  - template: b3-bvbg028
  - template: b3-cotahist-daily

  # no refdate needed — template doesn't declare it
  - template: b3-listed-stock-etfs
  - template: b3-indexes-composition

  # dynamic symbol resolution
  - template: b3-indexes-theoretical-portfolio
    args:
      index: "symbols:index"

  # per-task args with integer range and reprocess override
  - template: b3-indexes-historical-prices
    args:
      index: "symbols:index"
      year: "2020:2026"
    reprocess: true
```

### Top-level fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Human-readable identifier shown in reports |
| `description` | no | Free-text description (informational only) |
| `defaults` | no | Values merged into every task (task args take precedence) |
| `tasks` | yes | Ordered list of tasks to execute |

### `defaults` block

| Field | Default | Description |
|-------|---------|-------------|
| `refdate` | `null` | DateRangeParser string (see [Refdate strings](#refdate-strings)) |
| `calendar` | `"B3"` | Business calendar used when parsing refdate strings |
| `reprocess` | `false` | Force re-download even if data is cached |

### Task fields

| Field | Required | Description |
|-------|----------|-------------|
| `template` | yes | Template name (must exist in `brasa/files/templates/`) |
| `args` | no | Extra arguments passed to `download_marketdata` |
| `reprocess` | no | Overrides `defaults.reprocess` for this task |

---

## Refdate strings

The `refdate` field accepts the same strings as the CLI `--date` / `-d` flag,
parsed by `DateRangeParser`:

| String | Meaning |
|--------|---------|
| `"2026"` | All business days in 2026 |
| `"2026-03"` | All business days in March 2026 |
| `"2026-03-01"` | Single date |
| `"2025:2026"` | All business days from 2025 to 2026 |
| `"2026-01-01:"` | From 2026-01-01 up to yesterday (open end) |
| `"2026-01-01:today"` | Same as above — `today` normalises to an open end |
| `"2025-06-01:2026-03-01"` | Explicit date range |

The `calendar` field controls which business calendar is used:

| Value | Calendar |
|-------|----------|
| `B3` | B3 trading calendar (default) |
| `ANBIMA` | ANBIMA banking calendar |
| `actual` | Calendar days (no holiday filtering) |

---

## Refdate resolution priority

When a task runs, brasa determines the effective `refdate` using this priority
order (highest to lowest):

1. **CLI `--date`** — overrides everything when provided
2. **Task `args.refdate`** — per-task value in the YAML
3. **`defaults.refdate`** — plan-wide default

```bash
# Plan says refdate: "2025:"
# CLI overrides it to a single month:
brasa download --plan daily-b3.yaml -d 2026-01
```

---

## Smart refdate injection

`refdate` is only forwarded to a template if that template's downloader
actually declares it as an argument. Templates that do not use `refdate`
(e.g. `b3-indexes-composition`) simply ignore it — no error is raised.

This means you can safely put everything in one plan and let brasa do the
right thing:

```yaml
defaults:
  refdate: "2026:"

tasks:
  - template: b3-cotahist-daily          # receives refdate ✓
  - template: b3-indexes-composition     # refdate silently skipped ✓
```

---

## Dynamic argument resolution

### `symbols:<type>` — runtime symbol lookup

```yaml
args:
  index: "symbols:index"
```

At runtime this calls `get_symbols("index")` and passes the resulting list
as the `index` argument. Any symbol type accepted by `get_symbols` works:
`"symbols:equity"`, `"symbols:etf"`, `"symbols:reit"`, etc.

### `"<int>:<int>"` — integer ranges

```yaml
args:
  year: "2020:2026"
```

Expands to `[2020, 2021, 2022, 2023, 2024, 2025, 2026]`. Combined with a
template that iterates over `year`, this downloads all seven years in one
task entry.

---

## CLI integration

### Flags

| Flag | Description |
|------|-------------|
| `--plan FILE` | Path to a download plan YAML file |
| `-d / --date` | Override `defaults.refdate` for all tasks |
| `--calendar` | Override `defaults.calendar` for date parsing |
| `-v / --verbose` | Verbose output — one line per download |
| `-q / --quiet` | Quiet — only print on errors |
| `--report FILE` | Save aggregate report (`.json` → JSON, anything else → TXT) |

`--plan` and template names are **mutually exclusive**. Providing both is an
error.

### Examples

```bash
# Basic: use dates from the plan
brasa download --plan daily-b3.yaml

# Override the plan's refdate with a specific month
brasa download --plan daily-b3.yaml -d 2026-03

# Use a different calendar
brasa download --plan daily-b3.yaml -d 2026-03 --calendar ANBIMA

# Verbose output + save report
brasa download --plan daily-b3.yaml -v --report /tmp/report.json

# Quiet mode — only shows failures
brasa download --plan daily-b3.yaml -q
```

---

## Output and reporting

### Progress display

Each template produces its own progress line, identical to a regular
`brasa download` call:

```
Status legend: .(passed) F(failed) E(error) S(skipped) D(duplicated) I(invalid) C(corrupted)
Download b3-bvbg087 .......... [10/20]..........  [20/20] (3.2s)
Download b3-cotahist-daily ..................FF [20/20] (12.3s)
Download b3-listed-stock-etfs . [1/1] (0.5s)
```

### Plan summary

After all tasks complete, an aggregate summary is printed:

```
════════════════════════════════════════════════════════════
 PLAN SUMMARY — daily-b3
════════════════════════════════════════════════════════════
  b3-bvbg087                                20 passed
  b3-cotahist-daily                         18 passed, 2 failed
  b3-listed-stock-etfs                      1 passed
  b3-indexes-historical-prices              45 passed

Download plan 'daily-b3': 4 templates in 5m 23.4s
Overall: 3 templates ok, 1 with failures
```

### JSON report

Pass `--report report.json` to save the full results:

```json
{
  "plan_name": "daily-b3",
  "total_duration": 323.4,
  "success": false,
  "tasks": [
    {
      "template": "b3-bvbg087",
      "results": [
        {
          "status": "passed",
          "operation": "download",
          "template_name": "b3-bvbg087",
          "args": { "refdate": "2026-03-01 00:00:00" },
          "duration_seconds": 0.8,
          ...
        }
      ]
    }
  ]
}
```

Pass `--report report.txt` to save the plan summary as plain text instead.

---

## Error handling

### Fail-fast validation (before execution)

Before any download starts, brasa validates the plan:

- Missing `name` or empty `tasks` → error
- Unknown template name → error listing all unrecognised templates

```bash
brasa download --plan my-plan.yaml
# Error: Unknown template: 'b3-nonexistent'
```

Fix: check `brasa/files/templates/` for the correct name or run:

```bash
uv run python -m brasa.cli deps --list   # or inspect brasa/files/templates/ directory
```

### Continue-on-error (during execution)

Tasks are **independent** — a failure in one task never stops the remaining
tasks. Errors are captured in the per-template `TaskReport` and surfaced in
the summary.

---

## Python API

Download plans are also accessible programmatically:

```python
from brasa import DownloadPlan, execute_download_plan
from brasa.engine import Verbosity

# Load and validate
plan = DownloadPlan.from_file("daily-b3.yaml")
errors = plan.validate()
if errors:
    raise ValueError("\n".join(errors))

# Execute
report = execute_download_plan(
    plan,
    verbosity=Verbosity.NORMAL,
    report_file="report.json",
)

print(report.success)          # True / False
print(report.total_duration)   # seconds
print(report.summary())        # formatted text
```

### Override refdate programmatically

```python
from brasa.util import DateRange

period = DateRange(year=2026, calendar="B3")

report = execute_download_plan(
    plan,
    refdate_override=period,   # overrides defaults.refdate
)
```

### Inspect the report

```python
for template, task_report in report.task_reports.items():
    passed  = sum(1 for r in task_report.results if r.status.value == "passed")
    failed  = sum(1 for r in task_report.results if r.status.value == "failed")
    print(f"{template}: {passed} passed, {failed} failed")
```

### Build a plan in code

```python
from brasa.engine.download_plan import (
    DownloadPlan,
    DownloadPlanDefaults,
    DownloadPlanTask,
)

plan = DownloadPlan(
    name="ad-hoc",
    description="",
    defaults=DownloadPlanDefaults(refdate="2026-01:", calendar="B3"),
    tasks=[
        DownloadPlanTask(template="b3-bvbg087"),
        DownloadPlanTask(template="b3-cotahist-daily"),
        DownloadPlanTask(
            template="b3-indexes-historical-prices",
            args={"index": ["IBOV", "IFIX"], "year": [2024, 2025, 2026]},
            reprocess=True,
        ),
    ],
)
```

### Resolve args manually

```python
from brasa.engine.download_plan import resolve_plan_args

# symbols:<type> and integer ranges are expanded
resolved = resolve_plan_args({
    "index": "symbols:index",
    "year": "2020:2024",
})
# resolved == {"index": [...], "year": [2020, 2021, 2022, 2023, 2024]}
```

---

## Best practices

### Keep plan files in version control

Store `.yaml` plan files alongside your project code so download
configurations are reproducible and reviewable:

```
my-project/
├── plans/
│   ├── daily-b3.yaml
│   └── monthly-anbima.yaml
└── ...
```

### Use open-ended refdate for daily plans

```yaml
defaults:
  refdate: "2026-01-01:"   # downloads everything from that date to yesterday
```

Each run will pick up only the dates not yet cached.

### Pin specific dates for historical backfills

```yaml
defaults:
  refdate: "2020:2025"
  reprocess: false         # skip already-downloaded dates
```

### Separate templates that need reprocess from those that don't

```yaml
tasks:
  - template: b3-cotahist-daily          # cached, skip if present
  - template: b3-indexes-composition     # same
  - template: b3-indexes-historical-prices
    args:
      year: "2020:2026"
    reprocess: true                      # always re-fetch this one
```

### Validate before scheduling

Run validation without executing to catch typos early:

```python
plan = DownloadPlan.from_file("daily-b3.yaml")
errors = plan.validate()
for e in errors:
    print(e)
```
