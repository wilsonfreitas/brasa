# Implementation Plan: WIL-13 (Smart Update) + WIL-28 (Show Only Unprocessed)

## Executive Summary

This plan combines two tightly related features into a single coherent design. The core insight is that both problems share the same root cause: the download loop iterates over **all** kwargs (including already-cached ones), then checks each one individually. Smart update (WIL-13) is about generating the **right** kwargs for incremental updates, and WIL-28 is about not cluttering output with skipped items. A **pre-filtering** approach solves both problems simultaneously.

---

## Part 1: Design Decisions

### 1.1 Where Should Update Rules Live? — Hybrid (Convention + YAML Override)

**Recommendation: Convention-based with YAML escape hatch.**

Rationale:
- The 84 download templates fall into a small number of structural categories that can be detected automatically from existing YAML fields. Adding an `update:` section to all 84 templates is tedious and error-prone.
- Convention-based inference works for 95%+ of templates. The remaining edge cases can use an optional `update:` override.
- Convention detection uses data already present: `downloader.args`, `downloader.extra-key`, `dependencies`, URL format patterns.

**Template categories and their conventional update strategies:**

| Category | Detection Rule | Strategy | Example |
|----------|---------------|----------|---------|
| **refdate-daily** | `refdate: ~` in args, URL has `%d` or `%m` or `%Y-%m-%d` | Incremental date: from last-downloaded to yesterday | `b3-cotahist-daily` |
| **refdate-yearly** | `refdate: ~` in args, URL has only `%Y` (no `%m`/`%d`) | Incremental year: from last-downloaded-year to current year | `b3-cotahist-yearly` |
| **extra-key-date** | `extra-key: date` or `extra-key: datetime`, no `refdate` in args | Daily refresh: call with no args (extra-key auto-injects today) | `b3-listed-stock-etfs`, `b3-indexes-composition` |
| **dependency-driven** | Has `dependencies:` block | Re-resolve deps; new combos downloaded, existing skipped | `b3-indexes-historical-prices` |
| **extra-key-date + deps** | Has both `extra-key: date` AND `dependencies:` | Daily refresh with dep re-resolution | `b3-company-info`, `b3-cash-dividends` |
| **multi-param** | Multiple nullable args (e.g. `code: ~` + `refdate: ~`) | Incremental on refdate dimension; re-resolve other dims | `bcb-sgs-data` |
| **static/force** | No args, no extra-key, no refdate | Force re-download (`force=True`) | `anbima-index-imab` |

**YAML override syntax** (optional, for edge cases):

```yaml
update:
  strategy: incremental-date   # or: incremental-year, daily-refresh, dependency, force
  calendar: B3                 # override default calendar
  since: last-downloaded       # or: a specific date string
```

This override is only needed when convention detection picks the wrong strategy. We anticipate this will be rare.

### 1.2 How to Determine "Last Downloaded Date"

Query the `cache_metadata` table joined with `download_trials`:

```sql
SELECT MAX(json_extract(download_args, '$.refdate'))
FROM cache_metadata
WHERE template = ?
  AND id IN (
    SELECT DISTINCT cache_id FROM download_trials WHERE status_code = '.'
  )
```

For multi-param templates (e.g., `bcb-sgs-data` with `code` + `refdate`), the query extracts the max refdate across all codes. This works because the date dimension is the one that grows over time.

For `extra-key: date` templates, there is no refdate to query. Instead, check if there is a cache entry where `extra_key` matches today's date. If so, skip; if not, download.

For yearly templates, extract the year from the max refdate.

### 1.3 How to Integrate with Download Plans

Add a special refdate value `since-last` that triggers smart update behavior:

```yaml
# In a download plan YAML
defaults:
  refdate: since-last
  calendar: B3
tasks:
  - template: b3-cotahist-daily
  - template: b3-futures-settlement-prices
  - template: b3-indexes-historical-prices  # dependency-driven, refdate ignored
  - template: b3-listed-stock-etfs           # extra-key:date, refdate ignored
  - template: anbima-index-imab              # static, force=True auto-applied
```

When `refdate: since-last` is encountered for a template:
1. Detect the template category (convention-based)
2. Query cache for last-downloaded date
3. Generate the appropriate DateRange from last+1 to yesterday
4. For non-refdate templates, `since-last` is a no-op (strategy dictated by category)

### 1.4 WIL-28 Integration: Pre-Filtering + Display Options

**Two-pronged approach:**

**A. Pre-filtering in download loop (solves both WIL-13 and WIL-28):**
Before iterating kwargs, query the cache to identify which arg combinations are already downloaded. Filter them out of the iterator. The download loop then only processes truly new items. Result: no skipped entries, no `S` symbols.

**B. Fallback display filtering (for when pre-filtering is not used):**
When `force=False` and pre-filtering is not active (e.g., user manually provides a full date range), add a `show_skipped` option. Default: suppress skipped entries from progress display but still count them in the summary.

---

## Part 2: Architecture — The UpdateStrategy System

### 2.1 New Module: `brasa/engine/update_strategy.py`

This is the heart of the feature. It contains:

```python
class UpdateStrategy(Enum):
    INCREMENTAL_DATE = "incremental-date"
    INCREMENTAL_YEAR = "incremental-year"
    DAILY_REFRESH = "daily-refresh"
    DEPENDENCY_DRIVEN = "dependency"
    FORCE_REFRESH = "force"

@dataclass
class UpdatePlan:
    """Result of resolving an update strategy for a template."""
    strategy: UpdateStrategy
    kwargs: dict          # The resolved kwargs to pass to download_marketdata
    force: bool           # Whether to use force=True
    total_new: int        # Estimated number of new items (for progress display)
    skipped_count: int    # Number of items that were pre-filtered as already cached

def detect_strategy(template: MarketDataTemplate) -> UpdateStrategy:
    """Infer the update strategy from template structure."""

def resolve_update(
    template_name: str,
    calendar: str = "B3",
    since: str | None = None,  # Override: explicit start date
) -> UpdatePlan:
    """Resolve kwargs for an incremental update of a template."""
```

**`detect_strategy()` logic:**

```python
def detect_strategy(template: MarketDataTemplate) -> UpdateStrategy:
    # Check for YAML override first
    update_config = getattr(template, 'update', None)
    if update_config and 'strategy' in update_config:
        return UpdateStrategy(update_config['strategy'])

    has_downloader = template.has_downloader
    if not has_downloader:
        return UpdateStrategy.FORCE_REFRESH  # ETL-only templates

    args = template.downloader.args
    has_refdate = 'refdate' in args and args['refdate'] is None
    has_extra_key = template.downloader._extra_key in ('date', 'datetime')
    has_dependencies = bool(getattr(template, 'dependencies', None))
    url = template.downloader.url or ''

    # Category detection (ordered by specificity)
    if has_refdate:
        # Check URL to distinguish daily vs yearly
        if '%Y' in url and '%m' not in url and '%d' not in url:
            return UpdateStrategy.INCREMENTAL_YEAR
        return UpdateStrategy.INCREMENTAL_DATE

    if has_dependencies and has_extra_key:
        return UpdateStrategy.DAILY_REFRESH  # deps resolve themselves

    if has_dependencies:
        return UpdateStrategy.DEPENDENCY_DRIVEN

    if has_extra_key:
        return UpdateStrategy.DAILY_REFRESH

    # No iterable args, no extra-key => static file
    return UpdateStrategy.FORCE_REFRESH
```

**`resolve_update()` logic (per strategy):**

For **INCREMENTAL_DATE**:
1. Query cache: `SELECT MAX(json_extract(download_args, '$.refdate')) FROM cache_metadata WHERE template = ? AND id IN (SELECT cache_id FROM download_trials WHERE status_code = '.')`
2. Parse the date, add 1 business day => `start_date`
3. Generate `DateRange(start=start_date, calendar=calendar)` (defaults end to yesterday)
4. Return `UpdatePlan(kwargs={'refdate': date_range}, force=False)`
5. If no cache entries exist, fall back to a configurable default lookback (e.g., 30 days)

For **INCREMENTAL_YEAR**:
1. Same cache query but extract year
2. Generate list of years from `last_year` to `current_year`
3. Return `UpdatePlan(kwargs={'refdate': year_dates}, force=False)`

For **DAILY_REFRESH**:
1. Check if today's extra-key already exists in cache
2. If not, return `UpdatePlan(kwargs={}, force=False)` (extra-key auto-injects today)
3. If yes, return `UpdatePlan(kwargs={}, force=False, total_new=0)` (nothing to do)

For **DEPENDENCY_DRIVEN**:
1. Return `UpdatePlan(kwargs={}, force=False)` — dependency resolution + `_should_download` handles everything
2. The dependency resolver re-resolves args, and existing cache entries are naturally skipped

For **FORCE_REFRESH**:
1. Return `UpdatePlan(kwargs={}, force=True)`

### 2.2 Pre-Filtering: `get_uncached_kwargs()`

New function in `brasa/engine/update_strategy.py`:

```python
def get_uncached_kwargs(
    template_name: str,
    kwargs: dict,
    force: bool = False,
) -> tuple[dict, int]:
    """Filter kwargs to only include arg combos not yet cached.

    Returns:
        Tuple of (filtered_kwargs, skipped_count).
        filtered_kwargs has the same structure as input kwargs but
        with cached combos removed from iterable values.
    """
```

Implementation approach:
1. Expand `kwargs` via `KwargsIterator` to get all combos
2. For each combo, build a `CacheMetadata` and check `_should_download(cache, meta, force=False)`
3. Collect only the combos where `_should_download` returns True
4. Reconstruct kwargs dict from the filtered combos (re-grouping iterable values)
5. Return filtered kwargs + count of skipped combos

This is the key function that solves both WIL-13 and WIL-28. When used:
- The download loop only iterates over new items
- Progress display shows only actual downloads (no `S` symbols)
- Summary still reports the skipped count

**Performance consideration:** For large date ranges, this queries the cache for every combo. The `_should_download` function already does DB lookups, but they are individual queries. For very large ranges (1000+ dates), we should batch the cache check:

```python
def _batch_check_cached(
    template_name: str,
    combos: list[dict],
) -> set[str]:
    """Return set of cache IDs that already exist with successful trials."""
    cache = CacheManager()
    template = retrieve_template(template_name)
    # Build all cache IDs in memory
    ids = set()
    for args in combos:
        meta = CacheMetadata(template.id)
        meta.extra_key = template.downloader.extra_key
        meta.download_args = DownloadArgs(args)
        ids.add(meta.id)
    # Single batch query
    with closing(cache.meta_db_connection) as conn:
        placeholders = ','.join('?' * len(ids))
        c = conn.cursor()
        c.execute(
            f"SELECT DISTINCT cache_id FROM download_trials "
            f"WHERE cache_id IN ({placeholders}) AND downloaded = '1'",
            list(ids)
        )
        return {row[0] for row in c.fetchall()}
```

This reduces N individual queries to 1 batch query.

---

## Part 3: File-by-File Changes

### 3.1 New File: `brasa/engine/update_strategy.py`

**Create from scratch.** Contains:
- `UpdateStrategy` enum
- `UpdatePlan` dataclass
- `detect_strategy(template)` — convention-based strategy detection
- `resolve_update(template_name, calendar, since)` — generate kwargs for incremental update
- `get_uncached_kwargs(template_name, kwargs, force)` — pre-filter cached combos
- `_batch_check_cached(template_name, combos)` — batch cache lookup
- `_get_last_downloaded_date(template_name)` — query cache for most recent successful refdate
- `_get_last_downloaded_year(template_name)` — query cache for most recent successful year

### 3.2 Modified: `brasa/engine/api.py`

**Changes to `download_marketdata()`:**

Add a `smart_update` parameter (default `False`):

```python
def download_marketdata(
    template_name: str,
    force: bool = False,
    smart_update: bool = False,    # NEW
    show_skipped: bool = False,    # NEW (WIL-28)
    verbosity: Verbosity = Verbosity.NORMAL,
    report_file: str | Path | None = None,
    **kwargs,
) -> TaskReport:
```

New logic at the top of the function (before the main loop):

```python
    if smart_update and not kwargs and not force:
        from .update_strategy import resolve_update
        plan = resolve_update(template_name)
        kwargs = plan.kwargs
        force = plan.force

    # Pre-filter cached combos (unless force=True)
    skipped_prefiltered = 0
    if not force and kwargs:
        from .update_strategy import get_uncached_kwargs
        kwargs, skipped_prefiltered = get_uncached_kwargs(
            template_name, kwargs, force=False
        )
```

Changes to the main loop:
- When `show_skipped=False` (the new default for NORMAL verbosity), skipped results are still added to the report but marked so `ProgressDisplay` can suppress their symbol.
- Actually, with pre-filtering active, there will be no skipped entries in the loop at all. The `show_skipped` flag is only needed as a fallback when pre-filtering is not active.

Changes to report initialization:
```python
    total_display = len(kwargs_iter)
    report = TaskReport(
        operation="download",
        template_name=template_name,
        verbosity=verbosity,
    )
    report.start(total=total_display, prefiltered_skip_count=skipped_prefiltered)
```

### 3.3 Modified: `brasa/engine/reporting.py`

**Changes to `ProgressDisplay`:**

Add `show_skipped` parameter and `prefiltered_skip_count`:

```python
class ProgressDisplay:
    def __init__(
        self,
        total: int,
        operation: str,
        template_name: str,
        verbosity: Verbosity = Verbosity.NORMAL,
        console: Console | None = None,
        show_skipped: bool = True,          # NEW
        prefiltered_skip_count: int = 0,    # NEW
    ) -> None:
```

In `update()`:
```python
    def update(self, result: TaskResult) -> None:
        self.current += 1
        if self.verbosity == Verbosity.QUIET:
            return
        # WIL-28: suppress skipped symbols unless show_skipped is True
        if result.status == TaskStatus.SKIPPED and not self.show_skipped:
            return  # Still counted, just not displayed
        # ... existing display logic ...
```

In `finish()`, show prefiltered count in the timing line:
```python
    def finish(self) -> None:
        if self.verbosity == Verbosity.QUIET:
            return
        if self.verbosity == Verbosity.NORMAL:
            elapsed = (datetime.now() - self._start_time).total_seconds()
            skip_info = ""
            if self.prefiltered_skip_count > 0:
                skip_info = f", {self.prefiltered_skip_count} cached"
            self.console.print(f" ({elapsed:.1f}s{skip_info})")
```

**Changes to `TaskReport`:**

Add `prefiltered_skip_count` attribute:
```python
class TaskReport:
    def __init__(self, ...):
        ...
        self.prefiltered_skip_count: int = 0  # NEW

    def start(self, total: int, prefiltered_skip_count: int = 0) -> None:
        self.prefiltered_skip_count = prefiltered_skip_count
        ...
        self._progress = ProgressDisplay(
            ...,
            prefiltered_skip_count=prefiltered_skip_count,
        )
```

Update `_print_summary()` to include prefiltered count:
```python
    def _print_summary(self) -> None:
        ...
        if self.prefiltered_skip_count:
            parts.append(
                f"[dim]{self.prefiltered_skip_count} cached[/dim]"
            )
```

### 3.4 Modified: `brasa/engine/download_plan.py`

**Changes to `DownloadPlanDefaults`:**

Add `smart_update` field:
```python
@dataclass
class DownloadPlanDefaults:
    refdate: str | None = None
    calendar: str = "B3"
    reprocess: bool = False
    smart_update: bool = False  # NEW
```

**Changes to `execute_download_plan()`:**

When `defaults.refdate == "since-last"` or `defaults.smart_update == True`:
```python
    for task in plan.tasks:
        ...
        # Smart update: resolve refdate from cache
        use_smart_update = plan.defaults.smart_update or (
            plan.defaults.refdate == "since-last"
        )

        if use_smart_update and 'refdate' not in task.args:
            # Let download_marketdata handle smart_update
            resolved_args.pop('refdate', None)
            plan_report.task_reports[task.template] = _execute_task(
                task, resolved_args, verbosity, smart_update=True
            )
        else:
            # Existing flow
            ...
```

**Changes to `_execute_task()`:**

Add `smart_update` parameter:
```python
def _execute_task(
    task: DownloadPlanTask,
    resolved_args: dict,
    verbosity: Verbosity,
    smart_update: bool = False,  # NEW
) -> TaskReport:
    ...
    return download_marketdata(
        task.template,
        force=task.reprocess,
        smart_update=smart_update,  # NEW
        verbosity=verbosity,
        **resolved_args,
    )
```

### 3.5 Modified: `brasa/cli.py`

**Changes to `download` subcommand:**

Add `--update` flag:
```python
parser_download.add_argument(
    "--update",
    action="store_true",
    help="smart update: auto-detect date range from last download to today",
)
parser_download.add_argument(
    "--show-skipped",
    action="store_true",
    help="show skipped (cached) entries in progress display",
)
```

In the command handler:
```python
    if args.update:
        # Smart update mode: no args needed, auto-detect per template
        for template in templates:
            download_marketdata(
                template,
                smart_update=True,
                show_skipped=getattr(args, 'show_skipped', False),
                verbosity=verbosity,
                report_file=report_file,
            )
    else:
        # Existing flow
        ...
```

Also support `--update` with `--plan`:
```python
    if plan_file:
        plan = DownloadPlan.from_file(plan_file)
        if args.update:
            plan.defaults.smart_update = True
        ...
```

### 3.6 Modified: `brasa/engine/template.py`

**Changes to `MarketDataTemplate.load_template()`:**

Process the optional `update` section:
```python
    elif section_name == "update":
        self.update = section_data  # Store raw dict for detect_strategy()
```

No new classes needed — just store the raw dict.

### 3.7 Modified: `brasa/util.py`

No changes needed. `DateRange`, `KwargsIterator`, and `DateRangeParser` already support everything required.

---

## Part 4: CLI Usage Examples

### Smart update for a single template:
```bash
brasa download --update b3-cotahist-daily
# Auto-detects: last downloaded was 2026-03-28, generates 2026-03-31: to today
# Output: Download b3-cotahist-daily .... [4/4] (2.1s, 847 cached)
```

### Smart update for multiple templates:
```bash
brasa download --update b3-cotahist-daily b3-futures-settlement-prices b3-indexes-composition
```

### Smart update via download plan:
```bash
brasa download --plan daily-update.yaml --update
```

Or in the plan YAML itself:
```yaml
name: daily-update
defaults:
  smart_update: true
tasks:
  - template: b3-cotahist-daily
  - template: b3-futures-settlement-prices-pipeline
  - template: b3-indexes-composition
  - template: b3-listed-stock-etfs
  - template: anbima-index-imab
```

### Force mode still works:
```bash
brasa download --force b3-cotahist-daily --arg refdate=@2026-03
# Ignores smart update, downloads everything
```

### Show skipped entries:
```bash
brasa download b3-cotahist-daily --arg refdate=@2026-03 --show-skipped -v
# Shows all entries including S symbols
```

---

## Part 5: Backward Compatibility

1. **Existing CLI commands** — all unchanged. `--update` and `--show-skipped` are new opt-in flags.
2. **Existing API** — `download_marketdata()` signature adds optional params with defaults that preserve current behavior (`smart_update=False`, `show_skipped=False`). Actually, for WIL-28, the default should change: `show_skipped=False` by default suppresses `S` symbols. This is a **visible behavior change** but is the whole point of WIL-28. To minimize disruption, make this change only in NORMAL verbosity; VERBOSE mode continues to show skipped entries.
3. **Existing download plans** — no changes required. Plans without `smart_update: true` or `refdate: since-last` behave identically.
4. **Existing templates** — no changes required. The convention-based detection reads existing YAML fields.
5. **`force=True`** — completely bypasses smart update and pre-filtering. The `_should_download()` logic is untouched.
6. **Summary line** — still reports total counts including skipped. The change is visual (no `S` flood), not informational.

---

## Part 6: Implementation Sequence

### Phase 1: WIL-28 — Display filtering (independent, can ship first)
1. Add `show_skipped` param to `ProgressDisplay` — suppress `S` symbols when False
2. Add `prefiltered_skip_count` to `TaskReport` and `ProgressDisplay`
3. Add `--show-skipped` CLI flag
4. Default NORMAL verbosity to `show_skipped=False`
5. Write tests for `ProgressDisplay` with show_skipped=False

### Phase 2: Pre-filtering infrastructure
1. Create `brasa/engine/update_strategy.py` with `get_uncached_kwargs()` and `_batch_check_cached()`
2. Integrate into `download_marketdata()` — call pre-filter before main loop
3. Wire `prefiltered_skip_count` through to report
4. Write tests for `get_uncached_kwargs()`

### Phase 3: Smart update strategy detection
1. Implement `detect_strategy()` in `update_strategy.py`
2. Implement `_get_last_downloaded_date()` and `_get_last_downloaded_year()`
3. Implement `resolve_update()` for each strategy
4. Write tests for each strategy

### Phase 4: CLI and download plan integration
1. Add `--update` flag to CLI
2. Add `smart_update` to `DownloadPlanDefaults`
3. Support `refdate: since-last` in plans
4. Process optional `update:` section in template YAML
5. Write integration tests

---

## Part 7: Potential Challenges

1. **Year-based URL detection**: Checking for `%Y` without `%m` in the URL is heuristic. Templates like `b3-otc-trade-information` have `%Y-%m-%d` in the URL, so they correctly fall into daily. But if a new template uses `%Y-%m` (monthly), we would need a new strategy. The YAML override handles this.

2. **First-time download (no cache)**: When `smart_update=True` and there is no cache history, `_get_last_downloaded_date()` returns None. Fallback: use a configurable default lookback (e.g., 30 business days) or prompt the user. Recommendation: use 30 business days as default, configurable via `--since` flag.

3. **Multi-param templates with growing dimensions**: For `bcb-sgs-data` (code + refdate), smart update should only grow the refdate dimension. The code dimension comes from explicit args or dependency resolution. The pre-filter handles this naturally — existing (code, refdate) combos are filtered out.

4. **SQLite batch query size**: For very large template histories (10,000+ entries), the `IN (...)` clause could hit SQLite limits. Mitigate by chunking the batch query (SQLite limit is ~999 params by default).

5. **Race condition**: If two processes run smart update simultaneously, they might both compute the same "since-last" date and try to download the same items. The existing `_should_download()` check in the inner loop provides safety — one will succeed and the other will skip. Pre-filtering is an optimization, not a correctness mechanism.

---

## Part 8: Test Plan

### Unit tests for `update_strategy.py`:
- `test_detect_strategy_refdate_daily` — template with `refdate: ~` and `%d` in URL
- `test_detect_strategy_refdate_yearly` — template with `refdate: ~` and only `%Y` in URL
- `test_detect_strategy_extra_key_date` — template with `extra-key: date`
- `test_detect_strategy_dependency` — template with `dependencies:` block
- `test_detect_strategy_static` — template with no args, no extra-key
- `test_detect_strategy_yaml_override` — template with explicit `update:` section
- `test_resolve_update_incremental_date` — mock cache, verify DateRange output
- `test_resolve_update_no_cache_fallback` — verify 30-day lookback default
- `test_get_uncached_kwargs_filters_cached` — verify filtering
- `test_get_uncached_kwargs_preserves_uncached` — verify passthrough
- `test_batch_check_cached_performance` — verify single query for N combos

### Integration tests:
- `test_smart_update_cli_flag` — end-to-end with `--update`
- `test_show_skipped_suppresses_symbols` — WIL-28 display test
- `test_download_plan_since_last` — plan with `refdate: since-last`
- `test_smart_update_with_force_override` — `--force` beats smart update
