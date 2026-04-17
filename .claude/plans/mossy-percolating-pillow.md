# WIL-38: Converge download CLI --arg DSL and download plan refdate handling

## Context

CLI `--arg refdate=2026-03-18` passes a string (crashes), while plan YAML `refdate: "2026-03-18"` passes datetime (works). The `@` prefix works but shouldn't be required for obvious dates. Additionally, `@today`/`@yesterday` shortcuts are needed for common workflows.

Linear issue: WIL-38

## DSL Type Resolution

Values are parsed in this order:

1. **`@` prefix** — reinforces datetime type. `@2026-01` forces date parsing even if ambiguous. Supports `~CALENDAR` suffix. Also handles named variables (`@today`, `@yesterday`). `@YYYY` (e.g. `@2025`) → `DateRange` of all business days in that year according to default calendar.
2. **ISO date auto-detect** — bare `YYYY-MM-DD` → `datetime`, `YYYY-MM-DDTHH:MM:SS` → `datetime`, `YYYY-MM-DDTHH:MM:SS.sss` → `datetime`, `YYYY-MM` → `DateRange`. No `@` needed. Bare `YYYY` (e.g. `2025`) is **not** auto-detected as a date — it resolves to `int`. Use `@2025` to get the year as a date range.
3. **`$` prefix** — symbol lookup via `get_symbols()`.
4. **Comma rule** — splits into list, each element parsed individually.
5. **Integer** — bare numeric string → `int`.
6. **String** — fallback, returned as-is.

Key: `YYYY-MM-DD` resolves to a single `datetime` (not a list). The `@` prefix is optional for dates but reinforces intent. `@` + date range patterns (`YYYY-MM`, `YYYY-MM-DD:YYYY-MM-DD`) return `DateRange`.

## Task 1: Auto-detect ISO dates in `parse_arg_value()`

**Files:** `brasa/util.py`, `tests/test_util.py` (new)

1. Add `_DATE_PATTERN` regex matching `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS`, `YYYY-MM-DDTHH:MM:SS.sss`, `YYYY-MM`, and date range variants (`:`-separated)
2. Add `_looks_like_date()` helper before `parse_arg_value()`
3. Add auto-detection block in `parse_arg_value()` after `@`/`$` checks, before comma splitting — try `DateRangeParser.parse()`, fall through on failure
4. Create `tests/test_util.py` with `TestParseArgValueDateAutoDetect`:
   - `YYYY-MM-DD` → `datetime` (single value, not list)
   - `YYYY-MM-DDTHH:MM:SS` → `datetime`
   - `YYYY-MM-DDTHH:MM:SS.sss` → `datetime`
   - `YYYY-MM` → `DateRange`
   - `YYYY-MM-DD:YYYY-MM-DD` → `DateRange`
   - Non-date strings still return as string
   - Integers still return as int
5. Run tests, ruff, verify

## Task 2: Add `@today` and `@yesterday` date variables

**Files:** `brasa/util.py`, `tests/test_arg_dsl.py`

1. In `parse_arg_value()`, inside the `@` prefix branch, check for `today`/`yesterday` before calling `DateRangeParser`
2. `@today` → `datetime.combine(date.today(), datetime.min.time())`
3. `@yesterday` → `datetime.combine(date.today() - timedelta(days=1), datetime.min.time())`
4. Add tests in `tests/test_arg_dsl.py` under new `TestDateVariables` class
5. Run tests, ruff, verify

## Task 3: Converge plan args with CLI DSL

**Files:** `brasa/engine/download_plan.py`

1. Update `resolve_plan_args()` to route string values through `parse_arg_value()` so plan YAML args support the same DSL prefixes
2. Update tests if needed
3. Run tests, ruff, verify

## Task 4: Integration test for type convergence

**Files:** `tests/test_cli.py`

1. Add `TestDownloadRefdateConvergence` — assert `parse_arg_value("2026-03-18")` produces same type/value as plan path `_resolve_task_refdate({"refdate": "2026-03-18"}, None, "B3")`
2. Run full suite + pre-commit

## Verification

```bash
uv run pytest --no-integration -x
uv run ruff check . && uv run ruff format --check .
uv run pre-commit run --all-files
```
