# Converge Download refdate Handling

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `--arg refdate=2026-03-18` and plan-based `refdate: "2026-03-18"` follow the same code path, producing identical types at the downloader.

**Architecture:** Normalize refdate from string→datetime at the earliest boundary — `parse_arg_value()` for CLI and `_resolve_task_refdate()` for plans — so `download_marketdata()` always receives a `datetime` (or `list[datetime]` / `DateRange`). This is a single-point fix in `parse_arg_value()` plus a test to lock the invariant.

**Tech Stack:** Python, pytest

---

## Problem Statement

Two invocations that should be equivalent produce different `refdate` types:

| Invocation | refdate type at downloader | Works? |
|---|---|---|
| `--plan bvbg086.yaml` (with `refdate: "2026-03-18"`) | `datetime(2026, 3, 18)` | Yes |
| `--arg refdate=2026-03-18` | `"2026-03-18"` (string) | **Crashes** — `.strftime()` on str |
| `--arg refdate=@2026-03-18` | `[datetime(2026, 3, 18)]` | Yes (but requires knowing `@` prefix) |

The root cause: `parse_arg_value()` in `brasa/util.py` only triggers `DateRangeParser` when the `@` prefix is present. Without `@`, an ISO date string passes through as a plain string. Downloaders call `.strftime()` on refdate, so a string crashes.

## Design Decision

**Option A — Auto-detect ISO dates in `parse_arg_value()`:** If the value matches `YYYY-MM-DD` (or `YYYY-MM-DD:YYYY-MM-DD`), parse it as a date even without `@`. The `@` prefix remains supported but becomes optional for dates.

**Option B — Normalize inside `download_marketdata()`:** Add a type coercion step that converts string refdates to datetime before passing to `KwargsIterator`.

**Chosen: Option A.** It fixes the problem at the entry point (parsing), keeps `download_marketdata()` clean, and makes the CLI more intuitive. The `@` prefix stays as an explicit opt-in for edge cases (e.g., a value that looks like a date but isn't meant as one — unlikely for `refdate`).

## File Structure

| File | Role |
|---|---|
| `brasa/util.py` | Modify `parse_arg_value()` to auto-detect ISO dates |
| `tests/test_util.py` | Add tests for date auto-detection in `parse_arg_value()` |
| `tests/test_cli.py` | Add integration test: `--arg refdate=2026-03-18` produces same type as plan path |

---

### Task 1: Auto-detect ISO dates in `parse_arg_value()`

**Files:**
- Modify: `brasa/util.py:222-260` — `parse_arg_value()`
- Create: `tests/test_util.py`

- [ ] **Step 1: Write failing tests for ISO date auto-detection**

In `tests/test_util.py`, add tests that assert `parse_arg_value("2026-03-18")` returns `[datetime(2026, 3, 18)]` (same as `parse_arg_value("@2026-03-18")`), and that date ranges like `"2026-03-01:2026-03-18"` also auto-detect.

```python
from datetime import datetime
from brasa.util import parse_arg_value


class TestParseArgValueDateAutoDetect:
    """parse_arg_value should auto-detect ISO dates without @ prefix."""

    def test_single_date_without_prefix(self):
        """YYYY-MM-DD without @ should parse as [datetime]."""
        result = parse_arg_value("2026-03-18")
        assert result == [datetime(2026, 3, 18)]

    def test_single_date_matches_at_prefix(self):
        """Result should be identical with or without @ prefix."""
        without = parse_arg_value("2026-03-18")
        with_at = parse_arg_value("@2026-03-18")
        assert without == with_at

    def test_date_range_without_prefix(self):
        """YYYY-MM-DD:YYYY-MM-DD without @ should parse as DateRange."""
        from brasa.util import DateRange
        result = parse_arg_value("2026-03-01:2026-03-05")
        assert isinstance(result, DateRange)

    def test_open_date_range_without_prefix(self):
        """YYYY-MM-DD: without @ should parse as DateRange."""
        from brasa.util import DateRange
        result = parse_arg_value("2026-03-01:")
        assert isinstance(result, DateRange)

    def test_plain_string_still_works(self):
        """Non-date strings should still return as strings."""
        assert parse_arg_value("hello") == "hello"

    def test_integer_still_works(self):
        """Numeric strings should still return as int."""
        assert parse_arg_value("42") == 42

    def test_year_month_without_prefix(self):
        """YYYY-MM without @ should parse as DateRange."""
        from brasa.util import DateRange
        result = parse_arg_value("2026-03")
        assert isinstance(result, DateRange)

    def test_date_with_calendar_suffix(self):
        """YYYY-MM-DD~CALENDAR should auto-detect and use the specified calendar."""
        result = parse_arg_value("2026-03-18~ANBIMA")
        assert result == [datetime(2026, 3, 18)]

    def test_invalid_date_falls_through(self):
        """Invalid dates like 2026-13 should fall through to scalar parsing."""
        result = parse_arg_value("2026-13")
        assert result == "2026-13"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_util.py::TestParseArgValueDateAutoDetect -v`
Expected: `test_single_date_without_prefix` and `test_single_date_matches_at_prefix` FAIL (returns string instead of datetime list).

- [ ] **Step 3: Implement auto-detection in `parse_arg_value()`**

In `brasa/util.py`, add an ISO date detection block **before** the comma-splitting logic but **after** the `@` and `$` prefix checks. Use `DateRangeParser.parse()` which already handles all date formats — just try it, and if it raises, fall through to scalar parsing.

```python
def parse_arg_value(value: str, default_calendar: str = "B3"):
    # Date prefix — handled before comma splitting (dates contain no commas)
    if value.startswith("@"):
        date_str = value[1:]
        calendar = default_calendar
        if "~" in date_str:
            date_str, calendar = date_str.rsplit("~", 1)
        return DateRangeParser(calendar).parse(date_str)

    # Symbol prefix — handled before comma splitting
    if value.startswith("$"):
        from brasa.queries import get_symbols
        return get_symbols(value[1:])

    # Auto-detect ISO dates (YYYY-MM-DD, YYYY-MM, date ranges)
    # Try DateRangeParser first; fall through to scalar on failure.
    if _looks_like_date(value):
        date_str = value
        calendar = default_calendar
        if "~" in date_str:
            date_str, calendar = date_str.rsplit("~", 1)
        try:
            return DateRangeParser(calendar).parse(date_str)
        except Exception:
            pass

    # Comma-separated list
    if "," in value:
        return [_parse_scalar(v) for v in value.split(",")]

    return _parse_scalar(value)
```

Add the `_looks_like_date` helper right above `parse_arg_value`:

```python
import re

_DATE_PATTERN = re.compile(
    r"^\d{4}-\d{2}(?:-\d{2})?(?::(?:\d{4}-\d{2}(?:-\d{2})?)?)?$"
)


def _looks_like_date(value: str) -> bool:
    """Quick check if value could be an ISO date or date range.

    Matches: YYYY-MM, YYYY-MM-DD, YYYY-MM-DD:YYYY-MM-DD, YYYY-MM-DD:, etc.
    Also allows a trailing ~CALENDAR suffix.
    """
    text = value.rsplit("~", 1)[0] if "~" in value else value
    return bool(_DATE_PATTERN.match(text))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_util.py::TestParseArgValueDateAutoDetect -v`
Expected: All PASS.

- [ ] **Step 5: Run full test suite to check for regressions**

Run: `uv run pytest --no-integration -x`
Expected: All PASS. Existing `@`-prefixed tests must still work.

- [ ] **Step 6: Run linting**

Run: `uv run ruff check brasa/util.py tests/test_util.py && uv run ruff format --check brasa/util.py tests/test_util.py`
Expected: Clean.

- [ ] **Step 7: Commit**

```bash
git add brasa/util.py tests/test_util.py
git commit -m "feat: auto-detect ISO dates in CLI --arg values without @ prefix"
```

---

### Task 2: Integration test — CLI and plan paths produce identical refdate types

**Files:**
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write integration test**

This test verifies that both paths feed the same type to `download_marketdata()`. It mocks `download_marketdata` and captures the kwargs.

```python
class TestDownloadRefdateConvergence:
    """CLI --arg and plan paths must produce identical refdate types."""

    def test_cli_arg_refdate_type_matches_plan(self, tmp_path):
        """--arg refdate=YYYY-MM-DD should produce same type as plan refdate."""
        from datetime import datetime
        from unittest.mock import patch, MagicMock
        from brasa.util import parse_arg_value
        from brasa.engine.download_plan import _resolve_task_refdate

        # CLI path: parse_arg_value
        cli_refdate = parse_arg_value("2026-03-18")

        # Plan path: _resolve_task_refdate with string from YAML
        plan_refdate = _resolve_task_refdate(
            {"refdate": "2026-03-18"}, None, "B3"
        )

        assert type(cli_refdate) == type(plan_refdate)
        assert cli_refdate == plan_refdate
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest tests/test_cli.py::TestDownloadRefdateConvergence -v`
Expected: PASS (this is a regression lock — should pass after Task 1).

- [ ] **Step 3: Run full checks**

Run: `uv run pytest --no-integration -x && uv run ruff check . && uv run ruff format --check . && uv run pre-commit run --all-files`
Expected: All clean.

- [ ] **Step 4: Commit**

```bash
git add tests/test_cli.py
git commit -m "test: add convergence test for CLI and plan refdate types"
```

---

## Summary of Changes

| File | Change |
|---|---|
| `brasa/util.py` | Add `_looks_like_date()` + `_DATE_PATTERN`. Modify `parse_arg_value()` to try `DateRangeParser` when input looks like an ISO date. |
| `tests/test_util.py` | 9 new tests for date auto-detection (new file). |
| `tests/test_cli.py` | 1 convergence test locking both paths to same type. |

**Lines changed:** ~30 in production code, ~50 in tests.

**Risk:** Low. The `_looks_like_date` guard is conservative (only matches `YYYY-MM*` patterns), and the `try/except` ensures non-date strings fall through. The `@` prefix continues to work identically.
