# CLI Download `--arg` DSL Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the `-d`/`--date` flag on `brasa download` with a generic `--arg KEY=VALUE` system that uses a prefix-based DSL for typed value resolution.

**Architecture:** A standalone `parse_arg_value()` function in `brasa/util.py` handles the DSL parsing. The CLI collects `--arg` pairs, resolves each value through the parser, and passes the resulting dict as `**kwargs` to `download_marketdata()`. The `-d`/`--date` flag is removed; dates are now expressed as `--arg refdate=@2026-01`.

**Tech Stack:** Python argparse, `DateRangeParser` (already in `brasa/util.py`), `get_symbols()` (in `brasa/queries.py`).

---

## DSL Reference

| Prefix | Type | Example Value | Resolves To |
|--------|------|---------------|-------------|
| `@` | Date/range | `@2026-01-01` | `datetime(2026, 1, 1)` |
| `@` | Date/range | `@2026-01` | `DateRangeParser("B3").parse("2026-01")` |
| `@` | Date/range | `@2026-01-01:2026-01-31` | `DateRange(...)` |
| `@` | Date/range | `@2026-01~ANBIMA` | `DateRangeParser("ANBIMA").parse("2026-01")` |
| `$` | Symbol lookup | `$index` | `get_symbols("index")` |
| `$` | Symbol lookup | `$company` | `get_symbols("company")` |
| *(none, numeric)* | Integer | `2026` | `2026` |
| *(none)* | Plain string | `IBOV` | `"IBOV"` |

**Comma rule:** Values with commas become lists. Each element is individually parsed through the DSL. `IBOV,BOVA11` → `["IBOV", "BOVA11"]`, `2024,2025` → `[2024, 2025]`.

**Calendar:** `--calendar B3` (default) sets the default calendar for all `@` prefixed values. The `~CALENDAR` suffix on a specific value overrides the default for that value only.

---

## Task 1: Add `parse_arg_value()` to `brasa/util.py`

**Files:**
- Modify: `brasa/util.py` (append new function after `DateRangeParser` class, line 220)
- Create: `tests/test_arg_dsl.py`

### Step 1: Write failing tests for the DSL parser

Create `tests/test_arg_dsl.py` with tests covering all DSL cases. The function signature is:

```python
parse_arg_value(value: str, default_calendar: str = "B3") -> Any
```

Tests to write:

```python
"""Tests for the --arg DSL value parser."""

from datetime import datetime
from unittest.mock import patch

import pytest

from brasa.util import parse_arg_value


class TestPlainStrings:
    def test_plain_string(self):
        assert parse_arg_value("IBOV") == "IBOV"

    def test_plain_string_lowercase(self):
        assert parse_arg_value("pt-br") == "pt-br"

    def test_empty_string(self):
        assert parse_arg_value("") == ""


class TestIntegers:
    def test_positive_integer(self):
        result = parse_arg_value("2026")
        assert result == 2026
        assert isinstance(result, int)

    def test_zero(self):
        result = parse_arg_value("0")
        assert result == 0
        assert isinstance(result, int)


class TestCommaLists:
    def test_string_list(self):
        assert parse_arg_value("IBOV,BOVA11") == ["IBOV", "BOVA11"]

    def test_integer_list(self):
        assert parse_arg_value("2024,2025,2026") == [2024, 2025, 2026]

    def test_mixed_list(self):
        assert parse_arg_value("IBOV,2026") == ["IBOV", 2026]

    def test_single_element_no_list(self):
        """No comma means no list, just a scalar."""
        assert parse_arg_value("IBOV") == "IBOV"


class TestDatePrefix:
    def test_single_date(self):
        result = parse_arg_value("@2026-03-06")
        assert result == [datetime(2026, 3, 6)]

    def test_date_range_month(self):
        result = parse_arg_value("@2026-01")
        # DateRangeParser returns a DateRange for month patterns
        assert hasattr(result, "__iter__")
        dates = list(result)
        assert len(dates) > 0
        assert all(isinstance(d, datetime) for d in dates)

    def test_date_range_explicit(self):
        result = parse_arg_value("@2026-01-01:2026-01-03")
        assert hasattr(result, "__iter__")

    def test_calendar_override(self):
        result = parse_arg_value("@2026-01~ANBIMA")
        assert hasattr(result, "__iter__")
        dates = list(result)
        assert len(dates) > 0

    def test_default_calendar_used(self):
        r1 = parse_arg_value("@2026-01", default_calendar="B3")
        r2 = parse_arg_value("@2026-01", default_calendar="actual")
        # Different calendars may produce different date lists
        # Just verify both resolve without error
        assert hasattr(r1, "__iter__")
        assert hasattr(r2, "__iter__")


class TestSymbolPrefix:
    @patch("brasa.util.get_symbols", return_value=["IBOV", "SMLL"])
    def test_symbol_lookup(self, mock_get_symbols):
        result = parse_arg_value("$index")
        assert result == ["IBOV", "SMLL"]
        mock_get_symbols.assert_called_once_with("index")

    @patch("brasa.util.get_symbols", return_value=["ABEV", "PETR"])
    def test_symbol_lookup_company(self, mock_get_symbols):
        result = parse_arg_value("$company")
        assert result == ["ABEV", "PETR"]
        mock_get_symbols.assert_called_once_with("company")
```

### Step 2: Run tests to verify they fail

```bash
uv run pytest tests/test_arg_dsl.py -v
```

Expected: FAIL — `ImportError: cannot import name 'parse_arg_value' from 'brasa.util'`

### Step 3: Implement `parse_arg_value()`

Add to `brasa/util.py` after line 220 (end of `DateRangeParser` class):

```python
def parse_arg_value(value: str, default_calendar: str = "B3"):
    """Parse a CLI --arg value using the prefix DSL.

    Prefixes:
        @  — date or date range, parsed by DateRangeParser.
             Optional ~CALENDAR suffix overrides the default calendar.
        $  — symbol lookup via get_symbols().
        (none, numeric) — integer.
        (none) — plain string.

    Commas split the value into a list; each element is parsed individually.
    A single element (no commas) returns a scalar, not a one-element list.

    Args:
        value: The raw string value from the CLI.
        default_calendar: Default calendar for @ date values.

    Returns:
        Parsed value: str, int, datetime, list, or DateRange.
    """
    # Date prefix — handled before comma splitting (dates contain no commas)
    if value.startswith("@"):
        date_str = value[1:]
        calendar = default_calendar
        if "~" in date_str:
            date_str, calendar = date_str.rsplit("~", 1)
        return DateRangeParser(calendar).parse(date_str)

    # Symbol prefix — handled before comma splitting (symbol names contain no commas)
    if value.startswith("$"):
        from brasa.queries import get_symbols

        return get_symbols(value[1:])

    # Comma-separated list
    if "," in value:
        return [_parse_scalar(v) for v in value.split(",")]

    return _parse_scalar(value)


def _parse_scalar(value: str):
    """Parse a single scalar value: integer if numeric, else string."""
    try:
        return int(value)
    except ValueError:
        return value
```

### Step 4: Run tests to verify they pass

```bash
uv run pytest tests/test_arg_dsl.py -v
```

Expected: all PASS

### Step 5: Run ruff

```bash
uv run ruff check brasa/util.py tests/test_arg_dsl.py && uv run ruff format brasa/util.py tests/test_arg_dsl.py
```

### Step 6: Commit

```bash
git add brasa/util.py tests/test_arg_dsl.py
git commit -m "feat: add parse_arg_value() DSL for CLI --arg values"
```

---

## Task 2: Wire `--arg` into the CLI download command

**Files:**
- Modify: `brasa/cli.py:167-196` (parser definition) and `brasa/cli.py:705-768` (download handler)
- Modify: `tests/test_cli.py` (add/update `TestDownloadCommand`)

### Step 1: Write failing tests for CLI arg parsing

Add to `tests/test_cli.py` in the `TestDownloadCommand` class:

```python
class TestDownloadCommand:
    """Tests for the download CLI command parser."""

    def test_download_command_parser_exists(self) -> None:
        args = cli.parser.parse_args(["download", "b3-cotahist-daily"])
        assert args.command == "download"

    def test_download_command_force_default_false(self) -> None:
        args = cli.parser.parse_args(["download", "b3-cotahist-daily"])
        assert args.force is False

    def test_download_command_force_flag(self) -> None:
        args = cli.parser.parse_args(["download", "--force", "b3-cotahist-daily"])
        assert args.force is True

    def test_download_single_arg(self) -> None:
        args = cli.parser.parse_args([
            "download", "b3-bvbg087", "--arg", "refdate=@2026-01-01"
        ])
        assert args.arg == ["refdate=@2026-01-01"]

    def test_download_multiple_args(self) -> None:
        args = cli.parser.parse_args([
            "download", "tpl",
            "--arg", "index=IBOV",
            "--arg", "year=2026",
        ])
        assert args.arg == ["index=IBOV", "year=2026"]

    def test_download_no_args_defaults_empty(self) -> None:
        args = cli.parser.parse_args(["download", "tpl"])
        assert args.arg is None

    def test_download_calendar_default(self) -> None:
        args = cli.parser.parse_args(["download", "tpl"])
        assert args.calendar == "B3"

    def test_download_no_date_flag(self) -> None:
        """The old -d/--date flag should no longer exist."""
        with pytest.raises(SystemExit):
            cli.parser.parse_args(["download", "tpl", "-d", "2026-01"])
```

### Step 2: Run tests to verify they fail

```bash
uv run pytest tests/test_cli.py::TestDownloadCommand -v
```

Expected: FAIL — `args` has no attribute `arg`, and `-d` still works.

### Step 3: Update CLI parser and handler

**Parser changes** in `brasa/cli.py` — replace the `-d`/`--date` argument with `--arg`:

Remove lines 168-174 (`-d`/`--date` argument). Add:

```python
parser_download.add_argument(
    "--arg",
    action="append",
    metavar="KEY=VALUE",
    help=(
        "download argument as KEY=VALUE. Repeatable. "
        "Prefixes: @=date (@2026-01, @2026-01-01:2026-01-31, @2026-01~ANBIMA), "
        "$=symbols ($index, $company). "
        "Commas create lists (IBOV,BOVA11). Bare integers auto-convert."
    ),
)
```

**Handler changes** — replace the `args.date` handling block in the download handler.

Add a helper function `_parse_download_args`:

```python
def _parse_download_args(raw_args: list[str] | None, calendar: str) -> dict:
    """Parse --arg KEY=VALUE pairs into a kwargs dict."""
    if not raw_args:
        return {}
    kwargs = {}
    for item in raw_args:
        if "=" not in item:
            print(f"Error: invalid --arg format: {item!r} (expected KEY=VALUE)",
                  file=sys.stderr)
            sys.exit(1)
        key, value = item.split("=", 1)
        kwargs[key] = parse_arg_value(value, default_calendar=calendar)
    return kwargs
```

Update the download handler (non-plan branch):

```python
else:
    download_kwargs = _parse_download_args(args.arg, args.calendar)
    if verbosity != Verbosity.QUIET:
        print(
            "Status legend: .(passed) F(failed) E(error) "
            "S(skipped) D(duplicated) I(invalid) C(corrupted)"
        )
    for template in templates:
        download_marketdata(
            template,
            force=args.force,
            verbosity=verbosity,
            report_file=report_file,
            **download_kwargs,
        )
```

Update the plan branch similarly — replace `args.date` logic with `_parse_download_args` and pass `refdate` from kwargs if present:

```python
if plan_file:
    from .engine.download_plan import DownloadPlan, execute_download_plan

    plan = DownloadPlan.from_file(plan_file)
    errors = plan.validate()
    if errors:
        for err in errors:
            print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)
    download_kwargs = _parse_download_args(args.arg, args.calendar)
    refdate_override = download_kwargs.pop("refdate", None)
    execute_download_plan(
        plan,
        refdate_override=refdate_override,
        verbosity=verbosity,
        report_file=report_file,
    )
```

Add `parse_arg_value` to the imports at the top of `cli.py`:

```python
from .util import DateRangeParser, parse_arg_value
```

### Step 4: Run tests to verify they pass

```bash
uv run pytest tests/test_cli.py -v
```

Expected: all PASS

### Step 5: Run full check suite

```bash
uv run ruff check . && uv run ruff format --check .
uv run pytest --no-integration
```

### Step 6: Commit

```bash
git add brasa/cli.py tests/test_cli.py
git commit -m "feat: replace -d/--date with --arg KEY=VALUE DSL on download command"
```

---

## Task 3: Update download plan CLI tests

**Files:**
- Modify: `tests/test_download_plan.py:594-610` (`TestCliDownloadPlanArgs`)

### Step 1: Update plan-related CLI tests

The existing `test_plan_with_date_override` test uses `-d`. Update it to use `--arg`:

```python
def test_plan_with_date_override(self):
    args = cli.parser.parse_args([
        "download", "--plan", "plan.yaml",
        "--arg", "refdate=@2026-01",
    ])
    assert args.plan == "plan.yaml"
    assert args.arg == ["refdate=@2026-01"]
```

### Step 2: Run tests

```bash
uv run pytest tests/test_download_plan.py::TestCliDownloadPlanArgs -v
```

### Step 3: Commit

```bash
git add tests/test_download_plan.py
git commit -m "test: update download plan CLI tests for --arg flag"
```

---

## Task 4: Final verification

### Step 1: Run full test suite

```bash
uv run pytest --no-integration
```

### Step 2: Run pre-commit

```bash
uv run pre-commit run --all-files
```

### Step 3: Verify CLI help output

```bash
uv run python -m brasa.cli download --help
```

Verify: no `-d`/`--date` flag, `--arg KEY=VALUE` is documented with DSL description.
