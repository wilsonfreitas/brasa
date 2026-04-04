"""Tests for parse_arg_value ISO date auto-detection and named date variables."""

from datetime import datetime, timedelta

from brasa.util import DateRange, parse_arg_value


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

    def test_datetime_without_prefix(self):
        """YYYY-MM-DDTHH:MM:SS without @ should parse as datetime."""
        result = parse_arg_value("2026-03-18T10:30:00")
        assert result == datetime(2026, 3, 18, 10, 30, 0)

    def test_datetime_ms_without_prefix(self):
        """YYYY-MM-DDTHH:MM:SS.sss without @ should parse as datetime."""
        result = parse_arg_value("2026-03-18T10:30:00.123")
        assert isinstance(result, datetime)
        assert result.hour == 10
        assert result.minute == 30

    def test_date_range_without_prefix(self):
        """YYYY-MM-DD:YYYY-MM-DD without @ should parse as DateRange."""
        result = parse_arg_value("2026-03-01:2026-03-05")
        assert isinstance(result, DateRange)

    def test_open_date_range_without_prefix(self):
        """YYYY-MM-DD: without @ should parse as DateRange."""
        result = parse_arg_value("2026-03-01:")
        assert isinstance(result, DateRange)

    def test_year_month_without_prefix(self):
        """YYYY-MM without @ should parse as DateRange."""
        result = parse_arg_value("2026-03")
        assert isinstance(result, DateRange)

    def test_date_with_calendar_suffix(self):
        """YYYY-MM-DD~CALENDAR should auto-detect and use the specified calendar."""
        result = parse_arg_value("2026-03-18~ANBIMA")
        assert result == [datetime(2026, 3, 18)]

    def test_plain_string_still_works(self):
        assert parse_arg_value("hello") == "hello"

    def test_integer_still_works(self):
        assert parse_arg_value("42") == 42

    def test_bare_year_is_integer_not_date(self):
        """Bare YYYY (e.g. 2025) should return int, not a date."""
        result = parse_arg_value("2025")
        assert result == 2025
        assert isinstance(result, int)

    def test_at_year_is_date_range(self):
        """@YYYY should return DateRange of business days in that year."""
        result = parse_arg_value("@2025")
        assert isinstance(result, DateRange)
        dates = list(result)
        assert len(dates) > 0
        assert all(d.year == 2025 for d in dates)

    def test_invalid_date_falls_through(self):
        """Invalid dates like 2026-13 should fall through to scalar parsing."""
        result = parse_arg_value("2026-13")
        assert result == "2026-13"


class TestNamedDateVariables:
    """@today and @yesterday should return datetime values."""

    def test_at_today(self):
        result = parse_arg_value("@today")
        today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        assert result == today
        assert isinstance(result, datetime)

    def test_at_yesterday(self):
        result = parse_arg_value("@yesterday")
        yesterday = datetime.today().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)
        assert result == yesterday
        assert isinstance(result, datetime)


class TestDatetimeParsing:
    """DateRangeParser datetime and datetime_ms methods."""

    def test_datetime_via_at_prefix(self):
        result = parse_arg_value("@2026-03-18T10:30:00")
        assert result == datetime(2026, 3, 18, 10, 30, 0)

    def test_datetime_ms_via_at_prefix(self):
        result = parse_arg_value("@2026-03-18T10:30:00.500")
        assert isinstance(result, datetime)
        assert result.hour == 10


class TestRefdateConvergence:
    """CLI --arg and plan paths must produce identical refdate types."""

    def test_cli_arg_refdate_type_matches_plan(self):
        """--arg refdate=YYYY-MM-DD should produce same type as plan refdate."""
        from brasa.engine.download_plan import _resolve_task_refdate

        # CLI path: parse_arg_value
        cli_refdate = parse_arg_value("2026-03-18")

        # Plan path: _resolve_task_refdate with string from YAML
        plan_refdate = _resolve_task_refdate({"refdate": "2026-03-18"}, None, "B3")

        assert type(cli_refdate) is type(plan_refdate)
        assert cli_refdate == plan_refdate

    def test_cli_at_prefix_matches_plan(self):
        """@YYYY-MM-DD should also match plan path."""
        from brasa.engine.download_plan import _resolve_task_refdate

        cli_refdate = parse_arg_value("@2026-03-18")
        plan_refdate = _resolve_task_refdate({"refdate": "2026-03-18"}, None, "B3")

        assert type(cli_refdate) is type(plan_refdate)
        assert cli_refdate == plan_refdate
