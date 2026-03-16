"""Tests for the --arg DSL value parser."""

from datetime import datetime
from unittest.mock import patch

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
        r2 = parse_arg_value("@2026-01", default_calendar="ANBIMA")
        # Different calendars may produce different date lists
        # Just verify both resolve without error
        assert hasattr(r1, "__iter__")
        assert hasattr(r2, "__iter__")


class TestSymbolPrefix:
    @patch("brasa.queries.get_symbols", return_value=["IBOV", "SMLL"])
    def test_symbol_lookup(self, mock_get_symbols):
        result = parse_arg_value("$index")
        assert result == ["IBOV", "SMLL"]
        mock_get_symbols.assert_called_once_with("index")

    @patch("brasa.queries.get_symbols", return_value=["ABEV", "PETR"])
    def test_symbol_lookup_company(self, mock_get_symbols):
        result = parse_arg_value("$company")
        assert result == ["ABEV", "PETR"]
        mock_get_symbols.assert_called_once_with("company")
