"""_parse_download_args must raise instead of exiting the process (audit Q12.6)."""

from datetime import datetime

import pytest

from brasa.cli import _parse_download_args


def test_parses_key_value_pairs():
    kwargs = _parse_download_args(["refdate=2026-04-01", "code=42"], "B3")
    assert kwargs["refdate"] == [datetime(2026, 4, 1)]
    assert kwargs["code"] == 42


def test_empty_args_returns_empty_dict():
    assert _parse_download_args(None, "B3") == {}
    assert _parse_download_args([], "B3") == {}


def test_invalid_format_raises_value_error():
    with pytest.raises(ValueError, match="KEY=VALUE"):
        _parse_download_args(["not-a-pair"], "B3")


class TestDoctorLastOption:
    def test_days(self):
        from brasa import cli

        args = cli.parser.parse_args(["doctor", "--last", "7"])
        assert args.last == 7

    def test_all_keyword(self):
        from brasa import cli

        args = cli.parser.parse_args(["doctor", "--last", "all"])
        assert args.last == -1

    def test_minus_one_alias(self):
        from brasa import cli

        args = cli.parser.parse_args(["doctor", "--last", "-1"])
        assert args.last == -1

    def test_default_is_30(self):
        from brasa import cli

        args = cli.parser.parse_args(["doctor"])
        assert args.last == 30

    def test_below_minus_one_is_usage_error(self):
        from brasa import cli

        with pytest.raises(SystemExit):
            cli.parser.parse_args(["doctor", "--last", "-5"])

    def test_non_numeric_is_usage_error(self):
        from brasa import cli

        with pytest.raises(SystemExit):
            cli.parser.parse_args(["doctor", "--last", "everything"])
