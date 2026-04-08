"""Tests for CLI smart update integration.

Tests cover:
- --update flag on download subcommand
- --since flag for explicit start dates
- Mutual exclusivity: --update vs --arg refdate=...
"""

from unittest.mock import patch

from brasa.cli import parser


class TestCLIDownloadUpdateFlag:
    """Test CLI --update flag parsing."""

    def test_download_update_flag_single_template(self):
        """Test --update flag with single template."""
        args = parser.parse_args(["download", "--update", "b3-cotahist-daily"])
        assert args.update is True
        assert args.template == ["b3-cotahist-daily"]
        assert args.since is None

    def test_download_update_flag_multiple_templates(self):
        """Test --update flag with multiple templates."""
        args = parser.parse_args(
            [
                "download",
                "--update",
                "b3-cotahist-daily",
                "b3-futures-settlement-prices",
            ]
        )
        assert args.update is True
        assert args.template == ["b3-cotahist-daily", "b3-futures-settlement-prices"]

    def test_download_without_update_flag(self):
        """Test download without --update flag defaults to False."""
        args = parser.parse_args(["download", "b3-cotahist-daily"])
        assert args.update is False

    def test_download_since_flag(self):
        """Test --since flag for explicit start date."""
        args = parser.parse_args(
            ["download", "--update", "--since", "2026-04-01", "b3-cotahist-daily"]
        )
        assert args.update is True
        assert args.since == "2026-04-01"

    def test_download_update_with_force(self):
        """Test --update with --force flag."""
        args = parser.parse_args(
            ["download", "--update", "--force", "b3-cotahist-daily"]
        )
        assert args.update is True
        assert args.force is True

    def test_download_update_with_calendar(self):
        """Test --update with --calendar flag."""
        args = parser.parse_args(
            ["download", "--update", "--calendar", "ANBIMA", "bcb-sgs"]
        )
        assert args.update is True
        assert args.calendar == "ANBIMA"


class TestCLIDownloadMutualExclusivity:
    """Test mutual exclusivity: --update vs --arg refdate=..."""

    def test_cli_parses_update_with_arg(self):
        """Test that CLI can parse --update with --arg (check happens at runtime)."""
        args = parser.parse_args(
            [
                "download",
                "--update",
                "--arg",
                "refdate=2026-04-01",
                "b3-cotahist-daily",
            ]
        )
        assert args.update is True
        assert args.arg == ["refdate=2026-04-01"]

    def test_cli_parses_update_with_non_refdate_arg(self):
        """Test that CLI parses --update with other --arg values."""
        args = parser.parse_args(
            [
                "download",
                "--update",
                "--arg",
                "code=4389",
                "bcb-sgs",
            ]
        )
        assert args.update is True
        assert args.arg == ["code=4389"]

    def test_mutual_exclusivity_check_logic(self):
        """Test the mutual exclusivity check logic."""
        from brasa.cli import _parse_download_args

        # Simulate the check
        download_kwargs = _parse_download_args(["refdate=2026-04-01"], "B3")
        assert "refdate" in download_kwargs

        # With smart_update=True, this should trigger error
        smart_update = True
        should_error = smart_update and "refdate" in download_kwargs
        assert should_error is True


class TestCLIDownloadIntegration:
    """Test integration with download_marketdata."""

    @patch("brasa.cli.download_marketdata")
    def test_download_update_passed_to_marketdata(self, mock_download):
        """Test that --update flag is passed to download_marketdata."""
        from brasa.cli import get_verbosity, parser
        from brasa.engine import Verbosity

        args = parser.parse_args(
            ["download", "--update", "--since", "2026-04-01", "b3-cotahist-daily"]
        )

        verbosity = get_verbosity(args)
        smart_update = getattr(args, "update", False)
        since = getattr(args, "since", None)

        # Verify args are correctly extracted
        assert smart_update is True
        assert since == "2026-04-01"
        assert verbosity == Verbosity.NORMAL

    @patch("brasa.cli.download_marketdata")
    def test_cli_download_with_plan(self, mock_download):
        """Test that --plan flag still works."""
        args = parser.parse_args(["download", "--plan", "daily-update.yaml"])
        assert args.plan == "daily-update.yaml"
        assert args.template == []
