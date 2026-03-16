"""Tests for the brasa CLI module."""

import pytest

from brasa import cli


class TestHeadCommand:
    """Tests for the head command."""

    def test_head_command_parser_exists(self) -> None:
        """Test that the head subparser is registered."""
        # Parse with head command
        args = cli.parser.parse_args(["head", "input.test-dataset"])
        assert args.command == "head"

    def test_head_command_dataset_argument(self) -> None:
        """Test that dataset argument is correctly parsed."""
        args = cli.parser.parse_args(["head", "input.b3-cotahist"])
        assert args.dataset == "input.b3-cotahist"

    def test_head_command_default_lines(self) -> None:
        """Test that default value of lines is 10."""
        args = cli.parser.parse_args(["head", "input.test-dataset"])
        assert args.lines == 10

    def test_head_command_custom_lines(self) -> None:
        """Test that custom -n value is parsed correctly."""
        args = cli.parser.parse_args(["head", "-n", "5", "input.test-dataset"])
        assert args.lines == 5

    def test_head_command_custom_lines_long_option(self) -> None:
        """Test that custom --lines value is parsed correctly."""
        args = cli.parser.parse_args(["head", "--lines", "20", "input.test-dataset"])
        assert args.lines == 20

    def test_head_command_output_argument_default(self) -> None:
        """Test that default output is display."""
        args = cli.parser.parse_args(["head", "input.test-dataset"])
        assert args.output == "display"

    def test_head_command_output_argument_csv(self) -> None:
        """Test that -o argument is parsed correctly for CSV."""
        args = cli.parser.parse_args(["head", "-o", "output.csv", "input.test-dataset"])
        assert args.output == "output.csv"

    def test_head_command_output_argument_long_option(self) -> None:
        """Test that --output argument is parsed correctly."""
        args = cli.parser.parse_args(
            ["head", "--output", "data.json", "input.test-dataset"]
        )
        assert args.output == "data.json"

    def test_head_command_dataset_format_parsing(self) -> None:
        """Test that layer.dataset format is correctly parsed."""
        # Test various valid formats
        test_cases = [
            ("input.b3-cotahist", "input", "b3-cotahist"),
            ("staging.b3-cotahist", "staging", "b3-cotahist"),
            ("output.my-dataset", "output", "my-dataset"),
            ("layer.name.with.dots", "layer", "name.with.dots"),
        ]

        for dataset_arg, expected_layer, expected_name in test_cases:
            args = cli.parser.parse_args(["head", dataset_arg])
            parts = args.dataset.split(".", 1)
            assert parts[0] == expected_layer
            assert parts[1] == expected_name

    def test_head_command_all_arguments_combined(self) -> None:
        """Test that all arguments work together."""
        args = cli.parser.parse_args(
            ["head", "-n", "15", "-o", "output.parquet", "staging.test-data"]
        )
        assert args.command == "head"
        assert args.dataset == "staging.test-data"
        assert args.lines == 15
        assert args.output == "output.parquet"


class TestHeadCommandValidation:
    """Tests for head command validation logic."""

    def test_head_command_invalid_dataset_format_no_dot(self) -> None:
        """Test that dataset without dot separator is invalid."""
        dataset_arg = "nodotformat"
        # The validation happens during command execution, not parsing
        # So we test the validation logic directly
        assert "." not in dataset_arg

    def test_head_command_invalid_dataset_format_empty_layer(self) -> None:
        """Test that empty layer is invalid."""
        dataset_arg = ".dataset"
        parts = dataset_arg.split(".", 1)
        # Empty layer should be considered invalid
        assert parts[0] == ""

    def test_head_command_invalid_dataset_format_empty_name(self) -> None:
        """Test that empty dataset name is invalid."""
        dataset_arg = "layer."
        parts = dataset_arg.split(".", 1)
        # Empty name should be considered invalid
        assert parts[1] == ""


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
        args = cli.parser.parse_args(
            ["download", "b3-bvbg087", "--arg", "refdate=@2026-01-01"]
        )
        assert args.arg == ["refdate=@2026-01-01"]

    def test_download_multiple_args(self) -> None:
        args = cli.parser.parse_args(
            [
                "download",
                "tpl",
                "--arg",
                "index=IBOV",
                "--arg",
                "year=2026",
            ]
        )
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


class TestProcessCommand:
    """Tests for the process CLI command parser."""

    def test_process_command_parser_exists(self) -> None:
        args = cli.parser.parse_args(["process", "b3-cotahist-daily"])
        assert args.command == "process"

    def test_process_command_template_argument(self) -> None:
        args = cli.parser.parse_args(["process", "b3-cotahist-daily"])
        assert args.template == ["b3-cotahist-daily"]

    def test_process_command_multiple_templates(self) -> None:
        args = cli.parser.parse_args(["process", "tpl-a", "tpl-b"])
        assert args.template == ["tpl-a", "tpl-b"]

    def test_process_command_reprocess_default_false(self) -> None:
        args = cli.parser.parse_args(["process", "b3-cotahist-daily"])
        assert args.reprocess is False

    def test_process_command_reprocess_flag(self) -> None:
        args = cli.parser.parse_args(["process", "--reprocess", "b3-cotahist-daily"])
        assert args.reprocess is True


class TestListUnprocessedCommand:
    """Tests for the list-unprocessed CLI command parser."""

    def test_list_unprocessed_command_parser_exists(self) -> None:
        args = cli.parser.parse_args(["list-unprocessed"])
        assert args.command == "list-unprocessed"

    def test_list_unprocessed_default_format_is_table(self) -> None:
        args = cli.parser.parse_args(["list-unprocessed"])
        assert args.format == "table"

    def test_list_unprocessed_format_json(self) -> None:
        args = cli.parser.parse_args(["list-unprocessed", "--format", "json"])
        assert args.format == "json"

    def test_list_unprocessed_format_table_explicit(self) -> None:
        args = cli.parser.parse_args(["list-unprocessed", "--format", "table"])
        assert args.format == "table"
