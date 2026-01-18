---
goal: Implement `head` command in brasa CLI to display the first N lines of a dataset
version: 1.0
date_created: 2026-01-13
last_updated: 2026-01-13
owner: brasa-team
status: 'Planned'
tags: [feature, cli, usability]
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

This plan describes the implementation of a new `head` command for the brasa CLI. The command will display the first N rows of a specified dataset, providing a quick way to preview data directly from the command line without writing Python code. This feature mirrors the Unix `head` command behavior and leverages the existing `show` function from `brasa.queries`.

## 1. Requirements & Constraints

- **REQ-001**: The `head` command must accept a dataset name as a required positional argument in the format `layer.dataset` (e.g., `input.b3-cotahist`, `staging.b3-cotahist`)
- **REQ-002**: The `head` command must accept an optional `-n` / `--lines` argument to specify the number of rows to display (default: 10)
- **REQ-003**: The command must support output to various formats: display (default), CSV, JSON, Parquet, Excel
- **REQ-004**: The command must provide meaningful error messages when the dataset is not found
- **REQ-005**: The command must validate that the dataset argument contains a valid layer prefix (input, staging, or other defined layers)
- **CON-001**: Must follow existing CLI patterns established in [brasa/cli.py](brasa/cli.py)
- **CON-002**: Must use existing `get_dataset` function from `brasa.queries`
- **GUD-001**: Follow PEP 8 style guide and project coding conventions
- **GUD-002**: Include type hints for all function parameters and return values
- **GUD-003**: Include docstrings following PEP 257 conventions
- **PAT-001**: Follow the existing subparser pattern used for other commands (download, process, query)

## 2. Implementation Steps

### Implementation Phase 1: Add CLI Subparser for head Command

- GOAL-001: Define the `head` subparser with required arguments and options

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-001 | Add `parser_head` subparser to [brasa/cli.py](brasa/cli.py) after line 81 with name "head" and help text "show first N rows of a dataset" | | |
| TASK-002 | Add required positional argument `dataset` to `parser_head` with help text "dataset name in format layer.dataset (e.g., input.b3-cotahist, staging.b3-cotahist)" | | |
| TASK-003 | Add optional `-n` / `--lines` argument with type=int, default=10, and help text "number of rows to display (default: 10)" | | |
| TASK-004 | Add optional `-o` / `--output` argument with default="display" for output format specification | | |

### Implementation Phase 2: Implement head Command Logic

- GOAL-002: Implement the command execution logic in the main block

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-005 | Add `elif args.command == "head":` branch in the main block of [brasa/cli.py](brasa/cli.py) after line 131 | | |
| TASK-006 | Import `get_dataset` from `.queries` module at the top of [brasa/cli.py](brasa/cli.py) | | |
| TASK-007 | Parse `args.dataset` to extract layer and dataset name by splitting on `.` (e.g., "input.b3-cotahist" → layer="input", dataset="b3-cotahist") | | |
| TASK-008 | Implement dataset loading using `get_dataset(dataset, layer=layer)` | | |
| TASK-009 | Call `.head(args.lines)` on the dataset and convert to pandas DataFrame | | |
| TASK-010 | Implement output handling: display to stdout using `print(df.to_string())` for "display" mode | | |
| TASK-011 | Implement output to file formats: CSV (.csv), JSON (.json), Parquet (.parquet), Excel (.xlsx/.xls) based on `--output` extension | | |

### Implementation Phase 3: Error Handling

- GOAL-003: Add proper error handling for edge cases

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-012 | Add try-except block around dataset loading to catch `FileNotFoundError` or similar exceptions | | |
| TASK-013 | Print user-friendly error message when dataset is not found: "Error: Dataset '{layer}.{name}' not found" | | |
| TASK-014 | Validate dataset argument format contains exactly one `.` separator, print error "Error: Invalid dataset format. Use layer.dataset (e.g., input.b3-cotahist)" if invalid | | |
| TASK-015 | Exit with non-zero status code (1) on error using `sys.exit(1)` | | |
| TASK-016 | Import `sys` module at the top of [brasa/cli.py](brasa/cli.py) if not already present | | |

### Implementation Phase 4: Testing

- GOAL-004: Create unit tests for the head command

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-017 | Create [tests/test_cli.py](tests/test_cli.py) test file | | |
| TASK-018 | Add test `test_head_command_parser_exists` to verify the subparser is registered | | |
| TASK-019 | Add test `test_head_command_default_lines` to verify default value of 10 lines | | |
| TASK-020 | Add test `test_head_command_custom_lines` to verify custom -n value is parsed correctly | | |
| TASK-021 | Add test `test_head_command_output_argument` to verify -o argument is parsed correctly | | |
| TASK-022 | Add test `test_head_command_dataset_format_parsing` to verify layer.dataset format is correctly parsed | | |
| TASK-023 | Add test `test_head_command_invalid_dataset_format` to verify error handling for invalid format (missing layer prefix) | | |

## 3. Alternatives

- **ALT-001**: Use the existing `show` function directly instead of `get_dataset().head()` - Not chosen because `show` returns a styled DataFrame which is not suitable for CLI output and file exports
- **ALT-002**: Implement as a separate script file - Not chosen to maintain consistency with the existing CLI architecture using subparsers
- **ALT-003**: Use `click` library for CLI - Not chosen to maintain consistency with existing `argparse`-based implementation

## 4. Dependencies

- **DEP-001**: `brasa.queries.get_dataset` - Function to load datasets by name
- **DEP-002**: `pandas` - For DataFrame operations and output formatting
- **DEP-003**: `pyarrow.dataset` - Underlying dataset handling (via get_dataset)

## 5. Files

- **FILE-001**: [brasa/cli.py](brasa/cli.py) - Main CLI module where the head command will be added
- **FILE-002**: [tests/test_cli.py](tests/test_cli.py) - New test file for CLI command tests

## 6. Testing

- **TEST-001**: Test that `head` subparser is correctly registered with the argument parser
- **TEST-002**: Test that dataset name is correctly parsed as positional argument in `layer.dataset` format
- **TEST-003**: Test that `-n` / `--lines` argument defaults to 10
- **TEST-004**: Test that `-n 5` correctly sets lines to 5
- **TEST-005**: Test that `-o output.csv` correctly sets output path
- **TEST-006**: Test that `input.b3-cotahist` is correctly parsed into layer="input" and dataset="b3-cotahist"
- **TEST-007**: Test that invalid format without `.` separator produces error message
- **TEST-008**: Integration test with a mock dataset to verify head functionality

## 7. Risks & Assumptions

- **RISK-001**: Large datasets may cause memory issues when loading - Mitigated by using PyArrow's `.head()` method which only reads required rows
- **RISK-002**: Dataset not found errors may have cryptic messages - Mitigated by wrapping in try-except with user-friendly messages
- **ASSUMPTION-001**: Users have already processed datasets before running `head` command
- **ASSUMPTION-002**: The cache directory is properly set up via `brasa setup` command

## 8. Related Specifications / Further Reading

- [brasa/cli.py](brasa/cli.py) - Existing CLI implementation
- [brasa/queries.py](brasa/queries.py) - Dataset query functions including `get_dataset` and `show`
- [docs/USER_GUIDE.md](docs/USER_GUIDE.md) - User documentation (to be updated after implementation)
