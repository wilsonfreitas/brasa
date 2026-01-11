# GitHub Copilot Instructions for Brasa

## Project Overview

Brasa is a Python library for extracting, processing, and managing financial market data from Brazilian financial institutions (B3, ANBIMA, Tesouro Direto, CVM, BCB). It provides a template-driven ETL pipeline with caching, parsing, and querying capabilities.

## Technology Stack

- **Python**: 3.10+
- **Package Manager**: Poetry (always use `poetry run` for commands)
- **Linter/Formatter**: Ruff
- **Type Checker**: mypy
- **Testing**: pytest
- **Pre-commit**: Configured with ruff and standard hooks
- **Data Storage**: Parquet files with DuckDB for querying
- **Key Libraries**: pandas, numpy, pyarrow, duckdb, lxml, beautifulsoup4

## Poetry Usage

Always use Poetry for dependency management and running commands:

```bash
# Install dependencies
poetry install

# Add a new dependency
poetry add <package>

# Add a dev dependency
poetry add --group dev <package>

# Run any Python command
poetry run python <script.py>

# Run tests
poetry run pytest

# Run linting
poetry run ruff check .

# Run formatting
poetry run ruff format .
```

## Code Style Guidelines

### General Python Best Practices

1. **Type Hints**: Always use type hints for function parameters and return values
   ```python
   def process_data(df: pd.DataFrame, date: datetime) -> pd.DataFrame:
       ...
   ```

2. **Docstrings**: Use descriptive docstrings for modules, classes, and functions
   ```python
   def generate_checksum(template: str, args: dict) -> str:
       """Generates a hash for a template and its arguments.

       The hash is used to identify a template and its arguments.
       """
   ```

3. **Imports**:
   - Group imports: standard library, third-party, local
   - Use absolute imports for the `brasa` package
   - isort is configured via ruff

4. **Line Length**: 88 characters (Black-compatible)

5. **Quotes**: Double quotes for strings

6. **Naming Conventions**:
   - `snake_case` for functions and variables
   - `PascalCase` for classes
   - `UPPER_CASE` for constants
   - Prefix unused variables with underscore `_`

### Ruff Configuration

The project uses Ruff with these rule sets enabled:
- `E`, `W`: pycodestyle
- `F`: Pyflakes
- `I`: isort
- `B`: flake8-bugbear
- `C4`: flake8-comprehensions
- `UP`: pyupgrade
- `ARG`: flake8-unused-arguments
- `SIM`: flake8-simplify
- `PTH`: flake8-use-pathlib
- `PL`: Pylint
- `RUF`: Ruff-specific

### Before Committing

```bash
# Run linting and formatting
poetry run ruff check . --fix && poetry run ruff format .

# Or use pre-commit
poetry run pre-commit run --all-files
```

## Project Architecture

### Directory Structure

```
brasa/
├── __init__.py          # Public API exports
├── cli.py               # Command-line interface
├── etl.py               # ETL transformation functions
├── queries.py           # DuckDB query interface
├── util.py              # Utility functions
├── downloaders/         # HTTP/API download clients
├── engine/              # Core engine (cache, templates, processing)
├── fieldset_schema/     # Field schema definitions
├── parsers/             # Data parsers (B3, ANBIMA, etc.)
└── readers/             # Generic file readers

templates/               # YAML configuration files for data sources
tests/                   # pytest test files
notebooks/               # Jupyter notebooks for analysis
```

### Key Patterns

1. **Singleton Pattern**: Used for `CacheManager`
2. **Template-Driven**: YAML templates define data source behavior
3. **Factory Pattern**: `FieldHandlerFactory` for field parsing

### Public API

Main functions exposed via `brasa.__init__`:
- `get_marketdata()`: Retrieve market data with caching
- `download_marketdata()`: Batch download operations
- `process_marketdata()`: Process to parquet format
- `process_etl()`: Run ETL transformations
- `get_dataset()`, `get_prices()`, `get_returns()`: Query functions

## Testing Guidelines

```bash
# Run all tests
poetry run pytest

# Run specific test file
poetry run pytest tests/test_templates.py

# Run with verbose output
poetry run pytest -v

# Run with coverage (if configured)
poetry run pytest --cov=brasa
```

### Test File Naming
- Test files: `test_<module>.py`
- Test functions: `test_<functionality>()`

## Common Tasks

### Adding a New Data Source

1. Create YAML template in `templates/`
2. Implement parser in `brasa/parsers/` if needed
3. Add tests in `tests/`

### Working with DataFrames

- Use pandas for data manipulation
- Convert to pyarrow for storage
- Use DuckDB for analytical queries

### Cache Management

The cache is stored in `.brasa-cache/` (or `$BRASA_DATA_PATH`):
- `raw/`: Downloaded files
- `db/`: Parquet datasets
- `meta/`: SQLite metadata

## Error Handling

Use custom exceptions from `brasa.engine.exceptions`:
- `DownloadException`: For download failures
- `DuplicatedFolderException`: For cache conflicts

## Type Checking

```bash
# Run mypy
poetry run mypy brasa/

# Ignored packages (no stubs available)
# pandas, regexparser, bcb, pyarrow, bizdays
```

## Documentation

- Module docstrings at the top of each file
- Function/class docstrings with parameters and return types
- Update `docs/` for significant changes

## Environment Variables

- `BRASA_DATA_PATH`: Custom cache directory path (default: `.brasa-cache/`)

## Code Review Checklist

- [ ] Type hints added
- [ ] Docstrings present
- [ ] Tests written/updated
- [ ] Linting passes (`poetry run ruff check .`)
- [ ] Formatting applied (`poetry run ruff format .`)
- [ ] No hardcoded values (use constants or config)
- [ ] Proper error handling
- [ ] Uses `pathlib.Path` instead of string paths
