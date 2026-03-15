# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**brasa** is a Python library for extracting, processing, and querying financial market data from Brazilian institutions (B3, ANBIMA, Tesouro Direto, CVM, BCB). It uses a template-driven ETL architecture where YAML templates in `templates/` declaratively define download, parsing, transformation, and storage pipelines.

## Important: Always Use uv

This project uses **uv** for dependency management. **Every** Python command must be run through `uv run` — never use bare `python`, `pytest`, `ruff`, or `mypy` directly.

## Common Commands

```bash
# Install dependencies
uv sync

# Run all tests
uv run pytest

# Run tests without internet connection (skip integration tests)
uv run pytest --no-integration

# Run a single test file
uv run pytest tests/test_templates.py

# Run a specific test
uv run pytest tests/test_templates.py::test_function_name -v

# Linting
uv run ruff check .
uv run ruff check . --fix

# Formatting
uv run ruff format .

# Type checking
uv run mypy brasa/

# Pre-commit hooks
uv run pre-commit run --all-files
```

## Architecture

### Data Flow

```
YAML Template → Download → Cache (raw/) → Parse/Read → Parquet (db/) → ETL → Query
```

1. **Templates** (`templates/*.yaml`): Declarative pipeline configs defining how to download, parse, and store each dataset. Use pipeline-based templates (not legacy function-based).
2. **Downloaders** (`brasa/downloaders/`): HTTP/API clients that fetch raw data files.
3. **Cache** (`brasa/engine/`): `CacheManager` singleton manages file storage, metadata (SQLite), and checksum-based deduplication. Cache lives in `.brasa-cache/` (override with `BRASA_DATA_PATH` env var).
4. **Parsers/Readers** (`brasa/parsers/`, `brasa/readers/`): Parse raw files (CSV, FWF, XML, JSON) into DataFrames.
5. **Field System** (`brasa/fieldsets/`): Declarative type system for fields — `date(format='%Y%m%d')`, `numeric(dec=2.0)`, `character`, `category`. Adapters for pandas and pyarrow.
6. **Storage Layers** in `.brasa-cache/db/`: `input/` (raw parsed) → `staging/` (intermediate) → `curated/` (analytical).
7. **ETL** (`brasa/etl.py`): Transformations that load from one layer and write to another.
8. **Queries** (`brasa/queries.py`): Query interface via PyArrow datasets, helper functions (`get_returns`, `get_prices`, `get_symbols`), and DuckDB (`BrasaDB`).

### Public API

Exports are in `brasa/__init__.py` (`__all__`). Core workflow:
```python
from brasa import download_marketdata, process_marketdata, process_etl, get_marketdata
```

### Template Types

- **Single dataset**: `reader.pipeline` + `fields` block
- **Multi-dataset**: `reader.pipeline` + `datasets` block (each with its own `fields`)
- **ETL**: `etl.pipeline` that loads from upstream datasets

### Key Patterns

- `CacheManager` is a singleton — always use `CacheManager()` to get the instance
- Templates use `KwargsIterator` to expand download parameters (e.g., date ranges)
- Parquet files are partitioned (typically by `refdate`) for efficient querying
- Custom exceptions in `brasa/engine/exceptions.py` for download/cache/content errors

## Code Style

- **Ruff** for linting and formatting (line-length 88, double quotes, target py310)
- Google-style docstrings with `Args:` and `Returns:` blocks
- Type hints in new code
- Imports: stdlib → third-party → local
- Field names in templates: `snake_case`
- All commands run through `uv run`

## Testing

- pytest with `pythonpath = ["."]` configured in pyproject.toml
- `conftest.py` sets `BRASA_DATA_PATH` to a temp directory (session-scoped) to isolate tests from real cache
- Tests that require an internet connection are marked `@pytest.mark.integration` and can be skipped with `--no-integration`
- Use `pytest.mark.skip` for tests depending on external/unstable APIs (broken endpoints, resources no longer available)
- Tests live in `tests/test_*.py`

## Definition of Done — MANDATORY

**A task is NOT complete until all three checks pass.** This is non-negotiable:

1. **Tests pass**: `uv run pytest`
2. **Ruff passes**: `uv run ruff check . && uv run ruff format --check .`
3. **Pre-commit passes**: `uv run pre-commit run --all-files`

Never consider a task finished if any of these fail. Fix all issues before declaring the work done.

## Plans

Plans must be saved in `docs/plans/`. Filenames must include the creation date and a meaningful title in kebab-case: `YYYY-MM-DD-<title>.md`. Example: `2026-03-14-refactor-cache-manager.md`.

## Data Sources

B3 (equities, futures, options, indexes), ANBIMA (fixed income, curves), BCB/SGS (CDI, SELIC, IPCA, FX), CVM (company filings), Tesouro Direto (treasury bonds).
