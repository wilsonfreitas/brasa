# GitHub Copilot Instructions

## Priority Guidelines

When generating code for this repository:

1. **Version Compatibility**: Use only Python and dependency features that match the versions defined in [pyproject.toml](pyproject.toml).
2. **Context Files**: Prioritize instructions in `.github/instructions/*.instructions.md` and this file.
3. **Codebase Patterns**: When instructions are silent, scan similar modules for established patterns and follow the most consistent ones.
4. **Architectural Consistency**: Keep the modular library layout used in `brasa/` (engine, downloaders, parsers, readers, queries).
5. **Code Quality**: Favor maintainability and testability as seen in current modules and tests.

## Technology Versions (Observed)

- **Python**: ^3.10 (Ruff target version `py310`)
- **Package Manager**: uv
- **Linter/Formatter**: Ruff (line length 88, double quotes)
- **Type Checker**: mypy
- **Testing**: pytest

Key dependencies (see [pyproject.toml](pyproject.toml)):

- pandas ^2.0.0
- numpy ^2.0.0
- pyarrow ^19.0.0
- duckdb ^1.2.0
- lxml ^4.9.2
- beautifulsoup4 ^4.12.2
- pyyaml ^6.0
- python-bcb ^0.3.2

## Context Files

Follow these instruction files when they apply:

- [.github/instructions/python.instructions.md](.github/instructions/python.instructions.md)
- [.github/instructions/templates.instructions.md](.github/instructions/templates.instructions.md)
- [.github/instructions/r.instructions.md](.github/instructions/r.instructions.md)
- [.github/instructions/python-mcp-server.instructions.md](.github/instructions/python-mcp-server.instructions.md)

## Architecture and Module Boundaries

The project is a modular Python library with a template-driven ETL flow:

- `brasa/engine`: cache, templates, orchestration
- `brasa/downloaders`: HTTP/API download clients
- `brasa/readers`: file readers
- `brasa/parsers`: parsing implementations
- `brasa/queries`: DuckDB query interfaces
- `brasa/etl.py`: transformation routines using pandas/pyarrow

Keep changes within these boundaries; follow existing flows from templates -> downloaders/readers/parsers -> engine -> queries.

## Codebase Patterns (Examples)

Use these observed patterns as references:

- **Docstrings with Args/Returns**: See [brasa/fieldsets/field.py](brasa/fieldsets/field.py#L1-L120).
- **Module-level docstrings**: See [brasa/engine/core.py](brasa/engine/core.py#L1-L16).
- **Custom exceptions**: See [brasa/engine/exceptions.py](brasa/engine/exceptions.py#L1-L20).
- **pytest tests and skips**: See [tests/test_templates.py](tests/test_templates.py#L1-L120).
- **ETL with pandas/pyarrow**: See [brasa/etl.py](brasa/etl.py#L1-L160).

## Coding Standards (Observed)

- **Imports**: Standard library, third-party, then local (e.g., [brasa/etl.py](brasa/etl.py#L1-L20)).
- **Docstrings**: Use triple-quoted docstrings; common style is Google-style with `Args:` and `Returns:` blocks (e.g., [brasa/fieldsets/field.py](brasa/fieldsets/field.py#L20-L110)).
- **Type hints**: Present in many core modules (e.g., [brasa/engine/core.py](brasa/engine/core.py#L20-L70)) but not universal; match the surrounding file style and add type hints in new code where practical.
- **Formatting**: Use Ruff format settings (line length 88, double quotes) from [pyproject.toml](pyproject.toml#L43-L140).
- **Logging**: Use the standard `logging` module when needed (e.g., [brasa/downloaders/downloaders.py](brasa/downloaders/downloaders.py#L1-L120)).

## Error Handling

- Use the custom exceptions defined in [brasa/engine/exceptions.py](brasa/engine/exceptions.py#L1-L20) when signaling download/cache/content issues.
- Raise errors with descriptive messages; preserve exception context with `from e` when wrapping (see [brasa/fieldsets/field.py](brasa/fieldsets/field.py#L60-L90)).

## Testing Approach

- Tests are written with pytest and named `test_*.py` and `test_*()` (see [tests/test_templates.py](tests/test_templates.py#L1-L120)).
- Use `pytest.mark.skip` for external or unstable dependencies when needed (see [tests/test_templates.py](tests/test_templates.py#L30-L90)).

## Templates

- Template configuration is pipeline-based. Follow the rules in [.github/instructions/templates.instructions.md](.github/instructions/templates.instructions.md).
- Full specification lives in [docs/TEMPLATES.md](docs/TEMPLATES.md).

## Tooling Commands

Always run commands via uv:

```bash
uv run python cli-ei.py
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy brasa/
```

## Public API Surface

Public API exports are in [brasa/__init__.py](brasa/__init__.py#L1-L60). When adding new public functions, update `__all__` consistently.
