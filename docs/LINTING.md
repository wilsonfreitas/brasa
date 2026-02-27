# Linting and Code Quality Commands

This document covers the main commands for code linting, formatting, and pre-commit hooks in the brasa project.

## Ruff - Linter & Formatter

### Check for Linting Issues

```bash
# Check entire project
uv run ruff check .

# Check specific directory
uv run ruff check brasa/

# Check specific file
uv run ruff check brasa/engine.py

# Show statistics of issues found
uv run ruff check brasa/ --statistics
```

### Auto-fix Linting Issues

```bash
# Fix all auto-fixable issues
uv run ruff check . --fix

# Fix and exit with error if changes were made
uv run ruff check . --fix --exit-non-zero-on-fix
```

### Format Code

```bash
# Format entire project
uv run ruff format .

# Format specific directory
uv run ruff format brasa/

# Check formatting without making changes (dry-run)
uv run ruff format . --check

# Show diff of what would change
uv run ruff format . --diff
```

### Combined Lint + Format

```bash
# Fix linting issues and format code
uv run ruff check . --fix && uv run ruff format .
```

## Pre-commit Hooks

### Installation

```bash
# Install pre-commit hooks (run once after cloning)
uv run pre-commit install
```

### Running Pre-commit

```bash
# Run on all files
uv run pre-commit run --all-files

# Run on staged files only (default behavior on commit)
uv run pre-commit run

# Run specific hook
uv run pre-commit run ruff --all-files
uv run pre-commit run ruff-format --all-files

# Run on specific files
uv run pre-commit run --files brasa/engine.py brasa/api.py
```

### Managing Pre-commit

```bash
# Update hooks to latest versions
uv run pre-commit autoupdate

# Clean pre-commit cache
uv run pre-commit clean

# Uninstall hooks
uv run pre-commit uninstall
```

### Bypass Pre-commit (Emergency Only)

```bash
# Skip pre-commit hooks for a single commit (use sparingly!)
git commit --no-verify -m "your message"
```

## VS Code Integration

With the current VS Code settings, the following happens automatically:

- **Format on Save**: Files are formatted when you save
- **Fix on Save**: Auto-fixable linting issues are resolved on save
- **Organize Imports**: Imports are sorted on save

## Configuration Files

| File | Purpose |
|------|---------|
| `pyproject.toml` | Ruff configuration (rules, line length, excludes) |
| `.pre-commit-config.yaml` | Pre-commit hooks configuration |
| `.vscode/settings.json` | VS Code editor settings |

## Common Ruff Rules

| Code | Description |
|------|-------------|
| `E` | pycodestyle errors |
| `W` | pycodestyle warnings |
| `F` | Pyflakes (unused imports, undefined names) |
| `I` | isort (import sorting) |
| `B` | flake8-bugbear (common bugs) |
| `UP` | pyupgrade (Python version upgrades) |
| `SIM` | flake8-simplify (code simplification) |
| `PTH` | flake8-use-pathlib (use pathlib instead of os.path) |

## Troubleshooting

### Ignore a specific line

```python
x = 1  # noqa: E501
```

### Ignore a specific rule for a block

```python
# ruff: noqa: F401
from module import unused_import
```

### Ignore unused function arguments

Prefix with underscore:

```python
def callback(_unused_arg, used_arg):
    return used_arg
```
