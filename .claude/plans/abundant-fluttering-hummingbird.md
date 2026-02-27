# Migrate from Poetry to uv

## Context

The project currently uses Poetry for dependency management and packaging. The goal is to migrate to **uv** — a faster, modern Python package manager that follows PEP 621 standards. This affects `pyproject.toml` (core config), the lock file, and 25+ files containing `poetry run` / `poetry install` references.

---

## Step 1: Rewrite `pyproject.toml`

**File:** `pyproject.toml`

### 1a. Replace build system

```toml
# FROM:
[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"

# TO:
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### 1b. Replace `[tool.poetry]` with PEP 621 `[project]`

Convert metadata and all dependencies. Poetry's `^` specifiers must become `>=X.Y.Z,<NEXT` (PEP 508 doesn't support `^`).

Conversion rules:
- `^X.Y.Z` (X>0) → `>=X.Y.Z,<(X+1).0.0`
- `^0.Y.Z` (Y>0) → `>=0.Y.Z,<0.(Y+1).0`

```toml
[project]
name = "brasa"
version = "0.0.1"
description = "Python library to extract finance market data from brazillian financial institutions: B3, ANBIMA, Tesouro Direto, CVM."
authors = [
    { name = "wilsonfreitas", email = "wilson.freitas@gmail.com" },
]
license = "MIT"
readme = "README.md"
requires-python = ">=3.10"
dependencies = [
    "lxml>=4.9.2,<5.0.0",
    "bizdays>=1.0.15,<2.0.0",
    "pandas>=2.0.0,<3.0.0",
    "numpy>=2.0.0,<3.0.0",
    "xlrd>=2.0.1,<3.0.0",
    "regexparser>=0.1.0,<0.2.0",
    "pyyaml>=6.0,<7.0.0",
    "pyarrow>=19.0.0,<20.0.0",
    "progressbar2>=4.3.2,<5.0.0",
    "html5lib>=1.1,<2.0.0",
    "beautifulsoup4>=4.12.2,<5.0.0",
    "python-bcb>=0.3.2,<0.4.0",
    "duckdb>=1.2.0,<2.0.0",
    "rich>=13.0.0,<14.0.0",
    "openpyxl>=3.1.5,<4.0.0",
]
```

### 1c. Convert dependency groups to PEP 735 `[dependency-groups]`

```toml
[dependency-groups]
dev = [
    "ipykernel>=6.15.3,<7.0.0",
    "matplotlib>=3.7.1,<4.0.0",
    "mypy>=1.9.0,<2.0.0",
    "types-pyyaml>=6.0.12.20241230,<7.0.0",
    "types-pytz>=2024.2.0.20241221,<2025.0.0",
    "types-requests>=2.32.0.20241016,<3.0.0",
    "types-lxml>=2024.12.13,<2025.0.0",
    "ruff>=0.8.0,<1.0.0",
    "pre-commit>=4.0.0,<5.0.0",
]
docs = [
    "Sphinx>=5.1.1,<6.0.0",
]
tests = [
    "pytest>=7.1.3,<8.0.0",
]
```

### 1d. Add hatchling build config for `templates/` directory

```toml
[tool.hatch.build.targets.wheel]
packages = ["brasa", "templates"]
```

This replaces Poetry's `include = [{ path = "brasa" }, { path = "templates" }]`.

### 1e. Keep all `[tool.ruff]`, `[tool.pytest]`, `[tool.mypy]` sections unchanged

---

## Step 2: Replace lock file

- Delete `poetry.lock`
- Run `uv lock` to generate `uv.lock`
- Run `uv sync --all-groups` to install everything

---

## Step 3: Update all documentation (25 files)

Mechanical find-and-replace across all files:

| Pattern | Replacement |
|---------|-------------|
| `poetry run` | `uv run` |
| `poetry install` | `uv sync` |
| `Poetry` (as package manager name) | `uv` |
| `**Package Manager**: Poetry` | `**Package Manager**: uv` |

### Key files:

| File | Changes |
|------|---------|
| `CLAUDE.md` | Section header, description, all commands |
| `README.md` | 1 command |
| `docs/LINTING.md` | ~18 commands |
| `docs/USER_GUIDE.md` | Install + run commands |
| `docs/TEMPLATES.md` | Commands |
| `docs/DEPENDENCY_GRAPH.md` | ~12 commands |
| `docs/SUMMARY.md` | Install command + name |
| `docs/INVALID_DOWNLOADS_QUICK_REFERENCE.md` | 1 command |
| `.github/copilot-instructions.md` | Name + all commands |
| `.github/agents/Template Builder.agent.md` | Commands |
| `.github/prompts/plan-*.prompt.md` | 3 files, commands |
| `plan/*.md` | 5 files, commands |
| `scripts/migrate_download_trials_status.py` | Docstring command |
| `tests/benchmarks/benchmark_field_parsers.py` | Docstring commands |

---

## Step 4: Update VS Code settings

**File:** `.vscode/settings.json`

```jsonc
// FROM:
"python-envs.defaultEnvManager": "ms-python.python:poetry",
"python-envs.defaultPackageManager": "ms-python.python:poetry",

// TO:
"python-envs.defaultEnvManager": "ms-python.python:uv",
"python-envs.defaultPackageManager": "ms-python.python:uv",
```

---

## Step 5: Notebooks — no changes needed

The 3 notebooks referencing Poetry only have it in cell output tracebacks (stale virtualenv paths). These are historical artifacts, not instructions.

---

## Verification

```bash
rm poetry.lock
uv lock
uv sync --all-groups
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run pre-commit run --all-files
```
