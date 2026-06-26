# Installing brasa with Python + pip

A step-by-step manual for a **brand new** installation of brasa using only
standard `python` and `pip` — no `uv` required.

## 1. Requirements

- **Python 3.10 or newer** (`requires-python = ">=3.10"`).
- `pip` and `venv` (both ship with a standard CPython install).

Check your Python version:

```bash
python --version      # or: python3 --version
```

If this prints `Python 3.10.x` or higher, you are good to go.

## 2. Create and activate a virtual environment

Always install into an isolated virtual environment so brasa's dependencies
don't collide with other projects.

```bash
# Create the environment in a folder named .venv
python -m venv .venv

# Activate it
source .venv/bin/activate          # Linux / macOS (bash, zsh)
# .venv\Scripts\activate           # Windows (PowerShell / cmd)
```

Once activated, your prompt shows `(.venv)` and `python`/`pip` point at the
environment. Upgrade pip first:

```bash
pip install --upgrade pip
```

## 3. Install brasa

Pick **one** of the following.

### Option A — from PyPI (released version)

```bash
pip install brasa
```

### Option B — from GitHub (latest `main`)

```bash
pip install "git+https://github.com/wilsonfreitas/brasa.git"
```

### Option C — from a local clone (for development)

```bash
git clone https://github.com/wilsonfreitas/brasa.git
cd brasa
pip install -e .            # editable install of the library

# Optional: also install dev/test tooling
pip install -e ".[tests]"  # pytest
```

All required dependencies (pandas, pyarrow, duckdb, requests, lxml, etc.) are
installed automatically — the package bundles its YAML templates and SQL DDL,
so nothing else needs to be downloaded.

## 4. Verify the installation

```bash
python -c "import brasa; from brasa.engine.template import list_templates; print(len(list_templates()), 'templates available')"
```

You should see a non-zero count of templates. The `brasa` command-line tool is
also installed:

```bash
brasa --help
```

## 5. Choose where data is stored (`BRASA_DATA_PATH`)

brasa keeps everything — raw downloads, parsed parquet files, and the metadata
database — under a single **brasa home** directory. It is resolved from the
`BRASA_DATA_PATH` environment variable, falling back to `./.brasa-cache` in the
current directory if unset.

To use a fixed location, export the variable (add it to `~/.bashrc`,
`~/.zshrc`, or a project `.env` to make it permanent):

```bash
export BRASA_DATA_PATH="$HOME/brasa-home"
```

## 6. Run setup

Initialize the cache directories and the metadata database:

```bash
brasa setup
```

This prints the resolved home and the exact `export` line to activate it:

```
Brasa home ready at /home/you/brasa-home

To use this home in your shell:
  export BRASA_DATA_PATH="/home/you/brasa-home"
```

For a one-off, **project-local** dataset, set the variable just for that
command instead of globally:

```bash
BRASA_DATA_PATH=./brasa-home brasa setup
```

## 7. First download and process

Download market data for a date, then process the raw files into parquet:

```bash
# Download a dataset for a specific reference date
brasa download b3-bvbg028 --arg refdate=2026-04-23

# Process everything downloaded but not yet parsed
brasa process b3-bvbg028
```

Or do it from Python:

```python
from brasa import download_marketdata, process_marketdata

download_marketdata("b3-bvbg028", refdate="2026-04-23")
process_marketdata("b3-bvbg028")
```

## 8. Query the data

Create the database views, then query with SQL:

```bash
brasa create-views
brasa query "SELECT * FROM \"input.b3-cotahist-daily\" LIMIT 5"
```

## Daily use

Remember to **activate the virtual environment** in each new shell before using
brasa:

```bash
source .venv/bin/activate
```

To leave the environment:

```bash
deactivate
```

## Troubleshooting

- **`command not found: brasa`** — the virtual environment isn't activated, or
  `pip install` ran against a different Python. Re-run `source .venv/bin/activate`.
- **`ModuleNotFoundError: No module named 'brasa'`** — install ran outside the
  active environment. Activate `.venv`, then reinstall.
- **Wrong Python version** — if `python` is older than 3.10, try `python3.10`
  (or newer) explicitly when creating the venv: `python3.10 -m venv .venv`.
- **Data goes to an unexpected place** — check `echo $BRASA_DATA_PATH`. When
  unset, brasa uses `./.brasa-cache` relative to your current directory.
