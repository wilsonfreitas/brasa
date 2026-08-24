"""Bounded in-memory DuckDB connections (WIL-178).

Every in-memory DuckDB connection in brasa must go through
:func:`connect_memory` so a memory cap and a disk-spill directory are
always set. DuckDB's default ``memory_limit`` (80% of RAM) combined with
no ``temp_directory`` (an in-memory database cannot spill to disk) is
what caused the ``run-all`` OOM kills.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from .config import load_config, resolve_data_path


def _default_memory_limit() -> str:
    """Return 50% of physical RAM as a DuckDB memory-limit string."""
    try:
        total = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        return "8GB"  # ponytail: non-Linux fallback; override via config
    return f"{total // (2 * 2**20)}MB"


def connect_memory() -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with a memory cap and disk spill.

    Precedence for each setting: env var, then ``config.toml`` key, then
    default.

    - Memory limit: ``BRASA_DUCKDB_MEMORY_LIMIT`` env var, then
      ``duckdb_memory_limit`` config key, then 50% of physical RAM.
    - Spill directory: ``BRASA_DUCKDB_TEMP_DIRECTORY`` env var, then
      ``duckdb_temp_directory`` config key, then ``<data_path>/tmp/duckdb``.

    Returns:
        A configured DuckDB connection. The caller must close it.
    """
    config = load_config()
    mem = (
        os.environ.get("BRASA_DUCKDB_MEMORY_LIMIT")
        or config.get("duckdb_memory_limit")
        or _default_memory_limit()
    )
    tmp = (
        os.environ.get("BRASA_DUCKDB_TEMP_DIRECTORY")
        or config.get("duckdb_temp_directory")
        or str(Path(resolve_data_path()) / "tmp" / "duckdb")
    )
    Path(tmp).mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(":memory:")
    conn.execute(f"SET memory_limit='{mem}'")
    conn.execute(f"SET temp_directory='{tmp}'")
    return conn
