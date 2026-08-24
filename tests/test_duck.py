"""Tests for brasa.engine.duck.connect_memory (WIL-178)."""

import os
from pathlib import Path

import duckdb

from brasa.engine.duck import connect_memory


def _setting(conn, name: str) -> str:
    return conn.execute(f"SELECT current_setting('{name}')").fetchone()[0]


def _reference_memory_limit(value: str) -> str:
    """What DuckDB displays for a given memory_limit value."""
    ref = duckdb.connect(":memory:")
    try:
        ref.execute(f"SET memory_limit='{value}'")
        return _setting(ref, "memory_limit")
    finally:
        ref.close()


def test_defaults_cap_memory_and_set_spill_dir(monkeypatch):
    monkeypatch.delenv("BRASA_DUCKDB_MEMORY_LIMIT", raising=False)
    monkeypatch.delenv("BRASA_DUCKDB_TEMP_DIRECTORY", raising=False)
    monkeypatch.setattr("brasa.engine.duck.load_config", lambda: {})
    conn = connect_memory()
    try:
        default_conn = duckdb.connect(":memory:")
        assert _setting(conn, "memory_limit") != _setting(default_conn, "memory_limit")
        default_conn.close()
        expected = Path(os.environ["BRASA_DATA_PATH"]) / "tmp" / "duckdb"
        assert _setting(conn, "temp_directory") == str(expected)
        assert expected.is_dir()
    finally:
        conn.close()


def test_config_file_overrides_defaults(tmp_path, monkeypatch):
    monkeypatch.delenv("BRASA_DUCKDB_MEMORY_LIMIT", raising=False)
    monkeypatch.delenv("BRASA_DUCKDB_TEMP_DIRECTORY", raising=False)
    spill = tmp_path / "spill"
    monkeypatch.setattr(
        "brasa.engine.duck.load_config",
        lambda: {
            "duckdb_memory_limit": "512MB",
            "duckdb_temp_directory": str(spill),
        },
    )
    conn = connect_memory()
    try:
        assert _setting(conn, "memory_limit") == _reference_memory_limit("512MB")
        assert _setting(conn, "temp_directory") == str(spill)
        assert spill.is_dir()
    finally:
        conn.close()


def test_env_overrides_config(tmp_path, monkeypatch):
    monkeypatch.setenv("BRASA_DUCKDB_MEMORY_LIMIT", "256MB")
    monkeypatch.setenv("BRASA_DUCKDB_TEMP_DIRECTORY", str(tmp_path / "env-spill"))
    monkeypatch.setattr(
        "brasa.engine.duck.load_config",
        lambda: {
            "duckdb_memory_limit": "512MB",
            "duckdb_temp_directory": str(tmp_path / "cfg-spill"),
        },
    )
    conn = connect_memory()
    try:
        assert _setting(conn, "memory_limit") == _reference_memory_limit("256MB")
        assert _setting(conn, "temp_directory") == str(tmp_path / "env-spill")
    finally:
        conn.close()
