"""Tests for scripts/migrate_meta_db_fk.py."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

# Make the standalone script importable in tests.
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

import migrate_meta_db_fk as mig


def _create_pre_migration_db(path: Path) -> None:
    """Create a meta.db that matches the pre-migration schema."""
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(
            """
            CREATE TABLE cache_metadata (
                id TEXT unique,
                download_checksum TEXT unique,
                timestamp TEXT,
                response TEXT,
                download_args TEXT,
                template TEXT,
                downloaded_files TEXT,
                processed_files TEXT,
                extra_key TEXT,
                processing_errors TEXT,
                is_invalid_download TEXT,
                invalid_download_reason TEXT
            );
            CREATE TABLE download_trials (
                cache_id TEXT,
                timestamp TEXT,
                downloaded TEXT,
                status_code TEXT,
                status_name TEXT,
                reason TEXT,
                http_status INTEGER
            );
            INSERT INTO cache_metadata (id, template) VALUES ('keep-id', 't');
            INSERT INTO download_trials (cache_id, timestamp, downloaded)
                VALUES ('keep-id', '2026-01-01T00:00:00', '1');
            INSERT INTO download_trials (cache_id, timestamp, downloaded)
                VALUES ('orphan-id', '2026-01-01T00:00:00', '1');
            """
        )
        conn.commit()
    finally:
        conn.close()


def _has_fk_on_download_trials(db_path: Path) -> bool:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute("PRAGMA foreign_key_list(download_trials)").fetchall()
        return any(r[2] == "cache_metadata" and r[3] == "cache_id" for r in rows)
    finally:
        conn.close()


def _count(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()


def test_migration_wipes_orphans_and_adds_fk(tmp_path):
    """Migration drops orphan trial rows and adds the FK + cascade."""
    db = tmp_path / "meta.db"
    _create_pre_migration_db(db)
    assert _count(db, "download_trials") == 2
    assert _has_fk_on_download_trials(db) is False

    result = mig.migrate(str(db))

    assert result["orphans_removed"] == 1
    assert result["fk_added"] is True
    assert _count(db, "download_trials") == 1
    assert _has_fk_on_download_trials(db) is True


def test_migration_is_idempotent(tmp_path):
    """Running the migration twice is a no-op for the FK addition."""
    db = tmp_path / "meta.db"
    _create_pre_migration_db(db)

    first = mig.migrate(str(db))
    second = mig.migrate(str(db))

    assert first["fk_added"] is True
    assert second["fk_added"] is False
    assert second["orphans_removed"] == 0


def test_fk_cascade_after_migration(tmp_path):
    """After migration, deleting cache_metadata cascades to download_trials."""
    db = tmp_path / "meta.db"
    _create_pre_migration_db(db)
    mig.migrate(str(db))

    conn = sqlite3.connect(str(db))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("DELETE FROM cache_metadata WHERE id = 'keep-id'")
        conn.commit()
        remaining = conn.execute(
            "SELECT COUNT(*) FROM download_trials WHERE cache_id = 'keep-id'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert remaining == 0
