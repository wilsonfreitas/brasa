"""Tests for scripts/migrate_meta_db_fk.py."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

import migrate_meta_db_fk as mig


def _create_db(path: Path) -> None:
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


def _count(db_path: Path, table: str, where: str = "") -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        sql = f"SELECT COUNT(*) FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return conn.execute(sql).fetchone()[0]
    finally:
        conn.close()


def test_migration_wipes_orphans(tmp_path):
    """Migration removes trial rows with no matching cache_metadata entry."""
    db = tmp_path / "meta.db"
    _create_db(db)
    assert _count(db, "download_trials") == 2

    result = mig.migrate(str(db))

    assert result["orphans_removed"] == 1
    assert _count(db, "download_trials") == 1
    assert _count(db, "download_trials", "cache_id = 'keep-id'") == 1


def test_migration_is_idempotent(tmp_path):
    """Running the migration twice removes nothing on the second run."""
    db = tmp_path / "meta.db"
    _create_db(db)

    first = mig.migrate(str(db))
    second = mig.migrate(str(db))

    assert first["orphans_removed"] == 1
    assert second["orphans_removed"] == 0


def test_migration_no_orphans(tmp_path):
    """Migration reports zero removals when there are no orphans."""
    db = tmp_path / "meta.db"
    _create_db(db)
    # Remove the orphan manually first.
    conn = sqlite3.connect(str(db))
    conn.execute("DELETE FROM download_trials WHERE cache_id = 'orphan-id'")
    conn.commit()
    conn.close()

    result = mig.migrate(str(db))

    assert result["orphans_removed"] == 0
