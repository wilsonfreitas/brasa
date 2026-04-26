"""Tests for cache entry deletion (clean_meta_db, drop, FK enforcement)."""

from __future__ import annotations

import sqlite3

from brasa.engine import CacheManager
from brasa.engine.cache import CacheMetadata


def _seed_meta(cm: CacheManager, template: str = "test-template") -> CacheMetadata:
    """Seed a CacheMetadata row in cache_metadata and return it."""
    meta = CacheMetadata(template=template)
    meta.download_checksum = "deadbeef"
    cm.save_meta(meta)
    return meta


def _insert_trials(cm: CacheManager, cache_id: str, n: int) -> None:
    """Insert n download_trials rows for a given cache_id."""
    conn = sqlite3.connect(cm.cache_path(cm.meta_db_filename))
    try:
        for _ in range(n):
            conn.execute(
                "INSERT INTO download_trials "
                "(cache_id, timestamp, downloaded, status_code, status_name, "
                "reason, http_status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (cache_id, "2026-01-01T00:00:00", "1", ".", "PASSED", "", None),
            )
        conn.commit()
    finally:
        conn.close()


def _count_trials(cm: CacheManager, cache_id: str) -> int:
    conn = sqlite3.connect(cm.cache_path(cm.meta_db_filename))
    try:
        c = conn.execute(
            "SELECT COUNT(*) FROM download_trials WHERE cache_id = ?",
            (cache_id,),
        )
        return c.fetchone()[0]
    finally:
        conn.close()


def test_clean_meta_db_removes_trials_atomically():
    """clean_meta_db deletes both cache_metadata and download_trials rows."""
    cm = CacheManager()

    meta = _seed_meta(cm)
    _insert_trials(cm, meta.id, 3)
    assert _count_trials(cm, meta.id) == 3

    cm.clean_meta_db(meta)

    assert cm.has_meta(meta) is False
    assert _count_trials(cm, meta.id) == 0


def test_clean_meta_db_with_no_trials():
    """clean_meta_db works when no trials exist for the entry."""
    cm = CacheManager()

    meta = _seed_meta(cm)
    assert _count_trials(cm, meta.id) == 0

    cm.clean_meta_db(meta)  # must not raise

    assert cm.has_meta(meta) is False


def test_meta_db_connection_has_foreign_keys_on():
    """Every connection from CacheManager.meta_db_connection has FK enforcement on."""
    cm = CacheManager()

    conn = cm.meta_db_connection
    try:
        c = conn.execute("PRAGMA foreign_keys")
        (val,) = c.fetchone()
        assert val == 1
    finally:
        conn.close()
