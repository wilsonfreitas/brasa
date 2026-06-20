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


def test_clean_meta_db_keeps_trials():
    """clean_meta_db deletes the cache_metadata row but leaves download_trials.

    Regression for WIL-75: clean_meta_db runs in the download flow (INVALID,
    DUPLICATED, ...) right after save_trial, so it must NOT delete trials.
    """
    cm = CacheManager()

    meta = _seed_meta(cm)
    _insert_trials(cm, meta.id, 3)
    assert _count_trials(cm, meta.id) == 3

    cm.clean_meta_db(meta)

    assert cm.has_meta(meta) is False
    assert _count_trials(cm, meta.id) == 3


def test_download_flow_invalid_keeps_trial_and_skips():
    """INVALID outcome (save_trial + clean_meta_db) keeps the trial and is skipped.

    Regression for WIL-75 / REQ-011.
    """
    from brasa.engine.api import _should_download

    cm = CacheManager()
    meta = _seed_meta(cm, template="wil75-invalid")

    # Mirror download_marketdata INVALID branch: record trial, then clean meta row.
    cm.save_trial(
        meta,
        downloaded=False,
        status_code="I",
        status_name="INVALID",
        reason="empty content",
    )
    cm.clean_meta_db(meta)

    assert _count_trials(cm, meta.id) == 1
    assert cm.get_last_download_status(meta)["code"] == "I"
    assert _should_download(cm, meta, force=False) is False


def test_download_flow_duplicated_keeps_trial_and_skips():
    """DUPLICATED outcome keeps the trial and is skipped on the next run."""
    from brasa.engine.api import _should_download

    cm = CacheManager()
    meta = _seed_meta(cm, template="wil75-dup")

    cm.save_trial(
        meta,
        downloaded=True,
        status_code="D",
        status_name="DUPLICATED",
        reason="duplicate folder",
    )
    cm.clean_meta_db(meta)

    assert _count_trials(cm, meta.id) == 1
    assert cm.get_last_download_status(meta)["code"] == "D"
    assert _should_download(cm, meta, force=False) is False


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


def test_drop_removes_entry():
    """CacheManager.drop removes the entry and its trials by id."""
    cm = CacheManager()

    meta = _seed_meta(cm)
    _insert_trials(cm, meta.id, 2)

    cm.drop(meta.id)

    assert cm.has_meta(meta) is False
    assert _count_trials(cm, meta.id) == 0


def test_drop_unknown_id_raises_cache_error():
    """CacheManager.drop raises CacheError when no entry matches the id."""
    import pytest

    from brasa.engine.exceptions import CacheError

    cm = CacheManager()

    with pytest.raises(CacheError, match="No cache entry"):
        cm.drop("not-a-real-id")


def test_cli_cache_drop_yes(tmp_path):
    """`brasa cache drop <id> --yes` removes the entry without prompting."""
    import os
    import subprocess

    env = {**os.environ, "BRASA_DATA_PATH": str(tmp_path)}
    # Initialize a CacheManager to seed data using the tmp_path cache.
    # We need to connect directly since CacheManager is a singleton in the main process.

    meta_db = tmp_path / "meta" / "meta.db"
    meta_db.parent.mkdir(parents=True, exist_ok=True)

    # Use subprocess so the singleton is fresh.
    seed_script = f"""
import os
os.environ["BRASA_DATA_PATH"] = {str(tmp_path)!r}
from brasa.engine import CacheManager
from brasa.engine.cache import CacheMetadata
cm = CacheManager()
meta = CacheMetadata(template="test-template")
meta.download_checksum = "deadbeef"
cm.save_meta(meta)
print(meta.id)
"""
    result = subprocess.run(
        ["uv", "run", "python", "-c", seed_script],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    meta_id = result.stdout.strip()

    drop_result = subprocess.run(
        ["uv", "run", "python", "-m", "brasa.cli", "cache", "drop", meta_id, "--yes"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert drop_result.returncode == 0, drop_result.stderr

    check_script = f"""
import os
os.environ["BRASA_DATA_PATH"] = {str(tmp_path)!r}
from brasa.engine import CacheManager
from brasa.engine.cache import CacheMetadata
cm = CacheManager()
d = cm._load_meta_dict_by_id({meta_id!r})
print("gone" if d is None else "present")
"""
    check = subprocess.run(
        ["uv", "run", "python", "-c", check_script],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert check.stdout.strip() == "gone"


def test_cli_cache_drop_unknown_id_exits_nonzero(tmp_path):
    """`brasa cache drop <unknown-id> --yes` exits with non-zero status."""
    import os
    import subprocess

    env = {**os.environ, "BRASA_DATA_PATH": str(tmp_path)}
    result = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "-m",
            "brasa.cli",
            "cache",
            "drop",
            "not-a-real-id",
            "--yes",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode != 0
