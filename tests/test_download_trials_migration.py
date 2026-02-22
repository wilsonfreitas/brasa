"""Tests for the download_trials migration script.

Tests cover:
- Idempotent reruns
- Partial-schema caches
- Backfill correctness (downloaded=1 -> PASSED, downloaded=0 -> FAILED)
"""

import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from migrate_download_trials_status import migrate_download_trials


def _create_legacy_db(db_path: str) -> None:
    """Create a legacy download_trials table with no status columns."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "CREATE TABLE IF NOT EXISTS download_trials "
        "(cache_id TEXT, timestamp TEXT, downloaded TEXT)"
    )
    conn.commit()
    conn.close()


def _insert_legacy_rows(db_path: str, rows: list[tuple]) -> None:
    """Insert legacy rows into download_trials."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    for row in rows:
        c.execute("INSERT INTO download_trials VALUES (?, ?, ?)", row)
    conn.commit()
    conn.close()


class TestMigrationBackfill:
    """TEST-008A: Backfill correctness."""

    def test_backfill_passed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "meta.db")
            _create_legacy_db(db_path)
            _insert_legacy_rows(db_path, [("id1", "2025-01-01T00:00:00", "1")])

            result = migrate_download_trials(db_path)

            assert result["backfilled_passed"] == 1
            assert result["backfilled_failed"] == 0

            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute("SELECT status_code, status_name FROM download_trials")
            row = c.fetchone()
            assert row == (".", "PASSED")
            conn.close()

    def test_backfill_failed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "meta.db")
            _create_legacy_db(db_path)
            _insert_legacy_rows(db_path, [("id2", "2025-01-01T00:00:00", "0")])

            result = migrate_download_trials(db_path)

            assert result["backfilled_failed"] == 1
            assert result["backfilled_passed"] == 0

            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute("SELECT status_code, status_name FROM download_trials")
            row = c.fetchone()
            assert row == ("F", "FAILED")
            conn.close()

    def test_backfill_mixed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "meta.db")
            _create_legacy_db(db_path)
            _insert_legacy_rows(
                db_path,
                [
                    ("id1", "2025-01-01T00:00:00", "1"),
                    ("id2", "2025-01-02T00:00:00", "0"),
                    ("id3", "2025-01-03T00:00:00", "1"),
                ],
            )

            result = migrate_download_trials(db_path)

            assert result["backfilled_passed"] == 2
            assert result["backfilled_failed"] == 1


class TestMigrationIdempotency:
    """TEST-008A: Idempotent reruns."""

    def test_migrate_twice_no_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "meta.db")
            _create_legacy_db(db_path)
            _insert_legacy_rows(db_path, [("id1", "2025-01-01T00:00:00", "1")])

            migrate_download_trials(db_path)
            result2 = migrate_download_trials(db_path)

            # Second run should add no columns and backfill 0 rows
            assert result2["added_columns"] == []
            assert result2["backfilled_passed"] == 0
            assert result2["backfilled_failed"] == 0

    def test_data_unchanged_after_second_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "meta.db")
            _create_legacy_db(db_path)
            _insert_legacy_rows(
                db_path,
                [
                    ("id1", "2025-01-01T00:00:00", "1"),
                    ("id2", "2025-01-01T00:00:00", "0"),
                ],
            )

            migrate_download_trials(db_path)
            migrate_download_trials(db_path)

            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute(
                "SELECT cache_id, status_code, status_name "
                "FROM download_trials ORDER BY cache_id"
            )
            rows = c.fetchall()
            assert rows == [
                ("id1", ".", "PASSED"),
                ("id2", "F", "FAILED"),
            ]
            conn.close()


class TestMigrationPartialSchema:
    """TEST-008A: Partial-schema caches."""

    def test_partial_columns_added(self):
        """DB that already has some but not all new columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "meta.db")

            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute(
                "CREATE TABLE download_trials "
                "(cache_id TEXT, timestamp TEXT, downloaded TEXT, "
                "status_code TEXT)"
            )
            conn.commit()
            conn.close()

            result = migrate_download_trials(db_path)

            # status_code already existed; others were added
            assert "status_code" not in result["added_columns"]
            assert "status_name" in result["added_columns"]
            assert "reason" in result["added_columns"]
            assert "http_status" in result["added_columns"]

    def test_added_columns_list(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "meta.db")
            _create_legacy_db(db_path)

            result = migrate_download_trials(db_path)

            expected_cols = {"status_code", "status_name", "reason", "http_status"}
            assert set(result["added_columns"]) == expected_cols
