"""Tests for download status persistence in CacheManager.

Tests cover:
- save_trial with explicit status codes
- get_last_download_status retrieval
- Legacy backward compatibility (boolean only)
- Schema migration of existing databases
"""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from brasa.engine.cache import CacheManager, CacheMetadata, _extract_http_status


@pytest.fixture
def temp_cache():
    """Create a temporary cache directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_cache = CacheManager.__dict__.get("__it__")
        CacheManager.__it__ = None

        cache = CacheManager()
        cache._cache_folder = tmpdir
        Path(tmpdir).mkdir(parents=True, exist_ok=True)
        Path(cache.cache_path(cache._meta_folder)).mkdir(parents=True, exist_ok=True)
        Path(cache.cache_path(cache._db_folder)).mkdir(parents=True, exist_ok=True)
        cache.create_meta_db()

        yield cache

        if original_cache is not None:
            CacheManager.__it__ = original_cache
        else:
            CacheManager.__it__ = None


class TestSaveTrialWithStatus:
    """TEST-002, TEST-004, TEST-005, TEST-007: Persist explicit status."""

    def test_save_passed_status(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(
            meta,
            downloaded=True,
            status_code=".",
            status_name="PASSED",
        )
        status = temp_cache.get_last_download_status(meta)
        assert status is not None
        assert status["code"] == "."
        assert status["name"] == "PASSED"

    def test_save_failed_status_with_http(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="F",
            status_name="FAILED",
            reason="status_code = 404",
            http_status=404,
        )
        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "F"
        assert status["http_status"] == 404
        assert "404" in status["reason"]

    def test_save_duplicated_status(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(
            meta,
            downloaded=True,
            status_code="D",
            status_name="DUPLICATED",
            reason="folder exists",
        )
        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "D"
        assert status["name"] == "DUPLICATED"

    def test_save_invalid_status(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="I",
            status_name="INVALID",
            reason="validation failed",
        )
        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "I"

    def test_save_error_status(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="E",
            status_name="ERROR",
            reason="runtime crash",
        )
        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "E"
        assert status["name"] == "ERROR"

    def test_legacy_boolean_save_trial(self, temp_cache):
        """Backward compatibility: boolean-only call."""
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(meta, downloaded=True)
        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "."
        assert status["name"] == "PASSED"

    def test_legacy_boolean_false(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(meta, downloaded=False)
        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "F"
        assert status["name"] == "FAILED"


class TestGetLastDownloadStatus:
    """TEST-008: Retrieval from DB."""

    def test_no_trials_returns_none(self, temp_cache):
        meta = CacheMetadata("nonexistent")
        meta.download_args = {"x": 1}
        assert temp_cache.get_last_download_status(meta) is None

    def test_returns_latest_trial(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        from datetime import datetime

        meta.timestamp = datetime(2025, 1, 1, 0, 0, 0)
        temp_cache.save_trial(
            meta, downloaded=False, status_code="F", status_name="FAILED"
        )
        # Second trial (more recent timestamp)
        meta.timestamp = datetime(2025, 1, 1, 0, 0, 1)
        temp_cache.save_trial(
            meta, downloaded=True, status_code=".", status_name="PASSED"
        )
        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "."

    def test_reason_defaults_empty(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(
            meta, downloaded=True, status_code=".", status_name="PASSED"
        )
        status = temp_cache.get_last_download_status(meta)
        assert status["reason"] == ""

    def test_http_status_null(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(
            meta, downloaded=True, status_code=".", status_name="PASSED"
        )
        status = temp_cache.get_last_download_status(meta)
        assert status["http_status"] is None


class TestExtractHttpStatus:
    """TEST-004: HTTP status extraction from exception messages."""

    def test_extracts_standard_format(self):
        assert _extract_http_status(Exception("status_code = 404 url = ...")) == 404

    def test_extracts_no_spaces(self):
        assert _extract_http_status(Exception("status_code=500")) == 500

    def test_returns_none_for_no_match(self):
        assert _extract_http_status(Exception("some error")) is None

    def test_returns_none_for_empty(self):
        assert _extract_http_status(Exception("")) is None


class TestMigrationOnInit:
    """TEST-008: Migration does not break old data reads."""

    def test_legacy_schema_migration(self):
        """Create a DB with legacy schema, then verify migration adds columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            original_cache = CacheManager.__dict__.get("__it__")
            CacheManager.__it__ = None

            meta_dir = Path(tmpdir) / "meta"
            meta_dir.mkdir()
            db_path = str(meta_dir / "meta.db")

            # Create legacy schema
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute(
                "CREATE TABLE IF NOT EXISTS cache_metadata "
                "(id TEXT unique, download_checksum TEXT unique, "
                "timestamp TEXT, response TEXT, download_args TEXT, "
                "template TEXT, downloaded_files TEXT, processed_files TEXT, "
                "extra_key TEXT, processing_errors TEXT, "
                "is_invalid_download TEXT, invalid_download_reason TEXT)"
            )
            c.execute(
                "CREATE TABLE IF NOT EXISTS download_trials "
                "(cache_id TEXT, timestamp TEXT, downloaded TEXT)"
            )
            # Insert legacy rows
            c.execute(
                "INSERT INTO download_trials VALUES (?, ?, ?)",
                ("id1", "2025-01-01T00:00:00", "1"),
            )
            c.execute(
                "INSERT INTO download_trials VALUES (?, ?, ?)",
                ("id2", "2025-01-01T00:00:00", "0"),
            )
            conn.commit()
            conn.close()

            # Now init cache which triggers migration
            cache = CacheManager()
            cache._cache_folder = tmpdir
            Path(tmpdir).mkdir(parents=True, exist_ok=True)
            Path(cache.cache_path(cache._db_folder)).mkdir(parents=True, exist_ok=True)
            cache._migrate_download_trials()

            # Verify new columns exist
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute("PRAGMA table_info(download_trials)")
            cols = {row[1] for row in c.fetchall()}
            assert "status_code" in cols
            assert "status_name" in cols
            assert "reason" in cols
            assert "http_status" in cols

            # Verify backfill
            c.execute(
                "SELECT status_code, status_name FROM download_trials "
                "WHERE cache_id='id1'"
            )
            row = c.fetchone()
            assert row == (".", "PASSED")

            c.execute(
                "SELECT status_code, status_name FROM download_trials "
                "WHERE cache_id='id2'"
            )
            row = c.fetchone()
            assert row == ("F", "FAILED")
            conn.close()

            if original_cache is not None:
                CacheManager.__it__ = original_cache
            else:
                CacheManager.__it__ = None


class TestGetTemplatesWithUnprocessedDownloads:
    """Tests for CacheManager.get_templates_with_unprocessed_downloads."""

    def _make_meta(self, template: str, args: dict, downloaded: bool, processed: bool):
        """Helper to build a CacheMetadata with controlled state."""
        import hashlib

        meta = CacheMetadata(template)
        meta.download_args = args
        # Use a deterministic but unique checksum derived from template+args
        checksum = hashlib.md5(f"{template}{args}".encode()).hexdigest()[:8]
        meta.download_checksum = checksum
        if downloaded:
            meta.add_downloaded_file(f"raw/{template}/{checksum}/file.csv.gz")
        if processed:
            meta.add_processed_file("data", f"db/input/{template}/2024-01-01.parquet")
        return meta

    def test_empty_cache_returns_empty_list(self, temp_cache):
        result = temp_cache.get_templates_with_unprocessed_downloads()
        assert result == []

    def test_unprocessed_entry_is_returned(self, temp_cache):
        meta = self._make_meta("tmpl-a", {"refdate": "2024-01-01"}, True, False)
        temp_cache.save_meta(meta)

        result = temp_cache.get_templates_with_unprocessed_downloads()
        assert len(result) == 1
        assert result[0]["template"] == "tmpl-a"
        assert result[0]["count"] == 1

    def test_processed_entry_not_returned(self, temp_cache):
        meta = self._make_meta("tmpl-a", {"refdate": "2024-01-01"}, True, True)
        temp_cache.save_meta(meta)

        result = temp_cache.get_templates_with_unprocessed_downloads()
        assert result == []

    def test_not_downloaded_entry_not_returned(self, temp_cache):
        meta = self._make_meta("tmpl-a", {"refdate": "2024-01-01"}, False, False)
        temp_cache.save_meta(meta)

        result = temp_cache.get_templates_with_unprocessed_downloads()
        assert result == []

    def test_invalid_download_not_returned(self, temp_cache):
        meta = self._make_meta("tmpl-a", {"refdate": "2024-01-01"}, True, False)
        meta.is_invalid_download = True
        temp_cache.save_meta(meta)

        result = temp_cache.get_templates_with_unprocessed_downloads()
        assert result == []

    def test_count_aggregated_per_template(self, temp_cache):
        for day in ["2024-01-01", "2024-01-02", "2024-01-03"]:
            meta = self._make_meta("tmpl-a", {"refdate": day}, True, False)
            temp_cache.save_meta(meta)

        result = temp_cache.get_templates_with_unprocessed_downloads()
        assert len(result) == 1
        assert result[0]["template"] == "tmpl-a"
        assert result[0]["count"] == 3

    def test_multiple_templates_sorted(self, temp_cache):
        for template in ["tmpl-c", "tmpl-a", "tmpl-b"]:
            meta = self._make_meta(template, {"refdate": "2024-01-01"}, True, False)
            temp_cache.save_meta(meta)

        result = temp_cache.get_templates_with_unprocessed_downloads()
        assert [r["template"] for r in result] == ["tmpl-a", "tmpl-b", "tmpl-c"]

    def test_mixed_processed_and_unprocessed(self, temp_cache):
        processed = self._make_meta("tmpl-a", {"refdate": "2024-01-01"}, True, True)
        temp_cache.save_meta(processed)
        unprocessed = self._make_meta("tmpl-a", {"refdate": "2024-01-02"}, True, False)
        temp_cache.save_meta(unprocessed)

        result = temp_cache.get_templates_with_unprocessed_downloads()
        assert len(result) == 1
        assert result[0]["count"] == 1
