"""Tests for download status persistence in CacheManager.

Tests cover:
- save_trial with explicit status codes
- get_last_download_status retrieval
- Legacy backward compatibility (boolean only)
"""

from brasa.engine.cache import CacheMetadata
from brasa.util import DownloadArgs


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
            meta.mark_as_processed()
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


class TestDownloadArgsSerialization:
    """Verify save_meta/load_meta roundtrip preserves canonical form."""

    def test_save_and_load_meta_preserves_download_args(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = DownloadArgs({"refdate": "2000-01-01"})
        meta.download_checksum = "abc123"
        temp_cache.save_meta(meta)

        meta2 = CacheMetadata("test-template")
        meta2.download_args = DownloadArgs({"refdate": "2000-01-01"})
        temp_cache.load_meta(meta2)

        assert isinstance(meta2.download_args, DownloadArgs)
        assert meta2.download_args["refdate"] == "2000-01-01T00:00:00"

    def test_same_id_before_and_after_roundtrip(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = DownloadArgs({"refdate": "2000-01-01"})
        meta.download_checksum = "abc123"
        original_id = meta.id

        temp_cache.save_meta(meta)

        meta2 = CacheMetadata("test-template")
        meta2.download_args = DownloadArgs({"refdate": "2000-01-01"})
        temp_cache.load_meta(meta2)

        assert meta2.id == original_id

    def test_bare_date_and_datetime_string_produce_same_id(self, temp_cache):
        meta1 = CacheMetadata("test-template")
        meta1.download_args = DownloadArgs({"refdate": "2000-01-01"})

        meta2 = CacheMetadata("test-template")
        meta2.download_args = DownloadArgs({"refdate": "2000-01-01T00:00:00"})

        assert meta1.id == meta2.id

    def test_no_integrity_error_on_process_after_download(self, temp_cache):
        """Reproduce WIL-34: save with bare date, reload, save again — no UNIQUE error."""
        meta = CacheMetadata("bcb-sgs")
        meta.download_args = DownloadArgs({"refdate": "2000-01-01"})
        meta.download_checksum = "deadbeef"
        temp_cache.save_meta(meta)

        # Simulate process time: reload from DB
        meta2 = CacheMetadata("bcb-sgs")
        meta2.download_args = DownloadArgs({"refdate": "2000-01-01"})
        temp_cache.load_meta(meta2)

        # Mark as processed and save again — must NOT raise IntegrityError
        meta2.mark_as_processed()
        temp_cache.save_meta(meta2)  # should not raise

        # Verify it updated (not inserted a duplicate)
        with temp_cache.meta_db_connection as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM cache_metadata WHERE template = 'bcb-sgs'")
            count = c.fetchone()[0]
        assert count == 1
