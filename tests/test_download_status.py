"""Tests for deterministic download status classification.

Tests cover all 7 core outcomes: . (PASSED), F (FAILED), E (ERROR),
S (SKIPPED), D (DUPLICATED), I (INVALID), C (CORRUPTED).
"""

import tempfile
from pathlib import Path

import pytest

from brasa.engine.api import _should_download
from brasa.engine.cache import CacheManager, CacheMetadata
from brasa.engine.exceptions import (
    CorruptedContentException,
    DownloadException,
    DuplicatedFolderException,
    InvalidContentException,
)
from brasa.engine.reporting import (
    DownloadAttemptStatus,
    TaskStatus,
    map_exception_to_download_status,
    to_task_status,
)


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


class TestPassedStatus:
    """TEST-007: Successful downloads record '.' with downloaded files."""

    def test_passed_maps_from_none(self):
        assert map_exception_to_download_status(None) == (DownloadAttemptStatus.PASSED)

    def test_passed_symbol(self):
        assert DownloadAttemptStatus.PASSED.symbol == "."

    def test_passed_converts_to_task_passed(self):
        assert to_task_status(DownloadAttemptStatus.PASSED) == (TaskStatus.PASSED)

    def test_passed_persists_in_db(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(
            meta,
            downloaded=True,
            status_code=".",
            status_name="PASSED",
        )
        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "."
        assert status["name"] == "PASSED"


class TestFailedStatus:
    """TEST-004: DownloadException maps to F with HTTP status."""

    def test_failed_maps_from_download_exception(self):
        ex = DownloadException("status_code = 404")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.FAILED)

    def test_failed_symbol(self):
        assert DownloadAttemptStatus.FAILED.symbol == "F"

    def test_failed_converts_to_task_failed(self):
        assert to_task_status(DownloadAttemptStatus.FAILED) == (TaskStatus.FAILED)

    def test_failed_persists_with_http_status(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="F",
            status_name="FAILED",
            reason="status_code = 403",
            http_status=403,
        )
        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "F"
        assert status["http_status"] == 403


class TestErrorStatus:
    """TEST-005: Generic Exception maps to E."""

    def test_error_maps_from_generic_exception(self):
        ex = RuntimeError("unexpected")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.ERROR)

    def test_error_symbol(self):
        assert DownloadAttemptStatus.ERROR.symbol == "E"

    def test_error_converts_to_task_error(self):
        assert to_task_status(DownloadAttemptStatus.ERROR) == (TaskStatus.ERROR)


class TestSkippedStatus:
    """TEST-006: _should_download == False path returns S."""

    def test_skipped_symbol(self):
        assert DownloadAttemptStatus.SKIPPED.symbol == "S"

    def test_skipped_converts_to_task_skipped(self):
        assert to_task_status(DownloadAttemptStatus.SKIPPED) == (TaskStatus.SKIPPED)

    def test_should_download_returns_false_for_cached(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        meta.download_checksum = "chk"
        temp_cache.save_trial(meta, downloaded=True)
        temp_cache.save_meta(meta)

        meta2 = CacheMetadata("test-template")
        meta2.download_args = {"a": 1}
        assert _should_download(temp_cache, meta2, reprocess=False) is False


class TestDuplicatedStatus:
    """TEST-002, TEST-006A, TEST-006B: DuplicatedFolderException -> D."""

    def test_duplicated_maps_from_exception(self):
        ex = DuplicatedFolderException("folder exists")
        assert map_exception_to_download_status(ex) == (
            DownloadAttemptStatus.DUPLICATED
        )

    def test_duplicated_symbol(self):
        assert DownloadAttemptStatus.DUPLICATED.symbol == "D"

    def test_duplicated_converts_to_task_passed(self):
        """D is cache-reusable -> maps to PASSED."""
        assert to_task_status(DownloadAttemptStatus.DUPLICATED) == (TaskStatus.PASSED)

    def test_duplicated_persists(self, temp_cache):
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

    def test_duplicated_causes_skip_on_subsequent_attempt(self, temp_cache):
        """TEST-006A: First D -> subsequent attempt skipped when files exist."""
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        meta.download_checksum = "chk"

        # Create raw folder and file so file-existence guard passes
        raw_folder = Path(temp_cache.cache_path("raw/test-template/chk"))
        raw_folder.mkdir(parents=True, exist_ok=True)
        dummy_file = raw_folder / "data.txt"
        dummy_file.write_text("data")
        meta._downloaded_files = [str(Path("raw/test-template/chk/data.txt"))]

        temp_cache.save_trial(
            meta,
            downloaded=True,
            status_code="D",
            status_name="DUPLICATED",
        )
        temp_cache.save_meta(meta)

        meta2 = CacheMetadata("test-template")
        meta2.download_args = {"a": 1}
        result = _should_download(temp_cache, meta2, reprocess=False)
        assert result is False

    def test_duplicated_redownloads_when_files_missing(self, temp_cache):
        """TEST-006B: Prior D does not skip when files deleted."""
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        meta.download_checksum = "chk"
        meta._downloaded_files = [str(Path("raw/test-template/chk/missing.txt"))]

        temp_cache.save_trial(
            meta,
            downloaded=True,
            status_code="D",
            status_name="DUPLICATED",
        )
        temp_cache.save_meta(meta)

        meta2 = CacheMetadata("test-template")
        meta2.download_args = {"a": 1}
        result = _should_download(temp_cache, meta2, reprocess=False)
        assert result is True


class TestChecksumUniqueness:
    """Verify failed downloads don't collide on download_checksum UNIQUE."""

    def test_two_invalid_downloads_same_content_no_collision(self, temp_cache):
        """Two different dates producing the same empty file should both persist."""
        meta1 = CacheMetadata("test-template")
        meta1.download_args = {"refdate": "2025-01-01"}
        meta1.download_checksum = meta1.id  # simulate the fix
        meta1.is_invalid_download = True
        meta1.invalid_download_reason = "empty file"
        temp_cache.save_meta(meta1)

        meta2 = CacheMetadata("test-template")
        meta2.download_args = {"refdate": "2025-01-02"}
        meta2.download_checksum = meta2.id  # simulate the fix
        meta2.is_invalid_download = True
        meta2.invalid_download_reason = "empty file"
        temp_cache.save_meta(meta2)

        # Both entries saved — verify via has_meta
        assert temp_cache.has_meta(meta1)
        assert temp_cache.has_meta(meta2)
        assert meta1.id != meta2.id

    def test_two_failed_downloads_no_collision(self, temp_cache):
        """Two failed downloads (checksum never set) should both persist."""
        meta1 = CacheMetadata("test-template")
        meta1.download_args = {"refdate": "2025-01-01"}
        meta1.download_checksum = meta1.id
        temp_cache.save_meta(meta1)

        meta2 = CacheMetadata("test-template")
        meta2.download_args = {"refdate": "2025-01-02"}
        meta2.download_checksum = meta2.id
        temp_cache.save_meta(meta2)

        assert temp_cache.has_meta(meta1)
        assert temp_cache.has_meta(meta2)

    def test_invalid_meta_preserved_after_second_save(self, temp_cache):
        """is_invalid_download flag survives when checksum is meta.id."""
        meta = CacheMetadata("test-template")
        meta.download_args = {"refdate": "2025-01-01"}
        meta.download_checksum = meta.id
        meta.is_invalid_download = True
        meta.invalid_download_reason = "empty file"
        temp_cache.save_meta(meta)

        # Reload and verify
        meta2 = CacheMetadata("test-template")
        meta2.download_args = {"refdate": "2025-01-01"}
        assert temp_cache.has_meta(meta2)
        temp_cache.load_meta(meta2)
        assert meta2.is_invalid_download is True
        assert meta2.invalid_download_reason == "empty file"


class TestInvalidStatus:
    """TEST-003: InvalidContentException maps to I."""

    def test_invalid_maps_from_exception(self):
        ex = InvalidContentException("validation failed")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.INVALID)

    def test_invalid_symbol(self):
        assert DownloadAttemptStatus.INVALID.symbol == "I"

    def test_invalid_converts_to_task_failed(self):
        """I is a content validation failure -> maps to FAILED."""
        assert to_task_status(DownloadAttemptStatus.INVALID) == (TaskStatus.FAILED)

    def test_invalid_persists(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="I",
            status_name="INVALID",
            reason="File format validation failed",
        )
        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "I"
        assert "validation" in status["reason"]


class TestCorruptedStatus:
    """CorruptedContentException maps to C (transient, retryable)."""

    def test_corrupted_maps_from_exception(self):
        ex = CorruptedContentException("truncated file")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.CORRUPTED)

    def test_corrupted_symbol(self):
        assert DownloadAttemptStatus.CORRUPTED.symbol == "C"

    def test_corrupted_converts_to_task_failed(self):
        """C is a transient failure -> maps to FAILED."""
        assert to_task_status(DownloadAttemptStatus.CORRUPTED) == (TaskStatus.FAILED)

    def test_corrupted_persists(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="C",
            status_name="CORRUPTED",
            reason="Truncated file detected",
        )
        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "C"
        assert status["name"] == "CORRUPTED"

    def test_corrupted_does_not_skip_on_retry(self, temp_cache):
        """Unlike INVALID, CORRUPTED should allow re-download.

        After a corrupted download the raw folder is cleaned, so the
        previously downloaded files no longer exist on disk.
        """
        meta = CacheMetadata("test-template")
        meta.download_args = {"a": 1}
        meta.download_checksum = "chk"
        # Simulate files that were cleaned after corruption
        meta._downloaded_files = [str(Path("raw/test-template/chk/cleaned.txt"))]

        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="C",
            status_name="CORRUPTED",
            reason="Truncated file",
        )
        temp_cache.save_meta(meta)

        meta2 = CacheMetadata("test-template")
        meta2.download_args = {"a": 1}
        result = _should_download(temp_cache, meta2, reprocess=False)
        assert result is True


class TestRetryStatusIntegration:
    """TEST-015: Retries do not alter the deterministic status taxonomy.

    Verifying that after retry attempts, the final persisted status
    remains one of the canonical codes: '.', 'F', 'E', 'I', 'C', 'D'.
    """

    def test_retry_failures_then_success_preserves_passed(self, temp_cache):
        """Intermediate F trials + final PASSED: last status is '.'."""
        meta = CacheMetadata("test-template")
        meta.download_args = {"retry_test": "1"}

        # Simulate 2 failed retry attempts
        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="F",
            status_name="FAILED",
            reason="retry attempt 1",
            http_status=503,
        )
        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="F",
            status_name="FAILED",
            reason="retry attempt 2",
            http_status=503,
        )
        # Final success
        temp_cache.save_trial(
            meta,
            downloaded=True,
            status_code=".",
            status_name="PASSED",
        )

        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "."
        assert status["name"] == "PASSED"

    def test_exhausted_retries_preserves_failed(self, temp_cache):
        """All F trials: last status is 'F'."""
        meta = CacheMetadata("test-template")
        meta.download_args = {"retry_test": "2"}

        for i in range(3):
            temp_cache.save_trial(
                meta,
                downloaded=False,
                status_code="F",
                status_name="FAILED",
                reason=f"retry attempt {i + 1}",
                http_status=503,
            )

        status = temp_cache.get_last_download_status(meta)
        assert status["code"] == "F"

    def test_retry_does_not_introduce_new_status_code(self, temp_cache):
        """REQ-009: Retry must not introduce a new terminal status code."""
        meta = CacheMetadata("test-template")
        meta.download_args = {"retry_test": "3"}

        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="F",
            status_name="FAILED",
            reason="retry attempt 1",
        )
        temp_cache.save_trial(
            meta,
            downloaded=True,
            status_code=".",
            status_name="PASSED",
        )

        # All status codes in the DB must be from canonical set
        with temp_cache.meta_db_connection as conn:
            c = conn.cursor()
            c.execute(
                "SELECT DISTINCT status_code FROM download_trials "
                "WHERE cache_id = ?",
                (meta.id,),
            )
            codes = {row[0] for row in c.fetchall()}

        canonical = {".", "D", "I", "C", "F", "E", "S", "W"}
        assert codes.issubset(canonical), f"Unexpected codes: {codes - canonical}"

    def test_count_trials_helper(self, temp_cache):
        """TEST-012: count_trials returns correct trial count."""
        meta = CacheMetadata("test-template")
        meta.download_args = {"retry_test": "4"}

        assert temp_cache.count_trials(meta) == 0

        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="F",
            status_name="FAILED",
        )
        assert temp_cache.count_trials(meta) == 1

        temp_cache.save_trial(
            meta,
            downloaded=True,
            status_code=".",
            status_name="PASSED",
        )
        assert temp_cache.count_trials(meta) == 2

    def test_last_attempt_governs_scheduling(self, temp_cache):
        """TEST-010: Latest persisted attempt governs _should_download."""
        meta = CacheMetadata("test-template")
        meta.download_args = {"retry_sched": "1"}
        meta.download_checksum = "chk"

        # Create raw folder + file so file guard passes
        raw_folder = Path(temp_cache.cache_path("raw/test-template/chk"))
        raw_folder.mkdir(parents=True, exist_ok=True)
        dummy_file = raw_folder / "data.txt"
        dummy_file.write_text("data")
        meta._downloaded_files = [str(Path("raw/test-template/chk/data.txt"))]

        # First trial: F
        temp_cache.save_trial(
            meta,
            downloaded=False,
            status_code="F",
            status_name="FAILED",
            reason="retry attempt 1",
        )
        # Second trial: success - PASSED
        temp_cache.save_trial(
            meta,
            downloaded=True,
            status_code=".",
            status_name="PASSED",
        )
        temp_cache.save_meta(meta)

        meta2 = CacheMetadata("test-template")
        meta2.download_args = {"retry_sched": "1"}
        # Last status is PASSED and files exist -> should NOT download
        result = _should_download(temp_cache, meta2, reprocess=False)
        assert result is False
