"""Tests for DownloadAttemptStatus enum and helpers in reporting module.

Tests cover:
- Symbol set uniqueness and completeness
- Deterministic exception-to-status mapping
- to_task_status conversion compatibility
"""

from __future__ import annotations

from typing import ClassVar

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


class TestDownloadAttemptStatusSymbols:
    """TEST-001: Verify symbol uniqueness and completeness."""

    EXPECTED_SYMBOLS: ClassVar[set[str]] = {".", "F", "E", "S", "D", "I", "C", "W"}

    def test_symbol_set_matches_spec(self):
        """All expected symbols are present."""
        symbols = {s.symbol for s in DownloadAttemptStatus}
        assert symbols == self.EXPECTED_SYMBOLS

    def test_symbols_are_unique(self):
        """No two statuses share a symbol."""
        symbols = [s.symbol for s in DownloadAttemptStatus]
        assert len(symbols) == len(set(symbols))

    def test_symbols_are_single_character(self):
        """Each symbol is exactly one character."""
        for status in DownloadAttemptStatus:
            assert len(status.symbol) == 1, f"{status.name} symbol is not single-char"

    def test_all_members_present(self):
        """Enum contains exactly the 8 defined members."""
        expected_names = {
            "PASSED",
            "FAILED",
            "ERROR",
            "SKIPPED",
            "DUPLICATED",
            "INVALID",
            "CORRUPTED",
            "WARNING",
        }
        assert {s.name for s in DownloadAttemptStatus} == expected_names

    def test_specific_symbol_assignments(self):
        """Verify each status maps to its specified symbol."""
        assert DownloadAttemptStatus.PASSED.symbol == "."
        assert DownloadAttemptStatus.FAILED.symbol == "F"
        assert DownloadAttemptStatus.ERROR.symbol == "E"
        assert DownloadAttemptStatus.SKIPPED.symbol == "S"
        assert DownloadAttemptStatus.DUPLICATED.symbol == "D"
        assert DownloadAttemptStatus.INVALID.symbol == "I"
        assert DownloadAttemptStatus.CORRUPTED.symbol == "C"
        assert DownloadAttemptStatus.WARNING.symbol == "W"


class TestMapExceptionToDownloadStatus:
    """TEST-002 through TEST-005: Deterministic exception mapping."""

    def test_none_maps_to_passed(self):
        """None -> PASSED."""
        assert map_exception_to_download_status(None) == (DownloadAttemptStatus.PASSED)

    def test_download_exception_maps_to_failed(self):
        """DownloadException -> FAILED (TEST-004)."""
        ex = DownloadException("status_code = 404")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.FAILED)

    def test_duplicated_folder_maps_to_duplicated(self):
        """DuplicatedFolderException -> DUPLICATED (TEST-002)."""
        ex = DuplicatedFolderException("folder already exists")
        assert map_exception_to_download_status(ex) == (
            DownloadAttemptStatus.DUPLICATED
        )

    def test_invalid_content_maps_to_invalid(self):
        """InvalidContentException -> INVALID (TEST-003)."""
        ex = InvalidContentException("validation failed")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.INVALID)

    def test_corrupted_content_maps_to_corrupted(self):
        """CorruptedContentException -> CORRUPTED."""
        ex = CorruptedContentException("truncated file")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.CORRUPTED)

    def test_generic_exception_maps_to_error(self):
        """Generic Exception -> ERROR (TEST-005)."""
        ex = RuntimeError("unexpected error")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.ERROR)

    def test_value_error_maps_to_error(self):
        """ValueError -> ERROR."""
        ex = ValueError("bad value")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.ERROR)


class TestToTaskStatus:
    """Verify to_task_status conversion preserves backward compatibility."""

    def test_passed_to_task_passed(self):
        assert to_task_status(DownloadAttemptStatus.PASSED) == TaskStatus.PASSED

    def test_failed_to_task_failed(self):
        assert to_task_status(DownloadAttemptStatus.FAILED) == TaskStatus.FAILED

    def test_error_to_task_error(self):
        assert to_task_status(DownloadAttemptStatus.ERROR) == TaskStatus.ERROR

    def test_skipped_to_task_skipped(self):
        assert to_task_status(DownloadAttemptStatus.SKIPPED) == TaskStatus.SKIPPED

    def test_duplicated_to_task_passed(self):
        """DUPLICATED is cache-reusable, maps to PASSED."""
        assert to_task_status(DownloadAttemptStatus.DUPLICATED) == (TaskStatus.PASSED)

    def test_invalid_to_task_failed(self):
        """INVALID is a content-validation failure, maps to FAILED."""
        assert to_task_status(DownloadAttemptStatus.INVALID) == (TaskStatus.FAILED)

    def test_corrupted_to_task_failed(self):
        """CORRUPTED is a transient failure, maps to FAILED."""
        assert to_task_status(DownloadAttemptStatus.CORRUPTED) == (TaskStatus.FAILED)

    def test_warning_to_task_warning(self):
        assert to_task_status(DownloadAttemptStatus.WARNING) == (TaskStatus.WARNING)

    def test_all_statuses_have_mapping(self):
        """Every DownloadAttemptStatus must have a to_task_status mapping."""
        for status in DownloadAttemptStatus:
            result = to_task_status(status)
            assert isinstance(result, TaskStatus), f"{status.name} has no mapping"


class TestDownloadAttemptStatusColors:
    """Verify every status has a color attribute."""

    def test_all_statuses_have_color(self):
        for status in DownloadAttemptStatus:
            assert isinstance(status.color, str)
            assert len(status.color) > 0
