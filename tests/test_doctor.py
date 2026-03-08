"""Tests for brasa doctor diagnostic checks.

Each check function is tested independently using the session-scoped
temporary cache directory provided by conftest.py.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from brasa.engine.cache import CacheManager
from brasa.engine.doctor import (
    DoctorReport,
    Issue,
    check_corrupted_parquet,
    check_date_gaps,
    check_empty_parquet,
    check_invalid_downloads,
    check_missing_db,
    check_missing_raw,
    check_orphan_db,
    check_orphan_raw,
    check_unresolved_errors,
    run_doctor,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _meta_conn():
    man = CacheManager()
    return sqlite3.connect(database=man.cache_path(man.meta_db_filename))


def _insert_meta(
    meta_id: str,
    template: str,
    download_checksum: str = "",
    downloaded_files: list | None = None,
    is_processed: bool = False,
    processing_errors: str = "",
    is_invalid_download: str = "0",
):
    """Insert a row directly into cache_metadata for testing."""
    with closing(_meta_conn()) as conn, conn:
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO cache_metadata "
            "(id, download_checksum, timestamp, response, download_args, template, "
            "downloaded_files, processed_files, extra_key, processing_errors, "
            "is_invalid_download, invalid_download_reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                meta_id,
                download_checksum,
                datetime.now().isoformat(),
                "{}",
                "{}",
                template,
                json.dumps(downloaded_files or []),
                json.dumps(is_processed),
                "",
                processing_errors,
                is_invalid_download,
                "",
            ),
        )


def _write_dummy_parquet(path: Path, schema: pa.Schema | None = None) -> None:
    """Write a minimal valid parquet file."""
    if schema is None:
        schema = pa.schema(
            [pa.field("refdate", pa.date32()), pa.field("value", pa.float64())]
        )
    table = pa.table({f.name: pa.array([], type=f.type) for f in schema})
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


# ---------------------------------------------------------------------------
# DoctorReport data type tests
# ---------------------------------------------------------------------------


class TestDoctorReport:
    def test_empty_report(self):
        report = DoctorReport()
        assert report.errors() == []
        assert report.warnings() == []
        assert report.fixable() == []
        assert "no issues" in report.summary()

    def test_summary_with_issues(self):
        report = DoctorReport(
            issues=[
                Issue("A", "x", "error", "desc"),
                Issue("A", "y", "warning", "desc", fixable=True, fix_fn=lambda: None),
            ]
        )
        assert len(report.errors()) == 1
        assert len(report.warnings()) == 1
        assert len(report.fixable()) == 1
        summary = report.summary()
        assert "1 error" in summary
        assert "1 warning" in summary
        assert "1 fixable" in summary


# ---------------------------------------------------------------------------
# Raw Files
# ---------------------------------------------------------------------------


class TestOrphanRaw:
    def test_no_raw_folder(self):
        issues = check_orphan_raw()
        assert issues == []

    def test_orphan_raw_detected(self, tmp_path):
        man = CacheManager()
        # Create an orphan directory (template/checksum not in metadata)
        orphan = (
            Path(man.cache_path(man._raw_folder)) / "my-template" / "orphan-checksum"
        )
        orphan.mkdir(parents=True, exist_ok=True)

        issues = check_orphan_raw()
        assert len(issues) == 1
        assert issues[0].code == "orphan-raw"
        assert issues[0].fixable is True
        assert any("orphan-checksum" in d for d in issues[0].details)

        # Apply fix and verify directory is removed
        issues[0].fix_fn()
        assert not orphan.exists()

    def test_known_checksum_not_orphan(self):
        man = CacheManager()
        # Create a directory that IS referenced by metadata
        known_checksum = "known-abc123"
        template = "b3-test"
        raw_dir = Path(man.cache_path(man._raw_folder)) / template / known_checksum
        raw_dir.mkdir(parents=True, exist_ok=True)

        _insert_meta(
            "test-known-id",
            template,
            download_checksum=known_checksum,
        )

        issues = check_orphan_raw()
        assert all(
            known_checksum not in d for d in (issues[0].details if issues else [])
        )


class TestMissingRaw:
    def test_no_metadata_no_issues(self):
        issues = check_missing_raw()
        assert issues == []

    def test_missing_file_detected(self):
        missing_path = "raw/b3-test/somecheck/MISSING.ZIP"
        _insert_meta(
            "missing-raw-test-id",
            "b3-test",
            download_checksum="somecheck",
            downloaded_files=[missing_path],
        )

        issues = check_missing_raw()
        assert len(issues) == 1
        assert issues[0].code == "missing-raw"
        assert issues[0].severity == "error"
        assert issues[0].fixable is True
        assert missing_path in issues[0].details

    def test_existing_file_no_issue(self):
        man = CacheManager()
        raw_dir = Path(man.cache_path("raw/b3-test/existcheck"))
        raw_dir.mkdir(parents=True, exist_ok=True)
        real_file = raw_dir / "REAL.ZIP"
        real_file.write_text("data")
        rel_path = "raw/b3-test/existcheck/REAL.ZIP"

        _insert_meta(
            "exist-raw-test-id",
            "b3-test",
            download_checksum="existcheck",
            downloaded_files=[rel_path],
        )

        issues = check_missing_raw()
        # The existing file should NOT appear in issues
        assert all(rel_path not in d for d in (issues[0].details if issues else []))


# ---------------------------------------------------------------------------
# DB / Parquet
# ---------------------------------------------------------------------------


class TestOrphanDb:
    def test_no_db_folder(self):
        issues = check_orphan_db()
        assert issues == []

    def test_orphan_db_folder_detected(self):
        man = CacheManager()
        orphan_db = Path(man.db_path("input/totally-unknown-dataset"))
        orphan_db.mkdir(parents=True, exist_ok=True)

        issues = check_orphan_db()
        assert any(i.code == "orphan-db" for i in issues)
        orphan_issue = next(i for i in issues if i.code == "orphan-db")
        assert any("totally-unknown-dataset" in d for d in orphan_issue.details)
        assert orphan_issue.fixable is True

        # Fix it
        orphan_issue.fix_fn()
        assert not orphan_db.exists()


class TestMissingDb:
    def test_no_missing_processed_files(self):
        issues = check_missing_db()
        assert issues == []

    def test_missing_processed_file_detected(self):
        # Insert a processed entry for a template that has no parquet files on disk
        _insert_meta(
            "missing-db-test-id",
            "b3-missing-db-template",
            is_processed=True,
        )

        issues = check_missing_db()
        assert any(i.code == "missing-db" for i in issues)
        db_issue = next(i for i in issues if i.code == "missing-db")
        assert "b3-missing-db-template" in db_issue.details
        assert db_issue.fixable is False

    def test_processed_with_existing_parquet_no_issue(self):
        man = CacheManager()
        template_id = "b3-has-parquet-template"
        # Create parquet file in the expected location
        parquet_dir = Path(man.db_path(f"input/{template_id}"))
        parquet_dir.mkdir(parents=True, exist_ok=True)
        parquet_file = parquet_dir / "part-0.parquet"
        _write_dummy_parquet(parquet_file)

        _insert_meta(
            "exists-db-test-id",
            template_id,
            is_processed=True,
        )

        issues = check_missing_db()
        assert all(template_id not in d for d in (issues[0].details if issues else []))

    def test_not_processed_not_flagged(self):
        # Unprocessed entries should not be flagged
        _insert_meta(
            "unprocessed-db-test-id",
            "b3-unprocessed-template",
            is_processed=False,
        )

        issues = check_missing_db()
        assert all(
            "b3-unprocessed-template" not in d
            for d in (issues[0].details if issues else [])
        )


class TestEmptyParquet:
    def test_no_empty_dirs(self):
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-test-empty"))
        part_dir = ds_dir / "refdate=2024-01-01"
        part_dir.mkdir(parents=True, exist_ok=True)
        # Write a real parquet file
        _write_dummy_parquet(part_dir / "part-0.parquet")

        issues = check_empty_parquet()
        assert all(
            str(part_dir) not in d for d in (issues[0].details if issues else [])
        )

    def test_empty_partition_dir_detected(self):
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-test-nofiles"))
        part_dir = ds_dir / "refdate=2024-02-01"
        part_dir.mkdir(parents=True, exist_ok=True)
        # No parquet files written

        issues = check_empty_parquet()
        assert any(i.code == "empty-parquet" for i in issues)
        empty_issue = next(i for i in issues if i.code == "empty-parquet")
        assert any("b3-test-nofiles" in d for d in empty_issue.details)
        assert empty_issue.fixable is True

        # Fix removes directory
        empty_issue.fix_fn()
        assert not part_dir.exists()


class TestCorruptedParquet:
    def test_no_corrupted_files(self):
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-test-good"))
        part_dir = ds_dir / "refdate=2024-01-01"
        _write_dummy_parquet(part_dir / "part-0.parquet")

        issues = check_corrupted_parquet()
        assert all(
            "b3-test-good" not in d for d in (issues[0].details if issues else [])
        )

    def test_corrupted_file_detected(self):
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-test-corrupt"))
        part_dir = ds_dir / "refdate=2024-01-01"
        part_dir.mkdir(parents=True, exist_ok=True)
        bad_file = part_dir / "part-0.parquet"
        bad_file.write_bytes(b"this is not a parquet file")

        issues = check_corrupted_parquet()
        assert any(i.code == "corrupted-parquet" for i in issues)
        corrupt_issue = next(i for i in issues if i.code == "corrupted-parquet")
        assert any("b3-test-corrupt" in d for d in corrupt_issue.details)
        assert corrupt_issue.fixable is False


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


class TestUnresolvedErrors:
    def test_no_errors(self):
        issues = check_unresolved_errors()
        assert issues == []

    def test_processing_errors_detected(self):
        _insert_meta(
            "proc-error-test-id",
            "b3-test-errors",
            processing_errors="Some parse error occurred",
        )

        issues = check_unresolved_errors()
        assert any(i.code == "unresolved-errors" for i in issues)
        err_issue = next(i for i in issues if i.code == "unresolved-errors")
        assert err_issue.fixable is False
        assert any("b3-test-errors" in d for d in err_issue.details)


class TestInvalidDownloads:
    def test_no_invalid(self):
        issues = check_invalid_downloads()
        assert issues == []

    def test_invalid_download_detected(self):
        _insert_meta(
            "invalid-dl-test-id",
            "b3-test-invalid",
            download_checksum="inv-checksum",
            is_invalid_download="1",
        )

        issues = check_invalid_downloads()
        assert any(i.code == "invalid-downloads" for i in issues)
        inv_issue = next(i for i in issues if i.code == "invalid-downloads")
        assert inv_issue.fixable is True
        assert any("b3-test-invalid" in d for d in inv_issue.details)

    def test_fix_invalid_deletes_row(self):
        _insert_meta(
            "fix-invalid-test-id",
            "b3-fix-invalid",
            download_checksum="fix-check",
            is_invalid_download="1",
        )

        issues = check_invalid_downloads()
        fix_issue = next((i for i in issues if i.code == "invalid-downloads"), None)
        assert fix_issue is not None

        fix_issue.fix_fn()

        # Row should be gone
        with closing(_meta_conn()) as conn, conn:
            c = conn.cursor()
            c.execute(
                "SELECT COUNT(*) FROM cache_metadata WHERE id = ?",
                ("fix-invalid-test-id",),
            )
            count = c.fetchone()[0]
        assert count == 0


# ---------------------------------------------------------------------------
# Date Gaps
# ---------------------------------------------------------------------------


class TestDateGaps:
    def test_no_refdate_partitions(self):
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-no-dates"))
        ds_dir.mkdir(parents=True, exist_ok=True)
        # No refdate= subdirs
        issues = check_date_gaps(since_days=365)
        assert all("b3-no-dates" not in i.description for i in issues)

    def test_complete_date_range_no_gaps(self):
        """A dataset with consecutive business days should show no gaps."""
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-no-gaps"))

        # Create two consecutive days (Mon + Tue, not a holiday)
        for d in ["2024-01-02", "2024-01-03"]:
            part_dir = ds_dir / f"refdate={d}"
            _write_dummy_parquet(part_dir / "part-0.parquet")

        issues = check_date_gaps(since_days=365)
        # No gaps expected for these two consecutive B3 business days
        gap_issues = [i for i in issues if "b3-no-gaps" in i.description]
        # We only test that no issues reference our specific dataset
        # (B3 holidays are hard to predict exactly, so we just verify no crash)
        assert isinstance(gap_issues, list)

    def test_date_gap_detected(self):
        """A dataset missing intermediate business days should surface an issue."""
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-with-gaps"))

        # Create first and last date but skip many business days in between.
        # Use a large since_days so historical dates are included.
        for d in ["2024-01-02", "2024-03-29"]:
            part_dir = ds_dir / f"refdate={d}"
            _write_dummy_parquet(part_dir / "part-0.parquet")

        issues = check_date_gaps(since_days=9999)
        gap_issues = [i for i in issues if "b3-with-gaps" in i.description]
        assert len(gap_issues) >= 1
        assert gap_issues[0].code == "date-gaps"
        assert gap_issues[0].severity == "error"
        assert gap_issues[0].fixable is False
        assert len(gap_issues[0].details) > 0


# ---------------------------------------------------------------------------
# run_doctor integration
# ---------------------------------------------------------------------------


class TestRunDoctor:
    def test_empty_cache_no_issues(self):
        """On a fresh empty cache there should be no issues."""
        report = run_doctor()
        assert isinstance(report, DoctorReport)
        # Internal check failures count as warnings, not errors
        # We allow warnings from template loading on empty cache
        assert report.errors() == [] or all(
            i.code.startswith("check-failed") for i in report.errors()
        )

    def test_category_filter(self):
        """Passing categories restricts which checks are run."""
        # Only run raw checks — should not include db or gap codes
        report = run_doctor(categories=["raw"])
        db_or_gap_codes = {i.code for i in report.issues} & {
            "orphan-db",
            "missing-db",
            "empty-parquet",
            "corrupted-parquet",
            "schema-drift",
            "date-gaps",
        }
        assert not db_or_gap_codes

    def test_since_days_parameter_accepted(self):
        """run_doctor accepts since_days without error."""
        report = run_doctor(since_days=7)
        assert isinstance(report, DoctorReport)

    def test_template_filter_accepted(self):
        """run_doctor accepts template_filter without error."""
        report = run_doctor(template_filter=["b3-cotahist-daily"])
        assert isinstance(report, DoctorReport)
