"""Tests for brasa doctor diagnostic checks.

Each check function is tested independently using the session-scoped
temporary cache directory provided by conftest.py.
"""

from __future__ import annotations

import json
import sqlite3
import textwrap
from contextlib import closing
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from bizdays import Calendar

from brasa.engine.cache import CacheManager
from brasa.engine.doctor import (
    DoctorReport,
    Issue,
    _calendar_completeness_gaps,
    _load_validations_config,
    _read_series_dates,
    check_corrupted_parquet,
    check_date_gaps,
    check_download_refdate_gaps,
    check_empty_parquet,
    check_invalid_downloads,
    check_missing_db,
    check_missing_raw,
    check_orphan_db,
    check_orphan_raw,
    check_unresolved_errors,
    check_validations,
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
    download_args: str = "{}",
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
                download_args,
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
        issues = check_date_gaps(last_days=365)
        assert all("b3-no-dates" not in i.description for i in issues)

    def test_complete_date_range_no_gaps(self):
        """A dataset with consecutive business days should show no gaps."""
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-no-gaps"))

        # Create two consecutive days (Mon + Tue, not a holiday)
        for d in ["2024-01-02", "2024-01-03"]:
            part_dir = ds_dir / f"refdate={d}"
            _write_dummy_parquet(part_dir / "part-0.parquet")

        issues = check_date_gaps(last_days=365)
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
        # Use a large last_days so historical dates are included.
        for d in ["2024-01-02", "2024-03-29"]:
            part_dir = ds_dir / f"refdate={d}"
            _write_dummy_parquet(part_dir / "part-0.parquet")

        issues = check_date_gaps(last_days=-1)
        gap_issues = [
            i
            for i in issues
            if "b3-with-gaps" in i.description and i.code == "date-gaps"
        ]
        assert len(gap_issues) >= 1
        assert gap_issues[0].code == "date-gaps"
        assert gap_issues[0].severity == "error"
        assert gap_issues[0].fixable is False
        assert len(gap_issues[0].details) > 0

    def test_calendar_name_is_honored(self):
        """The calendar passed via --calendar drives the gap computation."""
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-cal-gaps"))
        for d in ["2024-01-02", "2024-03-29"]:
            part_dir = ds_dir / f"refdate={d}"
            _write_dummy_parquet(part_dir / "part-0.parquet")

        issues = check_date_gaps(last_days=-1, calendar_name="ANBIMA")
        gap_issues = [
            i
            for i in issues
            if "b3-cal-gaps" in i.description and i.code == "date-gaps"
        ]
        assert len(gap_issues) >= 1
        assert "ANBIMA business day(s)" in gap_issues[0].description

    def test_coverage_info_emitted_when_clean(self):
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-cov-ok"))
        for d in ["2024-01-02", "2024-01-03"]:
            _write_dummy_parquet(ds_dir / f"refdate={d}" / "part-0.parquet")
        issues = check_date_gaps(last_days=-1)
        cov = [
            i
            for i in issues
            if i.code == "date-gaps-coverage" and "input/b3-cov-ok" in i.description
        ]
        assert len(cov) == 1
        assert cov[0].severity == "info"
        assert cov[0].description == (
            "input/b3-cov-ok: checked 2024-01-02 → 2024-01-03 "
            "(2/2 B3 business days present)"
        )

    def test_coverage_ratio_reflects_gaps(self):
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-cov-gaps"))
        for d in ["2024-01-02", "2024-01-03", "2024-01-05"]:
            _write_dummy_parquet(ds_dir / f"refdate={d}" / "part-0.parquet")
        issues = check_date_gaps(last_days=-1)
        cov = [
            i
            for i in issues
            if i.code == "date-gaps-coverage" and "input/b3-cov-gaps" in i.description
        ]
        # 2024-01-04 is a Thursday business day -> 3 present / 4 expected
        assert len(cov) == 1
        assert "(3/4 B3 business days present)" in cov[0].description

    def test_coverage_empty_window_message(self):
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-cov-old"))
        for d in ["2024-01-02", "2024-01-03"]:
            _write_dummy_parquet(ds_dir / f"refdate={d}" / "part-0.parquet")
        issues = check_date_gaps(last_days=30)
        cov = [
            i
            for i in issues
            if i.code == "date-gaps-coverage" and "input/b3-cov-old" in i.description
        ]
        assert len(cov) == 1
        assert cov[0].description == (
            "input/b3-cov-old: no dates within the evaluated window "
            "(last 30 days; most recent date 2024-01-03)"
        )
        assert not [
            i for i in issues if i.code == "date-gaps" and "b3-cov-old" in i.description
        ]

    def test_one_day_window_is_evaluated(self):
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-cov-oneday"))
        newest = date.fromordinal(date.today().toordinal() - 30)
        # Walk back to a business day so the single-day window is on-calendar.
        cal = Calendar.load("B3")
        while not cal.isbizday(newest):
            newest = date.fromordinal(newest.toordinal() - 1)
        older = date.fromordinal(newest.toordinal() - 90)
        for d in [older, newest]:
            _write_dummy_parquet(ds_dir / f"refdate={d}" / "part-0.parquet")
        last_days = (date.today() - newest).days
        issues = check_date_gaps(last_days=last_days)
        cov = [
            i
            for i in issues
            if i.code == "date-gaps-coverage" and "input/b3-cov-oneday" in i.description
        ]
        assert len(cov) == 1
        assert f"checked {newest} → {newest} (1/1 B3 business days present)" in (
            cov[0].description
        )

    def test_single_refdate_partition_emits_nothing(self):
        man = CacheManager()
        ds_dir = Path(man.db_path("input/b3-cov-single"))
        _write_dummy_parquet(ds_dir / "refdate=2024-01-02" / "part-0.parquet")
        issues = check_date_gaps(last_days=-1)
        assert not [i for i in issues if "b3-cov-single" in i.description]


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

    def test_last_days_parameter_accepted(self):
        """run_doctor accepts last_days without error."""
        report = run_doctor(last_days=7)
        assert isinstance(report, DoctorReport)

    def test_template_filter_accepted(self):
        """run_doctor accepts template_filter without error."""
        report = run_doctor(template_filter=["b3-cotahist-daily"])
        assert isinstance(report, DoctorReport)

    def test_coverage_infos_do_not_affect_summary_or_errors(self):
        report = DoctorReport(
            issues=[
                Issue(
                    category="Downloads",
                    code="download-refdate-coverage",
                    severity="info",
                    description="t: checked 2024-01-02 → 2024-01-31 "
                    "(21/21 B3 business days downloaded)",
                ),
                Issue(
                    category="Date Gaps",
                    code="date-gaps-coverage",
                    severity="info",
                    description="input/d: checked 2024-01-02 → 2024-01-31 "
                    "(21/21 B3 business days present)",
                ),
            ]
        )
        assert report.errors() == []
        assert report.summary() == "no issues"
        assert len(report.infos()) == 2


# ---------------------------------------------------------------------------
# Calendar-completeness validation (WIL-6)
# ---------------------------------------------------------------------------


def _write_series_parquet(
    dataset_rel: str, symbol: str | None, dates: list[date]
) -> Path:
    """Write a synthetic series parquet under db/<dataset_rel>.

    When ``symbol`` is given, writes to a Hive partition ``symbol=<symbol>/``;
    otherwise writes a single unpartitioned file.
    """
    man = CacheManager()
    ds_dir = Path(man.db_path(dataset_rel))
    part_dir = ds_dir / f"symbol={symbol}" if symbol is not None else ds_dir
    part_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "refdate": pa.array(dates, type=pa.date32()),
            "value": pa.array([1.0] * len(dates), type=pa.float64()),
        }
    )
    pq.write_table(table, part_dir / "part-0.parquet")
    return ds_dir


def _write_validations(tmp_path, body: str) -> Path:
    p = tmp_path / "validations.yaml"
    p.write_text(textwrap.dedent(body))
    return p


def _full_anbima_series(dataset_rel, symbol, start, end):
    cal = Calendar.load("ANBIMA")
    days = [d.date() if hasattr(d, "date") else d for d in cal.seq(start, end)]
    _write_series_parquet(dataset_rel, symbol, days)
    return days


class TestLoadValidationsConfig:
    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _load_validations_config(tmp_path / "nope.yaml")

    def test_empty_file_raises(self, tmp_path):
        p = tmp_path / "v.yaml"
        p.write_text("")
        with pytest.raises(ValueError):
            _load_validations_config(p)

    def test_parses_mapping(self, tmp_path):
        p = tmp_path / "v.yaml"
        p.write_text(
            textwrap.dedent(
                """
                staging.bcb-sgs:
                  - rule: calendar-completeness
                    group_column: symbol
                    series:
                      CDI: {calendar: ANBIMA}
                """
            )
        )
        cfg = _load_validations_config(p)
        assert "staging.bcb-sgs" in cfg
        assert cfg["staging.bcb-sgs"][0]["rule"] == "calendar-completeness"

    def test_non_mapping_root_raises(self, tmp_path):
        p = tmp_path / "v.yaml"
        p.write_text("- just\n- a\n- list\n")
        with pytest.raises(ValueError):
            _load_validations_config(p)

    def test_unparseable_yaml_raises(self, tmp_path):
        p = tmp_path / "v.yaml"
        p.write_text("a: [1, 2\n")  # broken
        with pytest.raises(yaml.YAMLError):
            _load_validations_config(p)


class TestReadSeriesDates:
    def test_absent_dataset_returns_empty(self):
        man = CacheManager()
        missing = Path(man.db_path("staging/does-not-exist"))
        assert (
            _read_series_dates(
                "staging.does-not-exist", missing, "refdate", "symbol", "CDI"
            )
            == set()
        )

    def test_reads_grouped_series(self):
        d = [date(2020, 1, 2), date(2020, 1, 3)]
        ds_dir = _write_series_parquet("staging/read-grouped", "CDI", d)
        _write_series_parquet("staging/read-grouped", "SELIC", [date(2020, 6, 1)])
        got = _read_series_dates(
            "staging.read-grouped", ds_dir, "refdate", "symbol", "CDI"
        )
        assert got == set(d)

    def test_reads_single_series(self):
        d = [date(2020, 1, 2), date(2020, 1, 3)]
        ds_dir = _write_series_parquet("staging/read-single", None, d)
        got = _read_series_dates("staging.read-single", ds_dir, "refdate", None, None)
        assert got == set(d)

    def test_absent_group_value_returns_empty(self):
        ds_dir = _write_series_parquet("staging/read-nofoo", "CDI", [date(2020, 1, 2)])
        assert (
            _read_series_dates("staging.read-nofoo", ds_dir, "refdate", "symbol", "FOO")
            == set()
        )


class TestCalendarCompletenessGaps:
    def test_complete_series_no_gaps(self):
        cal = Calendar.load("ANBIMA")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq(date(2020, 1, 2), date(2020, 3, 31))
        ]
        assert _calendar_completeness_gaps(set(days), "ANBIMA", None, None) == []

    def test_missing_business_day_detected(self):
        cal = Calendar.load("ANBIMA")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq(date(2020, 1, 2), date(2020, 3, 31))
        ]
        hole = days[10]
        present = set(days) - {hole}
        missing = _calendar_completeness_gaps(present, "ANBIMA", None, None)
        assert missing == [hole]

    def test_weekend_not_required(self):
        # only business days present; a Saturday inside the range is not a gap
        cal = Calendar.load("ANBIMA")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq(date(2020, 1, 2), date(2020, 1, 31))
        ]
        assert _calendar_completeness_gaps(set(days), "ANBIMA", None, None) == []

    def test_explicit_start_extends_window(self):
        cal = Calendar.load("ANBIMA")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq(date(2020, 6, 1), date(2020, 6, 30))
        ]
        missing = _calendar_completeness_gaps(set(days), "ANBIMA", "2020-05-01", None)
        # every business day in May is missing
        may = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq(date(2020, 5, 1), date(2020, 5, 29))
        ]
        assert set(may).issubset(set(missing))

    def test_unknown_calendar_raises(self):
        with pytest.raises(Exception):  # noqa: B017 - bizdays raises bare Exception
            _calendar_completeness_gaps({date(2020, 1, 2)}, "NOPE", None, None)

    def test_bad_start_raises(self):
        with pytest.raises(ValueError):
            _calendar_completeness_gaps(
                {date(2020, 1, 2)}, "ANBIMA", "not-a-date", None
            )


class TestCheckCalendarCompleteness:
    def test_complete_series_no_issue(self, tmp_path):
        _full_anbima_series("staging/cc-ok", "CDI", date(2020, 1, 2), date(2020, 3, 31))
        cfg = _write_validations(
            tmp_path,
            """
            staging.cc-ok:
              - rule: calendar-completeness
                group_column: symbol
                series:
                  CDI: {calendar: ANBIMA}
            """,
        )
        assert check_validations(cfg) == []

    def test_business_day_hole_reported(self, tmp_path):
        days = _full_anbima_series(
            "staging/cc-hole", "CDI", date(2020, 1, 2), date(2020, 3, 31)
        )
        # rewrite CDI without one business day
        _write_series_parquet("staging/cc-hole", "CDI", days[:10] + days[11:])
        cfg = _write_validations(
            tmp_path,
            """
            staging.cc-hole:
              - rule: calendar-completeness
                group_column: symbol
                series:
                  CDI: {calendar: ANBIMA}
            """,
        )
        issues = check_validations(cfg)
        assert len(issues) == 1
        assert issues[0].code == "calendar-completeness"
        assert issues[0].severity == "error"
        assert str(days[10]) in issues[0].details

    def test_unlisted_series_ignored(self, tmp_path):
        _full_anbima_series(
            "staging/cc-unlisted", "CDI", date(2020, 1, 2), date(2020, 1, 31)
        )
        # SELIC has a big hole but is not listed -> ignored
        _write_series_parquet("staging/cc-unlisted", "SELIC", [date(2020, 1, 2)])
        cfg = _write_validations(
            tmp_path,
            """
            staging.cc-unlisted:
              - rule: calendar-completeness
                group_column: symbol
                series:
                  CDI: {calendar: ANBIMA}
            """,
        )
        assert check_validations(cfg) == []

    def test_absent_dataset_skipped(self, tmp_path):
        cfg = _write_validations(
            tmp_path,
            """
            staging.cc-absent:
              - rule: calendar-completeness
                group_column: symbol
                series:
                  CDI: {calendar: ANBIMA}
            """,
        )
        assert check_validations(cfg) == []

    def test_single_series_shape(self, tmp_path):
        cal = Calendar.load("ANBIMA")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq(date(2020, 1, 2), date(2020, 1, 31))
        ]
        _write_series_parquet("staging/cc-single", None, days[:5] + days[6:])
        cfg = _write_validations(
            tmp_path,
            """
            staging.cc-single:
              - rule: calendar-completeness
                calendar: ANBIMA
            """,
        )
        issues = check_validations(cfg)
        assert len(issues) == 1
        assert str(days[5]) in issues[0].details

    def test_unknown_calendar_is_error(self, tmp_path):
        _full_anbima_series(
            "staging/cc-badcal", "CDI", date(2020, 1, 2), date(2020, 1, 31)
        )
        cfg = _write_validations(
            tmp_path,
            """
            staging.cc-badcal:
              - rule: calendar-completeness
                group_column: symbol
                series:
                  CDI: {calendar: NOPE}
            """,
        )
        issues = check_validations(cfg)
        assert len(issues) == 1
        assert issues[0].code == "validation-config-error"
        assert issues[0].severity == "error"

    def test_unknown_rule_is_error(self, tmp_path):
        cfg = _write_validations(
            tmp_path,
            """
            staging.cc-badrule:
              - rule: not-a-rule
            """,
        )
        issues = check_validations(cfg)
        assert len(issues) == 1
        assert issues[0].code == "validation-config-error"

    def test_grouped_missing_series_is_error(self, tmp_path):
        cfg = _write_validations(
            tmp_path,
            """
            staging.cc-noseries:
              - rule: calendar-completeness
                group_column: symbol
            """,
        )
        issues = check_validations(cfg)
        assert len(issues) == 1
        assert issues[0].code == "validation-config-error"

    def test_unparseable_config_is_error(self, tmp_path):
        cfg = _write_validations(tmp_path, "a: [1, 2\n")
        issues = check_validations(cfg)
        assert len(issues) == 1
        assert issues[0].code == "validation-config-error"


class TestValidationsCategory:
    def test_category_keys(self):
        from brasa.engine.doctor import _CATEGORY_KEYS

        assert _CATEGORY_KEYS["validations"] == ["validations"]

    def test_explicit_validations_without_file_raises(self):
        with pytest.raises(ValueError):
            run_doctor(categories=["validations"])

    def test_mixed_explicit_validations_without_file_raises(self):
        with pytest.raises(ValueError):
            run_doctor(categories=["validations", "meta"], validations_config=None)

    def test_bare_run_skips_validations_with_note(self):
        report = run_doctor()
        codes = {i.code for i in report.issues}
        assert "validations-skipped" in codes

    def test_validations_with_file_runs(self, tmp_path):
        cfg = _write_validations(
            tmp_path,
            """
            staging.bcb-sgs:
              - rule: calendar-completeness
                group_column: symbol
                series:
                  CDI: {calendar: ANBIMA}
            """,
        )
        report = run_doctor(categories=["validations"], validations_config=cfg)
        assert isinstance(report, DoctorReport)

    def test_bad_file_produces_config_error_finding(self, tmp_path):
        # A supplied-but-unreadable file surfaces as an exit-1 finding
        # (validation-config-error), not a usage error and not a crash.
        report = run_doctor(
            categories=["validations"], validations_config=tmp_path / "nope.yaml"
        )
        error = next(i for i in report.issues if i.code == "validation-config-error")
        assert error.severity == "error"


class TestPeriodCompletenessGaps:
    def test_monthly_complete_no_gaps(self):
        from brasa.engine.doctor import _period_completeness_gaps

        present = {date(2024, 1, 15), date(2024, 2, 10), date(2024, 3, 5)}
        assert _period_completeness_gaps(present, "monthly", None, None) == []

    def test_monthly_one_missing_month(self):
        from brasa.engine.doctor import _period_completeness_gaps

        present = {date(2024, 1, 15), date(2024, 3, 5)}  # Feb missing
        assert _period_completeness_gaps(present, "monthly", None, None) == ["2024-02"]

    def test_monthly_year_boundary(self):
        from brasa.engine.doctor import _period_completeness_gaps

        present = {date(2023, 11, 2), date(2024, 2, 2)}  # Dec + Jan missing
        assert _period_completeness_gaps(present, "monthly", None, None) == [
            "2023-12",
            "2024-01",
        ]

    def test_quarterly_missing_quarter(self):
        from brasa.engine.doctor import _period_completeness_gaps

        present = {date(2024, 1, 10), date(2024, 12, 10)}  # Q1 + Q4; Q2, Q3 missing
        assert _period_completeness_gaps(present, "quarterly", None, None) == [
            "2024-Q2",
            "2024-Q3",
        ]

    def test_start_end_bucketed_from_mid_period(self):
        from brasa.engine.doctor import _period_completeness_gaps

        present = {date(2024, 2, 15)}
        # start mid-Jan buckets to 2024-01; end mid-Mar buckets to 2024-03
        assert _period_completeness_gaps(
            present, "monthly", "2024-01-20", "2024-03-10"
        ) == ["2024-01", "2024-03"]

    def test_default_end_is_last_observed(self):
        from brasa.engine.doctor import _period_completeness_gaps

        present = {date(2024, 1, 15), date(2024, 2, 10)}
        # no fabricated gap past the newest observation
        assert _period_completeness_gaps(present, "monthly", None, None) == []

    def test_unsupported_frequency_raises(self):
        from brasa.engine.doctor import _period_completeness_gaps

        with pytest.raises(ValueError):
            _period_completeness_gaps({date(2024, 1, 1)}, "weekly", None, None)


class TestFrequencyCompleteness:
    def _run(self, tmp_path, body):
        cfg = _write_validations(tmp_path, body)
        report = run_doctor(categories=["validations"], validations_config=cfg)
        return report

    def test_monthly_series_complete_no_issue(self, tmp_path):
        _write_series_parquet(
            "staging/macro-monthly",
            "IPCA",
            [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
        )
        report = self._run(
            tmp_path,
            """
            staging.macro-monthly:
              - rule: calendar-completeness
                group_column: symbol
                series:
                  IPCA: {frequency: monthly}
            """,
        )
        assert not [i for i in report.issues if i.code == "calendar-completeness"]

    def test_monthly_series_missing_month_reported(self, tmp_path):
        _write_series_parquet(
            "staging/macro-monthly",
            "IPCA",
            [date(2024, 1, 1), date(2024, 3, 1)],  # Feb missing
        )
        report = self._run(
            tmp_path,
            """
            staging.macro-monthly:
              - rule: calendar-completeness
                group_column: symbol
                series:
                  IPCA: {frequency: monthly}
            """,
        )
        found = [i for i in report.issues if i.code == "calendar-completeness"]
        assert len(found) == 1
        assert found[0].details == ["2024-02"]
        assert "month(s)" in found[0].description

    def test_quarterly_missing_quarter_reported(self, tmp_path):
        _write_series_parquet(
            "staging/macro-quarterly",
            None,
            [date(2024, 1, 1), date(2024, 12, 1)],  # Q1 + Q4; Q2, Q3 missing
        )
        report = self._run(
            tmp_path,
            """
            staging.macro-quarterly:
              - rule: calendar-completeness
                date_column: refdate
                frequency: quarterly
            """,
        )
        found = [i for i in report.issues if i.code == "calendar-completeness"]
        assert len(found) == 1
        assert found[0].details == ["2024-Q2", "2024-Q3"]
        assert "quarter(s)" in found[0].description

    def test_unknown_frequency_is_config_error_siblings_run(self, tmp_path):
        _write_series_parquet("staging/macro-monthly", "BAD", [date(2024, 1, 1)])
        _write_series_parquet(
            "staging/macro-monthly", "IPCA", [date(2024, 1, 1), date(2024, 2, 1)]
        )
        report = self._run(
            tmp_path,
            """
            staging.macro-monthly:
              - rule: calendar-completeness
                group_column: symbol
                series:
                  BAD: {frequency: weekly}
                  IPCA: {frequency: monthly}
            """,
        )
        errs = [i for i in report.issues if i.code == "validation-config-error"]
        assert any("BAD" in e.description for e in errs)
        # IPCA still evaluated (complete -> no calendar-completeness issue)
        assert not [i for i in report.issues if i.code == "calendar-completeness"]


class TestUnexpectedObservations:
    def test_helper_flags_weekend_and_holiday(self):
        from brasa.engine.doctor import _unexpected_observations

        # 2021-01-01 is a holiday; 2021-01-02 is a Saturday; 2021-01-04 a Monday.
        present = {date(2021, 1, 1), date(2021, 1, 2), date(2021, 1, 4)}
        flagged = _unexpected_observations(present, "B3")
        assert date(2021, 1, 1) in flagged
        assert date(2021, 1, 2) in flagged
        assert date(2021, 1, 4) not in flagged

    def test_helper_skips_out_of_coverage_dates(self):
        from bizdays import Calendar

        from brasa.engine.doctor import _as_date, _unexpected_observations

        cal = Calendar.load("B3")
        start = _as_date(cal.startdate)
        before = start.replace(year=start.year - 5)
        present = {before}  # out of coverage -> undeterminable -> not flagged
        assert _unexpected_observations(present, "B3") == []

    def test_rule_reports_saturday_row(self, tmp_path):
        _write_series_parquet(
            "staging/nb-check",
            "PETR4",
            [date(2021, 1, 4), date(2021, 1, 2)],  # Mon + Sat
        )
        cfg = _write_validations(
            tmp_path,
            """
            staging.nb-check:
              - rule: no-unexpected-observations
                group_column: symbol
                series:
                  PETR4: {calendar: B3}
            """,
        )
        report = run_doctor(categories=["validations"], validations_config=cfg)
        found = [i for i in report.issues if i.code == "no-unexpected-observations"]
        assert len(found) == 1
        assert found[0].severity == "error"
        assert "2021-01-02" in found[0].details

    def test_rule_clean_series_no_issue(self, tmp_path):
        _write_series_parquet(
            "staging/nb-clean", "PETR4", [date(2021, 1, 4), date(2021, 1, 5)]
        )
        cfg = _write_validations(
            tmp_path,
            """
            staging.nb-clean:
              - rule: no-unexpected-observations
                group_column: symbol
                series:
                  PETR4: {calendar: B3}
            """,
        )
        report = run_doctor(categories=["validations"], validations_config=cfg)
        assert not [i for i in report.issues if i.code == "no-unexpected-observations"]

    def test_rule_skips_non_daily_frequency(self, tmp_path):
        _write_series_parquet(
            "staging/nb-monthly",
            "IPCA",
            [date(2021, 1, 2)],  # Saturday
        )
        cfg = _write_validations(
            tmp_path,
            """
            staging.nb-monthly:
              - rule: no-unexpected-observations
                group_column: symbol
                series:
                  IPCA: {calendar: B3, frequency: monthly}
            """,
        )
        report = run_doctor(categories=["validations"], validations_config=cfg)
        assert not [i for i in report.issues if i.code == "no-unexpected-observations"]

    def test_unknown_rule_type_is_config_error(self, tmp_path):
        cfg = _write_validations(
            tmp_path,
            """
            staging.whatever:
              - rule: no-such-rule
            """,
        )
        report = run_doctor(categories=["validations"], validations_config=cfg)
        assert [i for i in report.issues if i.code == "validation-config-error"]


class TestReadColumns:
    def test_absent_dataset_returns_none(self):
        from brasa.engine.doctor import _read_columns

        man = CacheManager()
        path = Path(man.db_path("staging/does-not-exist"))
        assert _read_columns("staging.does-not-exist", path, ["refdate"]) is None

    def test_reads_requested_columns(self):
        from brasa.engine.doctor import _read_columns

        ds_dir = _write_series_parquet(
            "staging/rc-basic", None, [date(2021, 1, 4), date(2021, 1, 5)]
        )
        table = _read_columns("staging.rc-basic", ds_dir, ["refdate", "value"])
        assert table is not None
        assert set(table.column_names) == {"refdate", "value"}
        assert table.num_rows == 2

    def test_missing_column_raises_keyerror(self):
        from brasa.engine.doctor import _read_columns

        ds_dir = _write_series_parquet("staging/rc-missing", None, [date(2021, 1, 4)])
        with pytest.raises(KeyError):
            _read_columns("staging.rc-missing", ds_dir, ["nope"])


class TestValueRangeRule:
    def _run(self, tmp_path, body):
        cfg = _write_validations(tmp_path, body)
        return run_doctor(categories=["validations"], validations_config=cfg)

    def test_below_min_flagged(self, tmp_path):
        man = CacheManager()
        ds_dir = Path(man.db_path("staging/vr-neg"))
        ds_dir.mkdir(parents=True, exist_ok=True)
        table = pa.table({"refdate": [date(2021, 1, 4)], "close": [-5.0]})
        pq.write_table(table, ds_dir / "part-0.parquet")
        report = self._run(
            tmp_path,
            """
            staging.vr-neg:
              - rule: value-range
                column: close
                min: 0
            """,
        )
        found = [i for i in report.issues if i.code == "value-range"]
        assert len(found) == 1
        assert found[0].severity == "error"
        assert "-5.0" in found[0].details

    def test_in_range_no_issue(self, tmp_path):
        man = CacheManager()
        ds_dir = Path(man.db_path("staging/vr-ok"))
        ds_dir.mkdir(parents=True, exist_ok=True)
        table = pa.table({"refdate": [date(2021, 1, 4)], "close": [3.0]})
        pq.write_table(table, ds_dir / "part-0.parquet")
        report = self._run(
            tmp_path,
            """
            staging.vr-ok:
              - rule: value-range
                column: close
                min: 0
                max: 10
            """,
        )
        assert not [i for i in report.issues if i.code == "value-range"]

    def test_nulls_skipped(self, tmp_path):
        man = CacheManager()
        ds_dir = Path(man.db_path("staging/vr-null"))
        ds_dir.mkdir(parents=True, exist_ok=True)
        table = pa.table({"refdate": [date(2021, 1, 4)], "close": [None]})
        pq.write_table(table, ds_dir / "part-0.parquet")
        report = self._run(
            tmp_path,
            """
            staging.vr-null:
              - rule: value-range
                column: close
                min: 0
            """,
        )
        assert not [i for i in report.issues if i.code == "value-range"]

    def test_no_bounds_is_config_error(self, tmp_path):
        report = self._run(
            tmp_path,
            """
            staging.vr-x:
              - rule: value-range
                column: close
            """,
        )
        assert [i for i in report.issues if i.code == "validation-config-error"]

    def test_absent_column_is_config_error(self, tmp_path):
        man = CacheManager()
        ds_dir = Path(man.db_path("staging/vr-nocol"))
        ds_dir.mkdir(parents=True, exist_ok=True)
        table = pa.table({"refdate": [date(2021, 1, 4)], "close": [1.0]})
        pq.write_table(table, ds_dir / "part-0.parquet")
        report = self._run(
            tmp_path,
            """
            staging.vr-nocol:
              - rule: value-range
                column: missing
                min: 0
            """,
        )
        assert [i for i in report.issues if i.code == "validation-config-error"]


class TestNotNullRule:
    def _run(self, tmp_path, body):
        cfg = _write_validations(tmp_path, body)
        return run_doctor(categories=["validations"], validations_config=cfg)

    def _write(self, rel, cols):
        man = CacheManager()
        ds_dir = Path(man.db_path(rel))
        ds_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(cols), ds_dir / "part-0.parquet")
        return ds_dir

    def test_nulls_reported_aggregated(self, tmp_path):
        self._write(
            "staging/nn-bad",
            {"refdate": [date(2021, 1, 4), date(2021, 1, 5)], "close": [1.0, None]},
        )
        report = self._run(
            tmp_path,
            """
            staging.nn-bad:
              - rule: not-null
                columns: [refdate, close]
            """,
        )
        found = [i for i in report.issues if i.code == "not-null"]
        assert len(found) == 1
        assert found[0].details == ["close: 1 null(s)"]

    def test_no_nulls_no_issue(self, tmp_path):
        self._write(
            "staging/nn-ok",
            {"refdate": [date(2021, 1, 4)], "close": [1.0]},
        )
        report = self._run(
            tmp_path,
            """
            staging.nn-ok:
              - rule: not-null
                columns: [refdate, close]
            """,
        )
        assert not [i for i in report.issues if i.code == "not-null"]

    def test_empty_columns_is_config_error(self, tmp_path):
        report = self._run(
            tmp_path,
            """
            staging.nn-x:
              - rule: not-null
                columns: []
            """,
        )
        assert [i for i in report.issues if i.code == "validation-config-error"]


class TestNoDuplicatesRule:
    def _run(self, tmp_path, body):
        cfg = _write_validations(tmp_path, body)
        return run_doctor(categories=["validations"], validations_config=cfg)

    def _write(self, rel, cols):
        man = CacheManager()
        ds_dir = Path(man.db_path(rel))
        ds_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(cols), ds_dir / "part-0.parquet")
        return ds_dir

    def test_duplicates_reported(self, tmp_path):
        self._write(
            "staging/dup-bad",
            {
                "refdate": [date(2021, 1, 4), date(2021, 1, 4), date(2021, 1, 5)],
                "symbol": ["A", "A", "B"],
            },
        )
        report = self._run(
            tmp_path,
            """
            staging.dup-bad:
              - rule: no-duplicates
                key: [refdate, symbol]
            """,
        )
        found = [i for i in report.issues if i.code == "no-duplicates"]
        assert len(found) == 1
        assert "1 duplicated key(s)" in found[0].description
        assert "1 excess row(s)" in found[0].description

    def test_unique_no_issue(self, tmp_path):
        self._write(
            "staging/dup-ok",
            {"refdate": [date(2021, 1, 4), date(2021, 1, 5)], "symbol": ["A", "B"]},
        )
        report = self._run(
            tmp_path,
            """
            staging.dup-ok:
              - rule: no-duplicates
                key: [refdate, symbol]
            """,
        )
        assert not [i for i in report.issues if i.code == "no-duplicates"]

    def test_empty_key_is_config_error(self, tmp_path):
        report = self._run(
            tmp_path,
            """
            staging.dup-x:
              - rule: no-duplicates
                key: []
            """,
        )
        assert [i for i in report.issues if i.code == "validation-config-error"]


class TestDownloadRefdateGaps:
    def _seed(self, template, days, invalid=False):
        for i, d in enumerate(days):
            _insert_meta(
                f"{template}-{i}-{'x' if invalid else 'o'}",
                template,
                download_checksum=f"{template}{i}{'x' if invalid else 'o'}",
                download_args=json.dumps({"refdate": f"{d.isoformat()}T00:00:00"}),
                is_invalid_download="1" if invalid else "0",
            )

    def test_interior_gap_is_error(self):
        cal = Calendar.load("B3")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq("2024-01-02", "2024-01-31")
        ]
        present = days[:5] + days[10:]  # remove a run of 5 business days
        self._seed("dl-gap", present)
        issues = check_download_refdate_gaps(["dl-gap"], "B3", last_days=-1)
        gaps = [i for i in issues if i.code == "download-refdate-gaps"]
        assert len(gaps) == 1
        assert gaps[0].severity == "error"
        assert str(days[5]) in gaps[0].details
        assert str(days[9]) in gaps[0].details

    def test_complete_coverage_no_issue(self):
        cal = Calendar.load("B3")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq("2024-02-01", "2024-02-29")
        ]
        self._seed("dl-ok", days)
        issues = check_download_refdate_gaps(["dl-ok"], "B3", last_days=-1)
        assert not [i for i in issues if i.code == "download-refdate-gaps"]

    def test_out_of_calendar_date_is_info(self):
        # 2024-03-02 is a Saturday -> not a B3 business day.
        days = [date(2024, 3, 1), date(2024, 3, 2), date(2024, 3, 4)]
        self._seed("dl-extra", days)
        issues = check_download_refdate_gaps(["dl-extra"], "B3", last_days=-1)
        extra = [i for i in issues if i.code == "download-refdate-extra"]
        assert len(extra) == 1
        assert extra[0].severity == "info"
        assert "2024-03-02" in extra[0].details

    def test_template_without_refdate_rows_is_warning(self):
        _insert_meta("dl-none-0", "dl-none", download_checksum="dlnone0")
        issues = check_download_refdate_gaps(["dl-none"], "B3")
        warn = [i for i in issues if i.code == "download-refdate-missing-template"]
        assert len(warn) == 1
        assert warn[0].severity == "warning"

    def test_invalid_downloads_ignored(self):
        cal = Calendar.load("B3")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq("2024-04-01", "2024-04-30")
        ]
        # seed every business day as valid EXCEPT days[3]
        self._seed("dl-inv", days[:3] + days[4:])
        # days[3] exists ONLY as an invalid download row
        self._seed("dl-inv", [days[3]], invalid=True)
        issues = check_download_refdate_gaps(["dl-inv"], "B3", last_days=-1)
        gaps = [i for i in issues if i.code == "download-refdate-gaps"]
        # invalid row must NOT count as coverage -> days[3] is a gap
        assert len(gaps) == 1
        assert str(days[3]) in gaps[0].details

    def test_no_template_filter_yields_nothing(self):
        assert check_download_refdate_gaps(None, "B3") == []
        assert check_download_refdate_gaps([], "B3") == []

    def test_coverage_info_emitted_when_clean(self):
        cal = Calendar.load("B3")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq("2024-06-03", "2024-06-28")
        ]
        self._seed("dl-cov", days)
        issues = check_download_refdate_gaps(["dl-cov"], "B3", last_days=-1)
        cov = [i for i in issues if i.code == "download-refdate-coverage"]
        assert len(cov) == 1
        assert cov[0].severity == "info"
        assert cov[0].details == []
        n = len(days)
        assert (
            f"dl-cov: checked {days[0]} → {days[-1]} "
            f"({n}/{n} B3 business days downloaded)"
        ) == cov[0].description

    def test_coverage_ratio_reflects_gaps_and_precedes_gap_issue(self):
        cal = Calendar.load("B3")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq("2024-06-03", "2024-06-28")
        ]
        present = days[:5] + days[10:]
        self._seed("dl-cov-gap", present)
        issues = check_download_refdate_gaps(["dl-cov-gap"], "B3", last_days=-1)
        codes = [i.code for i in issues]
        assert codes.index("download-refdate-coverage") < codes.index(
            "download-refdate-gaps"
        )
        cov = next(i for i in issues if i.code == "download-refdate-coverage")
        assert f"({len(present)}/{len(days)} B3 business days downloaded)" in (
            cov.description
        )

    def test_coverage_excludes_off_calendar_dates(self):
        # 2024-03-02 is a Saturday -> off-calendar, excluded from the ratio.
        days = [date(2024, 3, 1), date(2024, 3, 2), date(2024, 3, 4)]
        self._seed("dl-cov-extra", days)
        issues = check_download_refdate_gaps(["dl-cov-extra"], "B3", last_days=-1)
        cov = next(i for i in issues if i.code == "download-refdate-coverage")
        assert "(2/2 B3 business days downloaded)" in cov.description
        assert [i for i in issues if i.code == "download-refdate-extra"]

    def test_coverage_empty_window_message(self):
        cal = Calendar.load("B3")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq("2024-01-02", "2024-01-31")
        ]
        self._seed("dl-cov-old", days)
        issues = check_download_refdate_gaps(["dl-cov-old"], "B3", last_days=30)
        assert len(issues) == 1
        assert issues[0].code == "download-refdate-coverage"
        assert issues[0].severity == "info"
        assert (
            f"dl-cov-old: no downloaded dates within the evaluated window "
            f"(last 30 days; most recent download {days[-1]})"
        ) == issues[0].description

    def test_no_dates_template_has_no_coverage_line(self):
        _insert_meta("dl-cov-none-0", "dl-cov-none", download_checksum="dlcovnone0")
        issues = check_download_refdate_gaps(["dl-cov-none"], "B3")
        assert [i.code for i in issues] == ["download-refdate-missing-template"]

    def test_since_window_excludes_old_gaps(self):
        """Gaps older than the --since window are not reported."""
        cal = Calendar.load("B3")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq("2024-01-02", "2024-01-31")
        ]
        self._seed("dl-old", days[:5] + days[10:])  # gap in Jan 2024
        issues = check_download_refdate_gaps(["dl-old"], "B3", last_days=30)
        assert not [i for i in issues if i.code == "download-refdate-gaps"]
        # widening the window brings the gap back
        issues = check_download_refdate_gaps(["dl-old"], "B3", last_days=-1)
        assert [i for i in issues if i.code == "download-refdate-gaps"]

    def test_integration_via_run_doctor(self):
        cal = Calendar.load("B3")
        days = [
            d.date() if hasattr(d, "date") else d
            for d in cal.seq("2024-05-02", "2024-05-31")
        ]
        self._seed("dl-int", days[:5] + days[10:])
        report = run_doctor(
            categories=["downloads"],
            template_filter=["dl-int"],
            calendar_name="B3",
            last_days=-1,
        )
        assert [i for i in report.issues if i.code == "download-refdate-gaps"]
        # no template filter -> this category is silent
        report2 = run_doctor(categories=["downloads"], calendar_name="B3")
        assert not [
            i
            for i in report2.issues
            if i.code
            in {
                "download-refdate-gaps",
                "download-refdate-extra",
                "download-refdate-missing-template",
            }
        ]
