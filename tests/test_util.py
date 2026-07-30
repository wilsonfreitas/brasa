"""Tests for brasa.util: arg parsing, DateRange, DownloadArgs, zip helpers, warnings."""

import json
import warnings
import zipfile
from datetime import date, datetime, timedelta
from io import BytesIO
from unittest.mock import patch

import pytest
from bizdays import Calendar

from brasa.engine.core import json_convert_from_object, json_convert_to_object
from brasa.util import (
    DateRange,
    DownloadArgs,
    KwargsIterator,
    SuppressUserWarnings,
    _is_zip,
    generate_checksum_for_template,
    generate_checksum_from_zip,
    is_iterable,
    parse_arg_value,
    unzip_and_get_content,
    unzip_file_to,
)


class TestParseArgValueDateAutoDetect:
    """parse_arg_value should auto-detect ISO dates without @ prefix."""

    def test_single_date_without_prefix(self):
        """YYYY-MM-DD without @ should parse as [datetime]."""
        result = parse_arg_value("2026-03-18")
        assert result == [datetime(2026, 3, 18)]

    def test_single_date_matches_at_prefix(self):
        """Result should be identical with or without @ prefix."""
        without = parse_arg_value("2026-03-18")
        with_at = parse_arg_value("@2026-03-18")
        assert without == with_at

    def test_datetime_without_prefix(self):
        """YYYY-MM-DDTHH:MM:SS without @ should parse as datetime."""
        result = parse_arg_value("2026-03-18T10:30:00")
        assert result == datetime(2026, 3, 18, 10, 30, 0)

    def test_datetime_ms_without_prefix(self):
        """YYYY-MM-DDTHH:MM:SS.sss without @ should parse as datetime."""
        result = parse_arg_value("2026-03-18T10:30:00.123")
        assert isinstance(result, datetime)
        assert result.hour == 10
        assert result.minute == 30

    def test_date_range_without_prefix(self):
        """YYYY-MM-DD:YYYY-MM-DD without @ should parse as DateRange."""
        result = parse_arg_value("2026-03-01:2026-03-05")
        assert isinstance(result, DateRange)

    def test_open_date_range_without_prefix(self):
        """YYYY-MM-DD: without @ should parse as DateRange."""
        result = parse_arg_value("2026-03-01:")
        assert isinstance(result, DateRange)

    def test_year_month_without_prefix(self):
        """YYYY-MM without @ should parse as DateRange."""
        result = parse_arg_value("2026-03")
        assert isinstance(result, DateRange)

    def test_date_with_calendar_suffix(self):
        """YYYY-MM-DD~CALENDAR should auto-detect and use the specified calendar."""
        result = parse_arg_value("2026-03-18~ANBIMA")
        assert result == [datetime(2026, 3, 18)]

    def test_plain_string_still_works(self):
        assert parse_arg_value("hello") == "hello"

    def test_integer_still_works(self):
        assert parse_arg_value("42") == 42

    def test_bare_year_is_integer_not_date(self):
        """Bare YYYY (e.g. 2025) should return int, not a date."""
        result = parse_arg_value("2025")
        assert result == 2025
        assert isinstance(result, int)

    def test_at_year_is_date_range(self):
        """@YYYY should return DateRange of business days in that year."""
        result = parse_arg_value("@2025")
        assert isinstance(result, DateRange)
        dates = list(result)
        assert len(dates) > 0
        assert all(d.year == 2025 for d in dates)

    def test_invalid_date_falls_through(self):
        """Invalid dates like 2026-13 should fall through to scalar parsing."""
        result = parse_arg_value("2026-13")
        assert result == "2026-13"


class TestNamedDateVariables:
    """@today and @yesterday should return datetime values."""

    def test_at_today(self):
        result = parse_arg_value("@today")
        today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        assert result == today
        assert isinstance(result, datetime)

    def test_at_yesterday(self):
        result = parse_arg_value("@yesterday")
        yesterday = datetime.today().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)
        assert result == yesterday
        assert isinstance(result, datetime)


class TestDatetimeParsing:
    """DateRangeParser datetime and datetime_ms methods."""

    def test_datetime_via_at_prefix(self):
        result = parse_arg_value("@2026-03-18T10:30:00")
        assert result == datetime(2026, 3, 18, 10, 30, 0)

    def test_datetime_ms_via_at_prefix(self):
        result = parse_arg_value("@2026-03-18T10:30:00.500")
        assert isinstance(result, datetime)
        assert result.hour == 10


class TestRefdateConvergence:
    """CLI --arg and plan paths must produce identical refdate types."""

    def test_cli_arg_refdate_type_matches_plan(self):
        """--arg refdate=YYYY-MM-DD should produce same type as plan refdate."""
        from brasa.engine.download_plan import _resolve_task_refdate

        # CLI path: parse_arg_value
        cli_refdate = parse_arg_value("2026-03-18")

        # Plan path: _resolve_task_refdate with string from YAML
        plan_refdate = _resolve_task_refdate({"refdate": "2026-03-18"}, None, "B3")

        assert type(cli_refdate) is type(plan_refdate)
        assert cli_refdate == plan_refdate

    def test_cli_at_prefix_matches_plan(self):
        """@YYYY-MM-DD should also match plan path."""
        from brasa.engine.download_plan import _resolve_task_refdate

        cli_refdate = parse_arg_value("@2026-03-18")
        plan_refdate = _resolve_task_refdate({"refdate": "2026-03-18"}, None, "B3")

        assert type(cli_refdate) is type(plan_refdate)
        assert cli_refdate == plan_refdate


# --- merged from tests/test_unzip_util.py ---


def _make_zip_file(path, members):
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return path


def test_unzip_file_to_extracts_members(tmp_path):
    archive = _make_zip_file(
        tmp_path / "ok.zip", {"a.txt": "alpha", "sub/b.txt": "beta"}
    )
    dest = tmp_path / "out"
    dest.mkdir()

    files = unzip_file_to(str(archive), str(dest))

    assert sorted(f.rsplit("/", 1)[-1] for f in files) == ["a.txt", "b.txt"]
    assert (dest / "a.txt").read_text() == "alpha"
    assert (dest / "sub" / "b.txt").read_text() == "beta"


def test_unzip_file_to_rejects_path_traversal(tmp_path):
    archive = _make_zip_file(
        tmp_path / "evil.zip", {"../evil.txt": "gotcha", "ok.txt": "fine"}
    )
    dest = tmp_path / "out"
    dest.mkdir()

    with pytest.raises(ValueError, match="outside"):
        unzip_file_to(str(archive), str(dest))

    assert not (tmp_path / "evil.txt").exists()


def test_unzip_and_get_content_reads_member(tmp_path):
    archive = _make_zip_file(tmp_path / "ok.zip", {"a.txt": "alpha"})
    assert unzip_and_get_content(str(archive)) == b"alpha"
    assert unzip_and_get_content(str(archive), encode=True, encoding="utf-8") == "alpha"


def test_is_zip_accepts_path_objects(tmp_path):
    archive = _make_zip_file(tmp_path / "ok.zip", {"a.txt": "alpha"})
    assert _is_zip(archive)
    assert _is_zip(str(archive))
    assert not _is_zip(str(tmp_path / "missing.zip"))


# --- merged from tests/test_zip_checksum.py ---
# In-memory zips via BytesIO so each test controls both content and
# non-deterministic metadata (date_time, OS byte via ZipInfo, entry order).


def _make_zip_bytes(
    entries: list[tuple[str, bytes]],
    date_time: tuple[int, int, int, int, int, int] = (2020, 1, 1, 0, 0, 0),
) -> BytesIO:
    """Build an in-memory zip from an ordered list of (name, content) pairs.

    Using a list (not a dict) preserves insertion order so tests can
    deliberately reorder entries in the central directory.
    """
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries:
            info = zipfile.ZipInfo(name, date_time=date_time)
            zf.writestr(info, data)
    buf.seek(0)
    return buf


def test_identical_content_different_metadata_same_checksum():
    z1 = _make_zip_bytes(
        [("a.txt", b"hello"), ("b.txt", b"world")],
        date_time=(2020, 1, 1, 0, 0, 0),
    )
    z2 = _make_zip_bytes(
        [("a.txt", b"hello"), ("b.txt", b"world")],
        date_time=(2024, 6, 15, 12, 30, 45),
    )
    assert z1.getvalue() != z2.getvalue()  # raw bytes differ
    assert generate_checksum_from_zip(z1) == generate_checksum_from_zip(z2)


def test_reordered_entries_same_checksum():
    z1 = _make_zip_bytes([("a.txt", b"one"), ("b.txt", b"two"), ("c.txt", b"three")])
    z2 = _make_zip_bytes([("c.txt", b"three"), ("a.txt", b"one"), ("b.txt", b"two")])
    assert generate_checksum_from_zip(z1) == generate_checksum_from_zip(z2)


def test_different_content_different_checksum():
    z1 = _make_zip_bytes([("a.txt", b"hello")])
    z2 = _make_zip_bytes([("a.txt", b"HELLO")])
    assert generate_checksum_from_zip(z1) != generate_checksum_from_zip(z2)


def test_adding_a_file_changes_checksum():
    z1 = _make_zip_bytes([("a.txt", b"x")])
    z2 = _make_zip_bytes([("a.txt", b"x"), ("b.txt", b"y")])
    assert generate_checksum_from_zip(z1) != generate_checksum_from_zip(z2)


def test_nested_zip_stable_across_container_rebuilds():
    inner1 = _make_zip_bytes(
        [("inner.txt", b"data")], date_time=(2020, 1, 1, 0, 0, 0)
    ).getvalue()
    inner2 = _make_zip_bytes(
        [("inner.txt", b"data")], date_time=(2024, 6, 15, 12, 30, 45)
    ).getvalue()
    assert inner1 != inner2
    outer1 = _make_zip_bytes([("nested.zip", inner1)], date_time=(2020, 1, 1, 0, 0, 0))
    outer2 = _make_zip_bytes(
        [("nested.zip", inner2)], date_time=(2024, 6, 15, 12, 30, 45)
    )
    assert generate_checksum_from_zip(outer1) == generate_checksum_from_zip(outer2)


def test_nested_zip_different_inner_content_different_checksum():
    inner_a = _make_zip_bytes([("x.txt", b"A")]).getvalue()
    inner_b = _make_zip_bytes([("x.txt", b"B")]).getvalue()
    outer_a = _make_zip_bytes([("nested.zip", inner_a)])
    outer_b = _make_zip_bytes([("nested.zip", inner_b)])
    assert generate_checksum_from_zip(outer_a) != generate_checksum_from_zip(outer_b)


def test_fp_seek_restored_to_zero():
    z = _make_zip_bytes([("a.txt", b"hello")])
    generate_checksum_from_zip(z)
    assert z.tell() == 0


def test_non_zip_fp_raises():
    fp = BytesIO(b"not a zip file at all")
    with pytest.raises(zipfile.BadZipFile):
        generate_checksum_from_zip(fp)


def test_recursion_depth_cap():
    payload = _make_zip_bytes([("leaf.txt", b"bottom")]).getvalue()
    for _ in range(10):
        payload = _make_zip_bytes([("nested.zip", payload)]).getvalue()
    with pytest.raises(RecursionError):
        generate_checksum_from_zip(BytesIO(payload))


# --- merged from tests/test_download_args.py ---


class TestNormalize:
    def test_datetime_is_formatted_as_iso_string(self):
        args = DownloadArgs({"refdate": datetime(2024, 1, 8)})
        assert args["refdate"] == "2024-01-08T00:00:00"

    def test_date_is_formatted_as_iso_string(self):
        args = DownloadArgs({"refdate": date(2024, 1, 8)})
        assert args["refdate"] == "2024-01-08T00:00:00"

    def test_bare_date_string_is_upcasted(self):
        args = DownloadArgs({"refdate": "2000-01-01"})
        assert args["refdate"] == "2000-01-01T00:00:00"

    def test_full_datetime_string_is_unchanged(self):
        args = DownloadArgs({"refdate": "2024-01-08T00:00:00"})
        assert args["refdate"] == "2024-01-08T00:00:00"

    def test_other_string_is_unchanged(self):
        args = DownloadArgs({"code": "today"})
        assert args["code"] == "today"

    def test_integer_is_unchanged(self):
        args = DownloadArgs({"series_id": 4398})
        assert args["series_id"] == 4398

    def test_empty_dict(self):
        args = DownloadArgs({})
        assert list(args.keys()) == []


class TestGetObject:
    def test_date_string_returns_datetime(self):
        args = DownloadArgs({"refdate": "2024-01-08T00:00:00"})
        obj = args.get_object("refdate")
        assert isinstance(obj, datetime)
        assert obj == datetime(2024, 1, 8)

    def test_non_date_string_returns_as_is(self):
        args = DownloadArgs({"code": "today"})
        assert args.get_object("code") == "today"

    def test_integer_returns_as_is(self):
        args = DownloadArgs({"series_id": 4398})
        assert args.get_object("series_id") == 4398


class TestSerialization:
    def test_to_json_roundtrip(self):
        args = DownloadArgs({"refdate": datetime(2024, 1, 8), "code": "abc"})
        restored = DownloadArgs.from_json(args.to_json())
        assert restored["refdate"] == "2024-01-08T00:00:00"
        assert restored["code"] == "abc"

    def test_from_json_does_not_reconvert_datetime_strings(self):
        args = DownloadArgs({"refdate": "2024-01-08T00:00:00"})
        restored = DownloadArgs.from_json(args.to_json())
        # Must still be a string, NOT a datetime object
        assert isinstance(restored["refdate"], str)
        assert restored["refdate"] == "2024-01-08T00:00:00"

    def test_from_json_normalizes_bare_date_strings(self):
        """Existing DB rows with bare dates are normalized on load."""
        raw_json = json.dumps({"refdate": "2000-01-01"})
        restored = DownloadArgs.from_json(raw_json)
        assert restored["refdate"] == "2000-01-01T00:00:00"

    def test_to_dict(self):
        args = DownloadArgs({"refdate": datetime(2024, 1, 8), "x": 1})
        d = args.to_dict()
        assert d == {"refdate": datetime(2024, 1, 8), "x": 1}
        assert isinstance(d, dict)


class TestDictInterface:
    def test_contains(self):
        args = DownloadArgs({"a": 1})
        assert "a" in args
        assert "b" not in args

    def test_get_with_default(self):
        args = DownloadArgs({"a": 1})
        assert args.get("a") == 1
        assert args.get("b", 99) == 99

    def test_items(self):
        args = DownloadArgs({"a": 1, "b": 2})
        assert set(args.items()) == {("a", 1), ("b", 2)}

    def test_iter(self):
        args = DownloadArgs({"a": 1, "b": 2})
        assert set(args) == {"a", "b"}


class TestHashStability:
    """The core requirement: same date in any form -> same hash."""

    def test_all_date_forms_produce_same_hash(self):
        template = "bcb-sgs"
        h1 = generate_checksum_for_template(
            template, DownloadArgs({"refdate": "2000-01-01"})
        )
        h2 = generate_checksum_for_template(
            template, DownloadArgs({"refdate": "2000-01-01T00:00:00"})
        )
        h3 = generate_checksum_for_template(
            template, DownloadArgs({"refdate": date(2000, 1, 1)})
        )
        h4 = generate_checksum_for_template(
            template, DownloadArgs({"refdate": datetime(2000, 1, 1)})
        )
        assert h1 == h2 == h3 == h4


# --- merged from tests/test_suppress_warnings.py ---


def test_suppresses_user_warnings_inside_block(recwarn):
    with SuppressUserWarnings():
        warnings.warn("hidden", UserWarning, stacklevel=1)
    assert len(recwarn) == 0


def test_restores_previous_filters_on_exit():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        before = list(warnings.filters)
        with SuppressUserWarnings():
            pass
        assert warnings.filters == before


# --- merged from tests/test_period.py ---


def test_start_end_period() -> None:
    p = DateRange(start=datetime(2023, 1, 1), end=datetime(2023, 1, 31), calendar="B3")
    assert p.start == datetime(2023, 1, 2)
    assert p.end == datetime(2023, 1, 31)


def test_start_no_end_period() -> None:
    cal = Calendar.load("B3")
    p = DateRange(start=datetime(2023, 1, 1), calendar="B3")
    assert p.start == datetime(2023, 1, 2)
    assert p.end == cal.offset(datetime.today(), -1)


def test_year_period() -> None:
    p = DateRange(year=2022, calendar="B3")
    assert p.start == datetime(2022, 1, 3)
    assert p.end == datetime(2022, 12, 29)


def test_current_year_period() -> None:
    year = datetime.today().year
    p = DateRange(year=year, calendar="B3")
    cal = Calendar.load("B3")
    assert p.start == cal.getdate("first bizday", year)
    assert p.end == cal.offset(datetime.today(), -1)


def test_if_is_iter() -> None:
    p = DateRange(year=2022, calendar="B3")
    assert is_iterable(p)


# --- merged from tests/test_arg_dsl.py ---


class TestPlainStrings:
    def test_plain_string(self):
        assert parse_arg_value("IBOV") == "IBOV"

    def test_plain_string_lowercase(self):
        assert parse_arg_value("pt-br") == "pt-br"

    def test_empty_string(self):
        assert parse_arg_value("") == ""


class TestIntegers:
    def test_positive_integer(self):
        result = parse_arg_value("2026")
        assert result == 2026
        assert isinstance(result, int)

    def test_zero(self):
        result = parse_arg_value("0")
        assert result == 0
        assert isinstance(result, int)


class TestCommaLists:
    def test_string_list(self):
        assert parse_arg_value("IBOV,BOVA11") == ["IBOV", "BOVA11"]

    def test_integer_list(self):
        assert parse_arg_value("2024,2025,2026") == [2024, 2025, 2026]

    def test_mixed_list(self):
        assert parse_arg_value("IBOV,2026") == ["IBOV", 2026]

    def test_single_element_no_list(self):
        """No comma means no list, just a scalar."""
        assert parse_arg_value("IBOV") == "IBOV"


class TestDatePrefix:
    def test_single_date(self):
        result = parse_arg_value("@2026-03-06")
        assert result == [datetime(2026, 3, 6)]

    def test_date_range_month(self):
        result = parse_arg_value("@2026-01")
        # DateRangeParser returns a DateRange for month patterns
        assert hasattr(result, "__iter__")
        dates = list(result)
        assert len(dates) > 0
        assert all(isinstance(d, datetime) for d in dates)

    def test_date_range_explicit(self):
        result = parse_arg_value("@2026-01-01:2026-01-03")
        assert hasattr(result, "__iter__")

    def test_calendar_override(self):
        result = parse_arg_value("@2026-01~ANBIMA")
        assert hasattr(result, "__iter__")
        dates = list(result)
        assert len(dates) > 0

    def test_default_calendar_used(self):
        r1 = parse_arg_value("@2026-01", default_calendar="B3")
        r2 = parse_arg_value("@2026-01", default_calendar="ANBIMA")
        # Different calendars may produce different date lists
        # Just verify both resolve without error
        assert hasattr(r1, "__iter__")
        assert hasattr(r2, "__iter__")


class TestSymbolPrefix:
    @patch("brasa.queries.get_symbols", return_value=["IBOV", "SMLL"])
    def test_symbol_lookup(self, mock_get_symbols):
        result = parse_arg_value("$index")
        assert result == ["IBOV", "SMLL"]
        mock_get_symbols.assert_called_once_with("index")

    @patch("brasa.queries.get_symbols", return_value=["ABEV", "PETR"])
    def test_symbol_lookup_company(self, mock_get_symbols):
        result = parse_arg_value("$company")
        assert result == ["ABEV", "PETR"]
        mock_get_symbols.assert_called_once_with("company")


# --- merged from tests/test_smart_kwargs.py ---


def test_smart_kwargs() -> None:
    kwargs = {
        "name": "test",
        "color": ["red", "blue", "green"],
    }

    args = KwargsIterator(kwargs)
    kwargs_list = list(args)
    assert isinstance(kwargs_list, list)


def test_smart_kwargs2() -> None:
    kwargs = {
        "refdate": DateRange(year=2020),
    }

    args = KwargsIterator(kwargs)
    kwargs_dict = list(args)
    assert isinstance(kwargs_dict, list)


# --- merged from tests/test_date_normalization.py ---


class TestJsonConvertFromObject:
    def test_date_serializes_with_time_component(self):
        result = json_convert_from_object(date(2025, 3, 12))
        assert result == "2025-03-12T00:00:00"

    def test_datetime_serializes_with_time_component(self):
        result = json_convert_from_object(datetime(2025, 3, 12))
        assert result == "2025-03-12T00:00:00"

    def test_date_and_datetime_serialize_identically(self):
        d = json_convert_from_object(date(2025, 3, 12))
        dt = json_convert_from_object(datetime(2025, 3, 12))
        assert d == dt


class TestJsonRoundTrip:
    def test_date_round_trips_as_datetime(self):
        serialized = json.dumps(
            {"refdate": date(2025, 3, 12)}, default=json_convert_from_object
        )
        deserialized = json.loads(serialized, object_hook=json_convert_to_object)
        assert deserialized["refdate"] == datetime(2025, 3, 12)
        assert isinstance(deserialized["refdate"], datetime)

    def test_datetime_round_trips_as_datetime(self):
        serialized = json.dumps(
            {"refdate": datetime(2025, 3, 12)}, default=json_convert_from_object
        )
        deserialized = json.loads(serialized, object_hook=json_convert_to_object)
        assert deserialized["refdate"] == datetime(2025, 3, 12)
        assert isinstance(deserialized["refdate"], datetime)


class TestChecksumNormalization:
    def test_date_and_datetime_produce_same_checksum(self):
        hash_date = generate_checksum_for_template(
            "tpl", DownloadArgs({"refdate": date(2025, 3, 12)})
        )
        hash_datetime = generate_checksum_for_template(
            "tpl", DownloadArgs({"refdate": datetime(2025, 3, 12)})
        )
        assert hash_date == hash_datetime

    def test_different_dates_produce_different_checksums(self):
        h1 = generate_checksum_for_template(
            "tpl", DownloadArgs({"refdate": date(2025, 3, 12)})
        )
        h2 = generate_checksum_for_template(
            "tpl", DownloadArgs({"refdate": date(2025, 3, 13)})
        )
        assert h1 != h2
