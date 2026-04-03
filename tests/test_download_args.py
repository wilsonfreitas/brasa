"""Tests for DownloadArgs normalization and serialization."""

from datetime import date, datetime

from brasa.util import DownloadArgs, generate_checksum_for_template


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
        import json

        raw_json = json.dumps({"refdate": "2000-01-01"})
        restored = DownloadArgs.from_json(raw_json)
        assert restored["refdate"] == "2000-01-01T00:00:00"

    def test_to_dict(self):
        args = DownloadArgs({"refdate": datetime(2024, 1, 8), "x": 1})
        d = args.to_dict()
        assert d == {"refdate": "2024-01-08T00:00:00", "x": 1}
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
