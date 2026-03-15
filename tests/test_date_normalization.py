"""Tests for date vs datetime normalization in JSON serialization and checksum generation."""

import json
from datetime import date, datetime

from brasa.engine.core import json_convert_from_object, json_convert_to_object
from brasa.util import generate_checksum_for_template


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
            "tpl", {"refdate": date(2025, 3, 12)}
        )
        hash_datetime = generate_checksum_for_template(
            "tpl", {"refdate": datetime(2025, 3, 12)}
        )
        assert hash_date == hash_datetime

    def test_different_dates_produce_different_checksums(self):
        h1 = generate_checksum_for_template("tpl", {"refdate": date(2025, 3, 12)})
        h2 = generate_checksum_for_template("tpl", {"refdate": date(2025, 3, 13)})
        assert h1 != h2
