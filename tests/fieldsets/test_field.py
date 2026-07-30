from datetime import date

import pytest

from brasa.fieldsets.field import Field


def test_field_creation_basic():
    field = Field("test_name", "Test description", "string")
    assert field.name == "test_name"
    assert field.description == "Test description"
    assert field.type_definition == "string"
    assert field.type_name == "string"
    assert field.parser is not None


def test_field_creation_with_type_params():
    field = Field("test_date", "A date field", "date(format='%Y%m%d')")
    assert field.name == "test_date"
    assert field.type_definition == "date(format='%Y%m%d')"
    assert field.type_name == "date"
    assert field.parser.parameters == {"format": "%Y%m%d"}


def test_field_creation_with_extra_attributes():
    field = Field("test_field", "Desc", "integer", required=True, default_value=0)
    assert field.get_attribute("required") is True
    assert field.get_attribute("default_value") == 0


def test_field_creation_invalid_name():
    with pytest.raises(ValueError, match="Field name must be a non-empty string"):
        Field("", "Desc", "string")
    with pytest.raises(ValueError, match="Field name must be a non-empty string"):
        Field("   ", "Desc", "string")


def test_field_creation_invalid_type_definition():
    with pytest.raises(
        ValueError, match="Invalid type definition for field 'bad_field'"
    ):
        Field("bad_field", "Desc", "unknown_type")
    with pytest.raises(
        ValueError, match="Invalid type definition for field 'bad_field'"
    ):
        Field("bad_field", "Desc", "date(invalid_param)")


def test_field_description_setter():
    field = Field("name", "Old desc", "string")
    field.description = "New description"
    assert field.description == "New description"


def test_field_parse_method():
    date_field = Field("my_date", "Date", "date(format='%d/%m/%Y')")
    parsed_date = date_field.parse("26/10/2023")
    assert parsed_date == date(2023, 10, 26)
    assert isinstance(parsed_date, date)

    int_field = Field("my_int", "Integer", "integer")
    assert int_field.parse("123") == 123


def test_field_parse_method_error():
    date_field = Field("my_date", "Date", "date(format='%d/%m/%Y')")
    with pytest.raises(ValueError, match="Error parsing field 'my_date'"):
        date_field.parse("2023-10-26")


def test_field_set_attribute_protected_name():
    field = Field("test", "Desc", "string")
    with pytest.raises(ValueError, match="conflicts with protected attribute"):
        field.set_attribute("_name", "new_name")
    with pytest.raises(ValueError, match="conflicts with protected attribute"):
        field.set_attribute("_extra_attributes", {})


def test_field_from_dict():
    data = {
        "name": "from_dict_field",
        "description": "Created from dict",
        "type": "integer",
        "min_value": 0,
    }
    field = Field.from_dict(data)
    assert field.name == "from_dict_field"
    assert field.description == "Created from dict"
    assert field.type_definition == "integer"
    assert field.get_attribute("min_value") == 0


def test_field_from_dict_missing_keys():
    with pytest.raises(ValueError, match="Missing required keys for Field"):
        Field.from_dict({"name": "bad_field", "type": "string"})


def test_field_repr_str():
    field = Field("my_field", "My description", "string", custom_attr=1)
    assert repr(field) == "Field(name='my_field', type='string', 1 custom attrs)"
    assert str(field) == "my_field (string): My description"


def test_field_equality():
    field1 = Field("name1", "desc1", "string")
    field2 = Field("name1", "desc2", "integer")  # Same name, different other attributes
    field3 = Field("name3", "desc3", "string")

    assert field1 == field2
    assert field1 != field3
    assert field1 != "not a field"
