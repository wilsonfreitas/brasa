from datetime import date

import pytest

from brasa.fieldset_schema.exceptions import FieldsetError, TypeParseError
from brasa.fieldset_schema.field import Field
from brasa.fieldset_schema.fieldset import Fieldset


@pytest.fixture
def sample_fields():
    return [
        Field("id", "Unique ID", "integer", required=True),
        Field("name", "Item Name", "string", max_length=50),
        Field("price", "Item Price", "numeric(dec=2)"),
        Field("date", "Creation Date", "date(format='%Y-%m-%d')"),
        Field("is_active", "Active Status", "boolean"),
    ]


def test_fieldset_creation_basic():
    fs = Fieldset()
    assert fs.name is None
    assert fs.description is None
    assert len(fs) == 0


def test_fieldset_creation_with_name_desc():
    fs = Fieldset(name="my_fieldset", description="A test fieldset")
    assert fs.name == "my_fieldset"
    assert fs.description == "A test fieldset"


def test_fieldset_add_field(sample_fields):
    fs = Fieldset()
    fs.add_field(sample_fields[0])
    assert len(fs) == 1
    assert fs.has_field("id")
    assert fs.get_field("id") == sample_fields[0]


def test_fieldset_add_fields(sample_fields):
    fs = Fieldset()
    fs.add_fields(*sample_fields)
    assert len(fs) == 5
    assert fs.has_field("id")
    assert fs.has_field("name")
    assert fs.has_field("is_active")


def test_fieldset_add_field_invalid_type():
    fs = Fieldset()
    with pytest.raises(FieldsetError, match="Expected Field instance"):
        fs.add_field("not_a_field")  # type: ignore


def test_fieldset_get_field(sample_fields):
    fs = Fieldset()
    fs.add_fields(*sample_fields)

    field = fs.get_field("price")
    assert field.name == "price"
    assert field.type_name == "numeric"


def test_fieldset_get_field_not_found():
    fs = Fieldset()
    with pytest.raises(FieldsetError, match="Field 'non_existent' not found"):
        fs.get_field("non_existent")


def test_fieldset_has_field():
    fs = Fieldset()
    fs.add_field(Field("id", "ID", "integer"))
    assert fs.has_field("id")
    assert not fs.has_field("name")


def test_fieldset_remove_field(sample_fields):
    fs = Fieldset()
    fs.add_fields(*sample_fields)

    removed_field = fs.remove_field("name")
    assert removed_field.name == "name"
    assert len(fs) == 4
    assert not fs.has_field("name")
    assert "name" not in fs.get_field_names()


def test_fieldset_remove_field_not_found():
    fs = Fieldset()
    with pytest.raises(FieldsetError, match="Field 'non_existent' not found"):
        fs.remove_field("non_existent")


def test_fieldset_get_all_fields(sample_fields):
    fs = Fieldset()
    fs.add_fields(sample_fields[0], sample_fields[1])
    fs.add_field(sample_fields[3])  # Add date field later

    all_fields = fs.get_all_fields()
    assert len(all_fields) == 3
    assert all_fields[0].name == "id"
    assert all_fields[1].name == "name"
    assert all_fields[2].name == "date"  # Check order


def test_fieldset_get_field_names():
    fs = Fieldset()
    fs.add_field(Field("a", "A", "string"))
    fs.add_field(Field("b", "B", "string"))
    assert fs.get_field_names() == ["a", "b"]


def test_fieldset_get_fields_by_type(sample_fields):
    fs = Fieldset()
    fs.add_fields(*sample_fields)

    int_fields = fs.get_fields_by_type("integer")
    assert len(int_fields) == 1
    assert int_fields[0].name == "id"

    string_fields = fs.get_fields_by_type("string")
    assert len(string_fields) == 1
    assert string_fields[0].name == "name"

    numeric_fields = fs.get_fields_by_type("numeric")
    assert len(numeric_fields) == 1
    assert numeric_fields[0].name == "price"


def test_fieldset_parse_record(sample_fields):
    fs = Fieldset()
    fs.add_fields(*sample_fields)

    record = {
        "id": "123",
        "name": "Test Item",
        "price": "9999",  # numeric(dec=2)
        "date": "2023-10-26",
        "is_active": "true",
    }

    parsed_record = fs.parse_record(record)
    assert parsed_record["id"] == 123
    assert parsed_record["name"] == "Test Item"
    assert parsed_record["price"] == 99.99
    assert parsed_record["date"] == date(2023, 10, 26)
    assert parsed_record["is_active"] is True


def test_fieldset_parse_record_unknown_field():
    fs = Fieldset()
    fs.add_field(Field("id", "ID", "integer"))

    record = {"id": "1", "unknown_field": "value"}
    with pytest.raises(FieldsetError, match="Unknown field 'unknown_field' in record"):
        fs.parse_record(record)


def test_fieldset_parse_record_parsing_error():
    fs = Fieldset()
    fs.add_field(Field("id", "ID", "integer"))

    record = {"id": "not_an_int"}
    with pytest.raises(TypeParseError, match="Error parsing field 'id'"):
        fs.parse_record(record)


def test_fieldset_validate_record():
    fs = Fieldset()
    fs.add_field(Field("id", "ID", "integer"))
    fs.add_field(Field("name", "Name", "string"))

    valid_record = {"id": "123", "name": "Valid"}
    invalid_record_type = {"id": "abc", "name": "Invalid"}
    invalid_record_unknown_field = {"id": "1", "unknown": "field"}

    assert fs.validate_record(valid_record) == {"id": True, "name": True}
    assert fs.validate_record(invalid_record_type) == {"id": False, "name": True}
    assert fs.validate_record(invalid_record_unknown_field) == {
        "id": True,
        "unknown": False,
    }


def test_fieldset_clear(sample_fields):
    fs = Fieldset()
    fs.add_fields(*sample_fields)
    assert len(fs) == 5

    fs.clear()
    assert len(fs) == 0
    assert not fs.has_field("id")


def test_fieldset_len_contains_getitem_iter(sample_fields):
    fs = Fieldset()
    assert len(fs) == 0

    fs.add_fields(sample_fields[0], sample_fields[1])
    assert len(fs) == 2

    assert "id" in fs
    assert "non_existent" not in fs

    assert fs["id"] == sample_fields[0]
    with pytest.raises(FieldsetError):
        _ = fs["non_existent"]

    field_names_iter = [f.name for f in fs]
    assert field_names_iter == ["id", "name"]


def test_fieldset_repr_str():
    fs = Fieldset(name="my_fs", description="A description")
    fs.add_field(Field("f1", "d1", "string"))
    fs.add_field(Field("f2", "d2", "integer"))

    assert repr(fs) == "Fieldset(name='my_fs', 2 fields)"
    expected_str = (
        "Fieldset: my_fs\n"
        "Description: A description\n"
        "Fields (2):\n"
        "  - f1 (string): d1\n"
        "  - f2 (integer): d2"
    )
    assert str(fs) == expected_str


def test_fieldset_to_dict(sample_fields):
    fs = Fieldset(name="test_fs", description="Test FS")
    fs.add_fields(sample_fields[0], sample_fields[1])

    expected_dict = {
        "name": "test_fs",
        "description": "Test FS",
        "fields": [
            {
                "name": "id",
                "description": "Unique ID",
                "type": "integer",
                "required": True,
            },
            {
                "name": "name",
                "description": "Item Name",
                "type": "string",
                "max_length": 50,
            },
        ],
    }
    assert fs.to_dict() == expected_dict


def test_fieldset_from_dict():
    data = {
        "name": "from_dict_fs",
        "description": "FS from dict",
        "fields": [
            {"name": "f1", "description": "Field 1", "type": "string"},
            {"name": "f2", "description": "Field 2", "type": "integer", "min_val": 0},
        ],
    }
    fs = Fieldset.from_dict(data)
    assert fs.name == "from_dict_fs"
    assert fs.description == "FS from dict"
    assert len(fs) == 2
    assert fs.get_field("f1").type_name == "string"
    assert fs.get_field("f2").get_attribute("min_val") == 0


def test_fieldset_from_dict_invalid_fields_type():
    data = {"name": "bad_fs", "fields": "not_a_list"}
    with pytest.raises(FieldsetError, match="Fieldset 'fields' must be a list."):
        Fieldset.from_dict(data)


def test_fieldset_from_dict_invalid_field_item_type():
    data = {"name": "bad_fs", "fields": ["not_a_dict"]}
    with pytest.raises(
        FieldsetError, match="Each field in 'fields' list must be a dictionary."
    ):
        Fieldset.from_dict(data)
