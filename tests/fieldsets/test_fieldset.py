import pytest

from brasa.fieldsets.field import Field
from brasa.fieldsets.fieldset import Fieldset


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
    assert "id" in fs
    assert fs.get_field("id") == sample_fields[0]


def test_fieldset_add_fields(sample_fields):
    fs = Fieldset()
    fs.add_fields(*sample_fields)
    assert len(fs) == 5
    assert "id" in fs
    assert "name" in fs
    assert "is_active" in fs


def test_fieldset_add_field_invalid_type():
    fs = Fieldset()
    with pytest.raises(ValueError, match="Expected Field instance"):
        fs.add_field("not_a_field")  # type: ignore


def test_fieldset_get_field(sample_fields):
    fs = Fieldset()
    fs.add_fields(*sample_fields)

    field = fs.get_field("price")
    assert field.name == "price"
    assert field.type_name == "numeric"


def test_fieldset_get_field_not_found():
    fs = Fieldset()
    with pytest.raises(ValueError, match="Field 'non_existent' not found"):
        fs.get_field("non_existent")


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


def test_fieldset_len_contains_getitem_iter(sample_fields):
    fs = Fieldset()
    assert len(fs) == 0

    fs.add_fields(sample_fields[0], sample_fields[1])
    assert len(fs) == 2

    assert "id" in fs
    assert "non_existent" not in fs

    assert fs["id"] == sample_fields[0]
    with pytest.raises(ValueError):
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
