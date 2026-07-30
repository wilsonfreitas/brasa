"""Tests for get_target_schema (PyArrow schema building)."""

import pyarrow as pa

from brasa.fieldsets import Field, Fieldset, get_target_schema


def _make_fieldset() -> Fieldset:
    fs = Fieldset(name="sample")
    fs.add_field(Field("id", "ID", "integer"))
    fs.add_field(Field("name", "Name", "string"))
    fs.add_field(Field("code", "Code", "character"))
    fs.add_field(Field("price", "Price", "numeric(dec=2)"))
    fs.add_field(Field("refdate", "Ref Date", "date(format='%Y-%m-%d')"))
    fs.add_field(Field("updated_at", "Updated", "datetime"))
    fs.add_field(Field("active", "Active", "boolean"))
    return fs


def test_get_target_schema_types():
    """Test that get_target_schema creates correct PyArrow types."""
    schema = get_target_schema(_make_fieldset())
    assert schema.field("id").type == pa.int64()
    assert schema.field("name").type == pa.string()
    assert schema.field("code").type == pa.string()
    assert schema.field("price").type == pa.float64()
    assert schema.field("refdate").type == pa.date32()
    assert schema.field("updated_at").type == pa.timestamp("us")
    assert schema.field("active").type == pa.bool_()


def test_get_target_schema_all_fields_nullable():
    """Test that all fields in schema are nullable."""
    schema = get_target_schema(_make_fieldset())
    assert len(schema) == 7
    assert all(f.nullable for f in schema)
