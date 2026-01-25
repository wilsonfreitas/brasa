import pytest
import yaml

from brasa.fieldsets.exceptions import FieldsetError
from brasa.fieldsets.fieldset import Fieldset


def test_fieldset_from_yaml_string(sample_fieldset_yaml_content):
    fieldset = Fieldset.from_yaml(sample_fieldset_yaml_content)

    assert fieldset.name == "test_schema"
    assert fieldset.description == "A schema for testing purposes"
    assert len(fieldset) == 8

    id_field = fieldset.get_field("id")
    assert id_field.name == "id"
    assert id_field.type_name == "integer"
    assert id_field.get_attribute("required") is True

    date_field = fieldset.get_field("effective_date")
    assert date_field.type_definition == 'date(format="%d/%m/%Y")'
    assert date_field.parser.parameters == {"format": "%d/%m/%Y"}


def test_fieldset_from_yaml_dict(sample_fieldset_yaml_content):
    yaml_dict = yaml.safe_load(sample_fieldset_yaml_content)
    fieldset = Fieldset.from_yaml(yaml_dict)

    assert fieldset.name == "test_schema"
    assert len(fieldset) == 8


def test_fieldset_from_yaml_invalid_content():
    with pytest.raises(FieldsetError, match="Invalid YAML content"):
        Fieldset.from_yaml("this is not valid yaml: -")

    with pytest.raises(FieldsetError, match="YAML content must represent a dictionary"):
        Fieldset.from_yaml("- item1\n- item2")


def test_fieldset_to_yaml(sample_fieldset):
    yaml_output = sample_fieldset.to_yaml()

    # Load the output YAML back to verify
    reloaded_fieldset = Fieldset.from_yaml(yaml_output)

    assert reloaded_fieldset.name == sample_fieldset.name
    assert reloaded_fieldset.description == sample_fieldset.description
    assert len(reloaded_fieldset) == len(sample_fieldset)

    # Check a specific field
    original_id_field = sample_fieldset.get_field("id")
    reloaded_id_field = reloaded_fieldset.get_field("id")
    assert original_id_field.to_dict() == reloaded_id_field.to_dict()

    original_date_field = sample_fieldset.get_field("effective_date")
    reloaded_date_field = reloaded_fieldset.get_field("effective_date")
    assert original_date_field.to_dict() == reloaded_date_field.to_dict()


def test_fieldset_to_yaml_empty_fieldset():
    fs = Fieldset(name="empty_fs")
    yaml_output = fs.to_yaml()
    expected_yaml = "description: null\nfields: []\nname: empty_fs\n"
    assert yaml_output == expected_yaml
