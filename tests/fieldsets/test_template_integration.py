"""
Tests for integration between fieldsets and template system.
"""

import pandas as pd

from brasa.engine import TemplateFields, retrieve_template
from brasa.fieldsets import Field, Fieldset
from brasa.fieldsets.adapters import PandasAdapter


class TestFromTemplateFields:
    """Test Fieldset.from_template_fields() classmethod."""

    def test_from_template_fields_basic(self):
        """Test creating Fieldset from TemplateFields with basic types."""
        # Create a simple TemplateFields from dict structure
        # TemplateField expects 'handler' with 'type' inside it
        fields_data = [
            {
                "name": "refdate",
                "description": "Reference date",
                "handler": {"type": "date"},
            },
            {
                "name": "symbol",
                "description": "Symbol",
                "handler": {"type": "character"},
            },
            {"name": "volume", "description": "Volume", "handler": {"type": "numeric"}},
            {
                "name": "quantity",
                "description": "Quantity",
                "handler": {"type": "numeric"},
            },  # integer maps to numeric
        ]
        template_fields = TemplateFields(fields_data)

        # Convert to Fieldset
        fieldset = Fieldset.from_template_fields(template_fields)

        # Check that all fields were created
        assert len(fieldset) == 4
        assert fieldset.has_field("refdate")
        assert fieldset.has_field("symbol")
        assert fieldset.has_field("volume")
        assert fieldset.has_field("quantity")

        # Check field types
        assert fieldset.get_field("refdate").type_name == "date"
        assert fieldset.get_field("symbol").type_name == "string"
        assert fieldset.get_field("volume").type_name == "numeric"
        assert fieldset.get_field("quantity").type_name == "numeric"

    def test_from_template_fields_preserves_descriptions(self):
        """Test that field descriptions are preserved."""
        fields_data = [
            {
                "name": "price",
                "description": "Price in BRL",
                "handler": {"type": "numeric"},
            },
        ]
        template_fields = TemplateFields(fields_data)
        fieldset = Fieldset.from_template_fields(template_fields)

        field = fieldset.get_field("price")
        assert field.description == "Price in BRL"

    def test_from_template_fields_with_real_template(self):
        """Test with real b3-bvbg086 template."""
        template = retrieve_template("b3-bvbg086")
        # Template.fields is now a Fieldset directly
        fieldset = template.fields

        # Check that fieldset was created
        assert len(fieldset) > 0

        # Check some expected fields
        assert fieldset.has_field("refdate")
        assert fieldset.has_field("symbol")
        assert fieldset.has_field("volume")
        assert fieldset.has_field("open")
        assert fieldset.has_field("close")


class TestApplyTypes:
    """Test PandasAdapter.apply_types() method."""

    def test_apply_types_basic(self):
        """Test applying types to a DataFrame with string columns."""
        # Create fieldset
        fieldset = Fieldset()
        fieldset.add_field(Field("date_col", "Date column", "date"))
        fieldset.add_field(Field("num_col", "Numeric column", "numeric"))
        fieldset.add_field(Field("int_col", "Integer column", "integer"))
        fieldset.add_field(Field("str_col", "String column", "string"))

        # Create DataFrame with all string columns
        df = pd.DataFrame(
            {
                "date_col": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "num_col": ["1.5", "2.7", "3.9"],
                "int_col": ["10", "20", "30"],
                "str_col": ["ABC", "DEF", "GHI"],
            }
        )

        # Apply types
        adapter = PandasAdapter(fieldset)
        df_typed = adapter.apply_types(df)

        # Check types
        assert pd.api.types.is_datetime64_any_dtype(df_typed["date_col"])
        assert pd.api.types.is_numeric_dtype(df_typed["num_col"])
        assert pd.api.types.is_integer_dtype(df_typed["int_col"])
        assert df_typed["str_col"].dtype == "string"

        # Check values
        assert df_typed["date_col"].iloc[0] == pd.Timestamp("2024-01-01")
        assert df_typed["num_col"].iloc[0] == 1.5
        assert df_typed["int_col"].iloc[0] == 10
        assert df_typed["str_col"].iloc[0] == "ABC"

    def test_apply_types_with_missing_columns(self):
        """Test that apply_types skips fields not in DataFrame."""
        fieldset = Fieldset()
        fieldset.add_field(Field("existing", "Existing", "numeric"))
        fieldset.add_field(Field("missing", "Missing", "numeric"))

        df = pd.DataFrame(
            {
                "existing": ["1.5", "2.5"],
            }
        )

        adapter = PandasAdapter(fieldset)
        df_typed = adapter.apply_types(df)

        # Should convert existing column
        assert pd.api.types.is_numeric_dtype(df_typed["existing"])
        # Should not add missing column
        assert "missing" not in df_typed.columns

    def test_apply_types_error_handling_coerce(self):
        """Test error handling with errors='coerce'."""
        fieldset = Fieldset()
        fieldset.add_field(Field("num_col", "Numeric", "numeric"))

        df = pd.DataFrame(
            {
                "num_col": ["1.5", "invalid", "3.5"],
            }
        )

        adapter = PandasAdapter(fieldset, errors="coerce")
        df_typed = adapter.apply_types(df)

        # Invalid value should become NaN
        assert df_typed["num_col"].iloc[0] == 1.5
        assert pd.isna(df_typed["num_col"].iloc[1])
        assert df_typed["num_col"].iloc[2] == 3.5

    def test_apply_types_preserves_original_df(self):
        """Test that apply_types doesn't modify the original DataFrame."""
        fieldset = Fieldset()
        fieldset.add_field(Field("num_col", "Numeric", "numeric"))

        df_original = pd.DataFrame(
            {
                "num_col": ["1.5", "2.5"],
            }
        )
        original_dtype = df_original["num_col"].dtype

        adapter = PandasAdapter(fieldset)
        df_typed = adapter.apply_types(df_original)

        # Original should be unchanged
        assert df_original["num_col"].dtype == original_dtype
        # New should be converted
        assert pd.api.types.is_numeric_dtype(df_typed["num_col"])


class TestTemplateIntegrationEndToEnd:
    """Test end-to-end template integration workflow."""

    def test_template_to_fieldset_to_adapter(self):
        """Test complete workflow: template -> fieldset -> adapter."""
        # Get template
        template = retrieve_template("b3-bvbg086")

        # Template.fields is now a Fieldset directly
        fieldset = template.fields

        # Create adapter
        adapter = PandasAdapter(fieldset)

        # Create sample DataFrame (simulating parser output)
        df = pd.DataFrame(
            {
                "refdate": ["2024-01-01"],
                "symbol": ["PETR4"],
                "volume": ["1000000.50"],
                "open": ["25.50"],
                "close": ["26.75"],
            }
        )

        # Apply types
        df_typed = adapter.apply_types(df)

        # Check conversions
        assert pd.api.types.is_datetime64_any_dtype(df_typed["refdate"])
        assert df_typed["symbol"].dtype == "string"
        assert pd.api.types.is_numeric_dtype(df_typed["volume"])
        assert pd.api.types.is_numeric_dtype(df_typed["open"])
        assert pd.api.types.is_numeric_dtype(df_typed["close"])

    def test_fieldset_field_names_match_template(self):
        """Test that fieldset field names match template field names."""
        template = retrieve_template("b3-bvbg086")
        # Template.fields is now a Fieldset directly
        fieldset = template.fields

        # Get fieldset field names (using .names for compatibility)
        fieldset_names = set(fieldset.names)

        # Should have expected fields
        assert "refdate" in fieldset_names
        assert "symbol" in fieldset_names

    def test_apply_types_handles_all_template_types(self):
        """Test that all types in template can be handled."""
        template = retrieve_template("b3-bvbg086")
        # Template.fields is now a Fieldset directly
        fieldset = template.fields
        adapter = PandasAdapter(fieldset, errors="coerce")

        # Create DataFrame with sample data for each field
        data = {}
        for field_name in fieldset.get_field_names():
            field = fieldset.get_field(field_name)
            type_name = field.type_name

            # Provide appropriate sample values
            if type_name == "date":
                data[field_name] = ["2024-01-01"]
            elif type_name in ("numeric", "integer"):
                data[field_name] = ["123.45"]
            else:  # character
                data[field_name] = ["TEST"]

        df = pd.DataFrame(data)

        # Apply types - should not raise errors
        df_typed = adapter.apply_types(df)

        # Check that conversion happened
        assert len(df_typed) == 1
        assert len(df_typed.columns) == len(fieldset)


class TestFieldsetToDict:
    """Test that fieldset can be serialized/deserialized."""

    def test_from_template_fields_to_dict_roundtrip(self):
        """Test converting template fields -> fieldset -> dict -> fieldset."""
        fields_data = [
            {
                "name": "refdate",
                "description": "Reference date",
                "handler": {"type": "date"},
            },
            {"name": "volume", "description": "Volume", "handler": {"type": "numeric"}},
        ]
        template_fields = TemplateFields(fields_data)

        # Template -> Fieldset
        fieldset1 = Fieldset.from_template_fields(template_fields)

        # Fieldset -> Dict
        fieldset_dict = fieldset1.to_dict()

        # Dict -> Fieldset
        fieldset2 = Fieldset.from_dict(fieldset_dict)

        # Should be equivalent
        assert len(fieldset1) == len(fieldset2)
        assert fieldset1.get_field_names() == fieldset2.get_field_names()

        for name in fieldset1.get_field_names():
            field1 = fieldset1.get_field(name)
            field2 = fieldset2.get_field(name)
            assert field1.name == field2.name
            assert field1.description == field2.description
            assert field1.type_name == field2.type_name
