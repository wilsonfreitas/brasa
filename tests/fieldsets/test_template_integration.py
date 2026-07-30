"""
Tests for integration between fieldsets and template system.
"""

import pandas as pd

from brasa.engine import retrieve_template
from brasa.fieldsets import Field, Fieldset
from brasa.fieldsets.adapters import PandasAdapter


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
        """Test that invalid values are coerced to NaN."""
        fieldset = Fieldset()
        fieldset.add_field(Field("num_col", "Numeric", "numeric"))

        df = pd.DataFrame(
            {
                "num_col": ["1.5", "invalid", "3.5"],
            }
        )

        adapter = PandasAdapter(fieldset)
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

        # Template.fields is a Fieldset directly
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
        # Template.fields is a Fieldset directly
        fieldset = template.fields

        # Get fieldset field names (using .names for compatibility)
        fieldset_names = set(fieldset.names)

        # Should have expected fields
        assert "refdate" in fieldset_names
        assert "symbol" in fieldset_names

    def test_apply_types_handles_all_template_types(self):
        """Test that all types in template can be handled."""
        template = retrieve_template("b3-bvbg086")
        # Template.fields is a Fieldset directly
        fieldset = template.fields
        adapter = PandasAdapter(fieldset)

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
