"""Tests for PandasAdapter apply_types method."""

import pandas as pd
import pytest

from brasa.fieldsets import Field, Fieldset
from brasa.fieldsets.adapters.pandas_adapter import PandasAdapter


@pytest.fixture
def sample_fieldset_for_pandas():
    fs = Fieldset(name="pandas_test_schema")
    fs.add_field(Field("id", "ID", "integer", required=True))
    fs.add_field(Field("name", "Name", "string"))
    fs.add_field(Field("amount", "Amount", "numeric(dec=2)"))
    fs.add_field(Field("tx_date", "Transaction Date", "date(format='%Y-%m-%d')"))
    fs.add_field(
        Field(
            "tx_datetime",
            "Transaction Datetime",
            "datetime(format='%Y-%m-%d %H:%M:%S')",
        )
    )
    fs.add_field(Field("is_active", "Active", "boolean"))
    fs.add_field(Field("description", "Description", "character"))
    return fs


def test_pandas_adapter_apply_types_basic(sample_fieldset_for_pandas):
    """Test that apply_types converts types correctly."""
    adapter = PandasAdapter(sample_fieldset_for_pandas)
    df = pd.DataFrame(
        {
            "id": ["1", "2", "3"],
            "name": ["Alice", "Bob", "Charlie"],
            "amount": ["123.45", "67.89", "10.00"],
            "tx_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "tx_datetime": [
                "2023-01-01 10:00:00",
                "2023-01-02 11:00:00",
                "2023-01-03 12:00:00",
            ],
            "is_active": ["true", "false", "true"],
            "description": ["First", "Second", "Third"],
        }
    )

    df_typed = adapter.apply_types(df)

    # Check types were applied
    assert df_typed["id"].dtype == "Int64"
    assert df_typed["name"].dtype == "string"
    assert df_typed["is_active"].dtype == "boolean"
    assert df_typed["description"].dtype == "string"
    assert pd.api.types.is_datetime64_any_dtype(df_typed["tx_date"])
    assert pd.api.types.is_datetime64_any_dtype(df_typed["tx_datetime"])


def test_pandas_adapter_coerce_errors(sample_fieldset_for_pandas):
    """Test that invalid data is coerced to NaN/NaT."""
    adapter = PandasAdapter(sample_fieldset_for_pandas)
    df = pd.DataFrame(
        {
            "id": ["1", "invalid", "3"],
            "name": ["Alice", "Bob", "Charlie"],
            "amount": ["123.45", "invalid", "10.00"],
            "tx_date": ["2023-01-01", "invalid_date", "2023-01-03"],
            "tx_datetime": [
                "2023-01-01 10:00:00",
                "invalid_datetime",
                "2023-01-03 12:00:00",
            ],
            "is_active": ["true", "false", "invalid_bool"],
            "description": ["First", "Second", "Third"],
        }
    )

    df_typed = adapter.apply_types(df)

    # Invalid values should be coerced to NaN/NaT
    assert pd.isna(df_typed["id"].iloc[1])
    assert pd.isna(df_typed["amount"].iloc[1])
    assert pd.isna(df_typed["tx_date"].iloc[1])
    assert pd.isna(df_typed["tx_datetime"].iloc[1])
    assert pd.isna(df_typed["is_active"].iloc[2])

    # Valid values should be converted
    assert df_typed["id"].iloc[0] == 1
    assert df_typed["name"].iloc[0] == "Alice"
    assert df_typed["is_active"].iloc[0]


def test_pandas_adapter_skips_missing_columns(sample_fieldset_for_pandas):
    """Test that apply_types handles missing columns gracefully."""
    adapter = PandasAdapter(sample_fieldset_for_pandas)
    df = pd.DataFrame(
        {
            "id": ["1", "2"],
            "name": ["Alice", "Bob"],
            # Missing amount, tx_date, etc. — should not raise
        }
    )

    df_typed = adapter.apply_types(df)
    # Should still have id and name converted
    assert df_typed["id"].dtype == "Int64"
    assert df_typed["name"].dtype == "string"
