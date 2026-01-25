import io
from datetime import date, datetime, time

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
    fs.add_field(Field("tx_time", "Transaction Time", "time(format='%H:%M:%S')"))
    fs.add_field(Field("is_active", "Active", "boolean"))
    fs.add_field(Field("description", "Description", "character"))  # Alias for string
    fs.add_field(Field("raw_int", "Raw Int", "integer"))  # For default format
    fs.add_field(Field("raw_date", "Raw Date", "date"))  # For default format
    return fs


@pytest.fixture
def sample_csv_content_for_pandas():
    return """id,name,amount,tx_date,tx_datetime,tx_time,is_active,description,raw_int,raw_date
1,Alice,12345,2023-01-01,2023-01-01 10:00:00,10:00:00,true,First entry,100,2023-01-01
2,Bob,67890,2023-01-02,2023-01-02 11:00:00,11:00:00,false,Second entry,200,2023-01-02
3,Charlie,10000,2023-01-03,2023-01-03 12:00:00,12:00:00,true,Third entry,300,2023-01-03
4,David,invalid,2023-01-04,2023-01-04 13:00:00,13:00:00,false,Fourth entry,400,2023-01-04
5,Eve,20000,invalid_date,2023-01-05 14:00:00,14:00:00,true,Fifth entry,500,invalid_date
6,Frank,30000,2023-01-06,invalid_datetime,15:00:00,false,Sixth entry,600,2023-01-06
7,Grace,40000,2023-01-07,2023-01-07 16:00:00,invalid_time,true,Seventh entry,700,2023-01-07
8,Heidi,,2023-01-08,2023-01-08 17:00:00,17:00:00,,Eighth entry,800,2023-01-08
"""


def test_pandas_adapter_init(sample_fieldset_for_pandas):
    adapter = PandasAdapter(sample_fieldset_for_pandas)
    assert isinstance(adapter, PandasAdapter)
    assert adapter.errors == "coerce"
    assert adapter.use_nullable_dtypes is True


def test_pandas_adapter_get_dtype_converters_parse_dates(sample_fieldset_for_pandas):
    adapter = PandasAdapter(sample_fieldset_for_pandas)

    dtype_dict = adapter.get_dtype_dict()
    converters = adapter.get_converters()
    parse_dates = adapter.get_parse_dates()

    # Check dtypes for simple types
    assert dtype_dict["id"] == "Int64"
    assert dtype_dict["name"] == "string"
    assert dtype_dict["is_active"] == "boolean"
    assert dtype_dict["description"] == "string"
    assert dtype_dict["raw_int"] == "Int64"

    # Check converters for complex types and date/datetime/time with error handling
    assert "amount" in converters
    assert "tx_date" in converters
    assert "tx_datetime" in converters
    assert "tx_time" in converters
    # raw_date uses converter too when errors='coerce' (default) for better error handling
    assert "raw_date" in converters

    # parse_dates should be empty when errors='coerce' because dates use converters for error handling
    assert len(parse_dates) == 0


def test_pandas_adapter_read_csv_success(
    sample_fieldset_for_pandas, sample_csv_content_for_pandas
):
    adapter = PandasAdapter(sample_fieldset_for_pandas, errors="raise")
    df = adapter.read_csv(
        io.StringIO(
            sample_csv_content_for_pandas.splitlines()[0]
            + "\n"
            + sample_csv_content_for_pandas.splitlines()[1]
        )
    )

    assert len(df) == 1
    assert df["id"].iloc[0] == 1
    assert df["name"].iloc[0] == "Alice"
    assert df["amount"].iloc[0] == 123.45
    assert df["tx_date"].iloc[0] == date(2023, 1, 1)
    assert df["tx_datetime"].iloc[0] == datetime(2023, 1, 1, 10, 0, 0)
    assert df["tx_time"].iloc[0] == time(10, 0, 0)
    assert df["is_active"].iloc[0]
    assert df["description"].iloc[0] == "First entry"
    assert df["raw_int"].iloc[0] == 100
    assert df["raw_date"].iloc[0] == pd.Timestamp(2023, 1, 1)

    # Check dtypes
    assert df["id"].dtype == "Int64"
    assert df["name"].dtype == "string"
    assert df["amount"].dtype == "float64"  # Converters return float
    assert df["tx_date"].dtype == "object"  # Converters return object
    assert df["tx_datetime"].dtype == "datetime64[ns]"  # Converters return object
    assert df["tx_time"].dtype == "object"  # Converters return object
    assert df["is_active"].dtype == "boolean"
    assert df["description"].dtype == "string"
    assert df["raw_int"].dtype == "Int64"
    assert pd.api.types.is_datetime64_any_dtype(
        df["raw_date"]
    )  # parse_dates makes it datetime64


def test_pandas_adapter_read_csv_coerce_errors(
    sample_fieldset_for_pandas, sample_csv_content_for_pandas
):
    adapter = PandasAdapter(
        sample_fieldset_for_pandas, errors="coerce", verbose_warnings=False
    )
    df = adapter.read_csv(io.StringIO(sample_csv_content_for_pandas))

    # Check invalid 'amount'
    assert pd.isna(df["amount"].iloc[3])

    # Check invalid 'tx_date'
    assert pd.isna(df["tx_date"].iloc[4])

    # Check invalid 'tx_datetime'
    assert pd.isna(df["tx_datetime"].iloc[5])

    # Check invalid 'tx_time'
    assert pd.isna(df["tx_time"].iloc[6])

    # Check missing 'is_active'
    assert pd.isna(df["is_active"].iloc[7])


def test_pandas_adapter_read_csv_raise_errors(
    sample_fieldset_for_pandas, sample_csv_content_for_pandas
):
    adapter = PandasAdapter(
        sample_fieldset_for_pandas, errors="raise", verbose_warnings=False
    )
    with pytest.raises(ValueError, match="Error parsing field 'amount'"):
        adapter.read_csv(io.StringIO(sample_csv_content_for_pandas))


def test_pandas_adapter_read_csv_ignore_errors(
    sample_fieldset_for_pandas, sample_csv_content_for_pandas
):
    adapter = PandasAdapter(
        sample_fieldset_for_pandas, errors="ignore", verbose_warnings=False
    )
    df = adapter.read_csv(io.StringIO(sample_csv_content_for_pandas))

    # Should behave like coerce but without warnings
    assert pd.isna(df["amount"].iloc[3])
    assert pd.isna(df["tx_date"].iloc[4])


def test_pandas_adapter_validate_dataframe(
    sample_fieldset_for_pandas, sample_csv_content_for_pandas
):
    adapter = PandasAdapter(
        sample_fieldset_for_pandas, errors="coerce", verbose_warnings=False
    )
    df = adapter.read_csv(io.StringIO(sample_csv_content_for_pandas))

    validation_results = adapter.validate_dataframe(df)

    assert validation_results["id"]["present"] is True
    assert validation_results["id"]["non_null_count"] == 8
    assert validation_results["id"]["dtype"] == "Int64"

    assert validation_results["amount"]["present"] is True
    assert (
        validation_results["amount"]["non_null_count"] == 6
    )  # One invalid, one missing
    assert validation_results["amount"]["null_count"] == 2  # One invalid, one missing

    assert validation_results["tx_date"]["present"] is True
    assert validation_results["tx_date"]["non_null_count"] == 7  # One invalid

    # Test missing column
    df_missing_col = df.drop(columns=["name"])
    validation_results_missing = adapter.validate_dataframe(df_missing_col)
    assert validation_results_missing["name"]["present"] is False
    assert "error" in validation_results_missing["name"]


def test_pandas_adapter_no_nullable_dtypes(
    sample_fieldset_for_pandas, sample_csv_content_for_pandas
):
    adapter = PandasAdapter(
        sample_fieldset_for_pandas,
        use_nullable_dtypes=False,
        errors="coerce",
        verbose_warnings=False,
    )
    df = adapter.read_csv(io.StringIO(sample_csv_content_for_pandas))

    assert df["id"].dtype == "float64"  # int becomes float64 without nullable
    assert df["name"].dtype == "object"  # string becomes object
    assert df["is_active"].dtype == "object"  # boolean becomes object

    assert pd.isna(df["id"].iloc[0]) is False  # No NaN for valid int
    assert pd.isna(df["amount"].iloc[3])  # Invalid numeric still NaN
