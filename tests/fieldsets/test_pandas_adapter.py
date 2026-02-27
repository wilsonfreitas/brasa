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
    assert df["raw_date"].iloc[0] == date(2023, 1, 1)

    # Check dtypes
    assert df["id"].dtype == "Int64"
    assert df["name"].dtype == "string"
    assert df["amount"].dtype == "float64"  # Converters return float
    assert (
        df["tx_date"].dtype == "object"
    )  # date converter returns datetime.date objects
    assert (
        df["tx_datetime"].dtype == "datetime64[ns]"
    )  # datetime converter, auto-promoted
    assert df["tx_time"].dtype == "object"  # time converter returns time objects
    assert df["is_active"].dtype == "boolean"
    assert df["description"].dtype == "string"
    assert df["raw_int"].dtype == "Int64"
    assert df["raw_date"].dtype == "object"  # converter returns datetime.date objects


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


# ---------------------------------------------------------------------------
# Phase 4 regression tests  (TASK-015, TASK-016)
# ---------------------------------------------------------------------------


# --- TASK-015: Parameterised date format conversion via apply_types ----------


@pytest.fixture
def date_format_fieldset():
    """Fieldset with custom date format parameters."""
    fs = Fieldset(name="date_format_test")
    fs.add_field(Field("d_yyyymmdd", "Date YYYYMMDD", "date(format='%Y%m%d')"))
    fs.add_field(Field("d_dmY", "Date d/m/Y", "date(format='%d/%m/%Y')"))
    fs.add_field(
        Field("dt_custom", "Datetime custom", "datetime(format='%d-%m-%Y %H:%M')")
    )
    return fs


def test_apply_types_date_with_custom_format(date_format_fieldset):
    """apply_types converts parameterised date/datetime columns vectorially."""
    df = pd.DataFrame(
        {
            "d_yyyymmdd": ["20230101", "20231231", "99999999"],  # last is invalid
            "d_dmY": ["01/01/2023", "31/12/2023", "bad"],
            "dt_custom": ["01-01-2023 09:30", "31-12-2023 23:59", "nope"],
        }
    )

    adapter = PandasAdapter(
        date_format_fieldset, errors="coerce", verbose_warnings=False
    )
    result = adapter.apply_types(df)

    assert pd.api.types.is_datetime64_any_dtype(result["d_yyyymmdd"])
    assert result["d_yyyymmdd"].iloc[0] == pd.Timestamp("2023-01-01")
    assert result["d_yyyymmdd"].iloc[1] == pd.Timestamp("2023-12-31")
    assert pd.isna(result["d_yyyymmdd"].iloc[2])  # invalid coerced to NaT

    assert pd.api.types.is_datetime64_any_dtype(result["d_dmY"])
    assert result["d_dmY"].iloc[0] == pd.Timestamp("2023-01-01")
    assert pd.isna(result["d_dmY"].iloc[2])

    assert pd.api.types.is_datetime64_any_dtype(result["dt_custom"])
    assert result["dt_custom"].iloc[0] == pd.Timestamp("2023-01-01 09:30")
    assert pd.isna(result["dt_custom"].iloc[2])


def test_apply_types_date_format_raise_errors(date_format_fieldset):
    """apply_types with errors='raise' propagates ValueError on bad date string."""
    df = pd.DataFrame(
        {
            "d_yyyymmdd": ["baddate"],
            "d_dmY": ["01/01/2023"],
            "dt_custom": ["01-01-2023 09:00"],
        }
    )
    from brasa.fieldsets.exceptions import TypeParseError

    adapter = PandasAdapter(date_format_fieldset, errors="raise")
    with pytest.raises(TypeParseError):
        adapter.apply_types(df)


# --- TASK-015: Parameterised numeric conversion via apply_types ---------------


@pytest.fixture
def numeric_params_fieldset():
    """Fieldset exercising all NumericParser parameters."""
    fs = Fieldset(name="numeric_params_test")
    fs.add_field(Field("n_dec2", "Implied decimal 2", "numeric(dec=2)"))
    fs.add_field(Field("n_thousands", "Thousands separator", "numeric(thousands=',')"))
    fs.add_field(
        Field(
            "n_br_fmt",
            "Brazilian BRL",
            "numeric(thousands='.', decimal=',')",
        )
    )
    fs.add_field(Field("n_sign_neg", "Negative sign", "numeric(sign='-')"))
    fs.add_field(
        Field(
            "n_combo",
            "Combo dec+thousands",
            "numeric(dec=2, thousands=',')",
        )
    )
    return fs


def test_apply_types_numeric_dec(numeric_params_fieldset):
    """apply_types applies implied decimal places via vectorised path."""
    df = pd.DataFrame(
        {
            "n_dec2": ["12345", "67890", "bad"],
            "n_thousands": ["1,000", "1,234,567", "x"],
            "n_br_fmt": ["1.234,56", "999.000,00", "na"],
            "n_sign_neg": ["100", "200", "na"],
            "n_combo": ["1,23456", "2,00000", "na"],
        }
    )
    adapter = PandasAdapter(
        numeric_params_fieldset, errors="coerce", verbose_warnings=False
    )
    result = adapter.apply_types(df)

    # n_dec2: 12345 / 100 = 123.45
    assert abs(result["n_dec2"].iloc[0] - 123.45) < 1e-9
    assert abs(result["n_dec2"].iloc[1] - 678.90) < 1e-9
    assert pd.isna(result["n_dec2"].iloc[2])

    # n_thousands: "1,000" -> 1000.0
    assert result["n_thousands"].iloc[0] == 1000.0
    assert result["n_thousands"].iloc[1] == 1_234_567.0
    assert pd.isna(result["n_thousands"].iloc[2])

    # n_br_fmt: "1.234,56" -> 1234.56
    assert abs(result["n_br_fmt"].iloc[0] - 1234.56) < 1e-9
    assert result["n_br_fmt"].iloc[1] == 999_000.0
    assert pd.isna(result["n_br_fmt"].iloc[2])

    # n_sign_neg: sign='-' negates value
    assert result["n_sign_neg"].iloc[0] == -100.0
    assert result["n_sign_neg"].iloc[1] == -200.0

    # n_combo: dec=2, thousands=',': "1,23456" -> 123456/100 = 1234.56... wait
    # "1,23456" -> remove "," -> "123456" -> float 123456 -> / 100 = 1234.56
    assert abs(result["n_combo"].iloc[0] - 1234.56) < 1e-9


def test_apply_types_numeric_raise_errors(numeric_params_fieldset):
    """apply_types with errors='raise' propagates on unparseable numeric."""
    df = pd.DataFrame(
        {
            "n_dec2": ["not_a_number"],
            "n_thousands": ["1,000"],
            "n_br_fmt": ["1.000,00"],
            "n_sign_neg": ["1"],
            "n_combo": ["100"],
        }
    )
    from brasa.fieldsets.exceptions import TypeParseError

    adapter = PandasAdapter(numeric_params_fieldset, errors="raise")
    with pytest.raises(TypeParseError):
        adapter.apply_types(df)


# --- TASK-015: Boolean vectorised conversion -----------------------------------


def test_apply_types_boolean_vectorised():
    """apply_types maps boolean strings correctly including invalid -> NA."""
    fs = Fieldset(name="bool_test")
    fs.add_field(Field("flag", "Flag", "boolean"))

    df = pd.DataFrame({"flag": ["true", "false", "yes", "no", "1", "0", "maybe", ""]})
    adapter = PandasAdapter(fs, errors="coerce", verbose_warnings=False)
    result = adapter.apply_types(df)

    assert result["flag"].dtype == "boolean"
    assert result["flag"].iloc[0]  # "true"
    assert not result["flag"].iloc[1]  # "false"
    assert result["flag"].iloc[2]  # "yes"
    assert not result["flag"].iloc[3]  # "no"
    assert result["flag"].iloc[4]  # "1"
    assert not result["flag"].iloc[5]  # "0"
    assert pd.isna(result["flag"].iloc[6])  # "maybe" -> NA
    assert pd.isna(result["flag"].iloc[7])  # "" -> NA


# --- TASK-016: No row-wise apply for vectorizable fields -----------------------


def test_no_row_wise_apply_for_vectorizable_date_fields():
    """
    Regression: _can_vectorize_date returns True for all date/datetime fields
    so that apply_types never falls back to _convert_with_converter.

    Verifies CAND-004 dispatch: vectorized path is taken for both parameterised
    (custom format) and default date/datetime fields.
    """
    fs = Fieldset(name="vectorize_check")
    fields_to_check = [
        Field("d_fmt", "Custom format", "date(format='%Y%m%d')"),
        Field("d_default", "Default format", "date"),
        Field("dt_fmt", "Custom datetime", "datetime(format='%Y%m%d%H%M%S')"),
        Field("dt_default", "Default datetime", "datetime"),
    ]
    for f in fields_to_check:
        fs.add_field(f)

    for errors in ("raise", "coerce", "ignore"):
        adapter = PandasAdapter(fs, errors=errors)  # type: ignore[arg-type]
        for f in fields_to_check:
            assert adapter._can_vectorize_date(f), (
                f"Expected _can_vectorize_date=True for field '{f.name}' "
                f"(type: {f.type_definition}, errors: {errors})"
            )


def test_no_row_wise_apply_for_vectorizable_numeric_fields():
    """
    Regression: _can_vectorize_numeric returns True for all numeric fields
    (with and without custom parameters) so apply_types uses vectorized path.
    """
    fs = Fieldset(name="numeric_vectorize_check")
    fields_to_check = [
        Field("n_plain", "Plain", "numeric"),
        Field("n_dec", "Dec", "numeric(dec=2)"),
        Field("n_thousands", "Thousands", "numeric(thousands=',')"),
        Field("n_decimal", "Decimal sep", "numeric(decimal=',')"),
        Field("n_sign", "Sign", "numeric(sign='-')"),
        Field("n_combo", "Combo", "numeric(dec=2, thousands='.', decimal=',')"),
    ]
    for f in fields_to_check:
        fs.add_field(f)

    adapter = PandasAdapter(fs)
    for f in fields_to_check:
        assert adapter._can_vectorize_numeric(f), (
            f"Expected _can_vectorize_numeric=True for field '{f.name}' "
            f"(type: {f.type_definition})"
        )


def test_needs_converter_used_only_for_read_csv():
    """
    Confirm that _needs_converter decisions are independent of apply_types
    vectorization.  All date/datetime/time fields need a converter for read_csv,
    but this does NOT prevent vectorized apply_types from running.
    """
    fs = Fieldset(name="read_csv_converter_check")
    date_fields = [
        Field("d_fmt", "Custom format date", "date(format='%Y%m%d')"),
        Field("d_default", "Default date", "date"),
        Field("dt_fmt", "Custom datetime", "datetime(format='%Y-%m-%dT%H:%M:%S')"),
        Field("dt_default", "Default datetime", "datetime"),
    ]
    for f in date_fields:
        fs.add_field(f)

    for errors in ("raise", "coerce", "ignore"):
        adapter = PandasAdapter(fs, errors=errors)  # type: ignore[arg-type]
        converters = adapter.get_converters()
        # All date/datetime fields must have a converter in read_csv path
        for f in date_fields:
            assert f.name in converters, (
                f"Field '{f.name}' missing from read_csv converters (errors='{errors}')"
            )
        # And all are also independently vectorizable in apply_types
        for f in date_fields:
            assert adapter._can_vectorize_date(f), (
                f"Field '{f.name}' should still be vectorizable in apply_types"
            )
