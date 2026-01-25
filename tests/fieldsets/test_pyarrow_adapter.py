import io
from datetime import date, datetime, time

import pyarrow as pa
import pytest

from brasa.fieldsets import Field, Fieldset
from brasa.fieldsets.adapters.pyarrow_adapter import PyArrowAdapter


@pytest.fixture
def sample_fieldset_for_pyarrow():
    fs = Fieldset(name="pyarrow_test_schema")
    fs.add_field(Field("id", "ID", "integer", required=True))
    fs.add_field(Field("name", "Name", "string"))
    fs.add_field(Field("amount", "Amount", "numeric(dec=4)"))  # Custom numeric
    fs.add_field(
        Field("tx_date", "Transaction Date", "date(format='%Y-%m-%d')")
    )  # Custom date
    fs.add_field(
        Field(
            "tx_datetime",
            "Transaction Datetime",
            "datetime(format='%Y-%m-%d %H:%M:%S')",
        )
    )  # Custom datetime
    fs.add_field(
        Field("tx_time", "Transaction Time", "time(format='%H:%M:%S')")
    )  # Custom time
    fs.add_field(Field("is_active", "Active", "boolean"))
    fs.add_field(Field("description", "Description", "character"))  # Alias for string
    fs.add_field(Field("raw_int", "Raw Int", "integer"))  # Simple int
    fs.add_field(Field("raw_date", "Raw Date", "date"))  # Simple date
    return fs


@pytest.fixture
def sample_csv_content_for_pyarrow():
    return """id,name,amount,tx_date,tx_datetime,tx_time,is_active,description,raw_int,raw_date
1,Alice,12345678,2023-01-01,2023-01-01 10:00:00,10:00:00,true,First entry,100,2023-01-01
2,Bob,87654321,2023-01-02,2023-01-02 11:00:00,11:00:00,false,Second entry,200,2023-01-02
3,Charlie,10000000,2023-01-03,2023-01-03 12:00:00,12:00:00,true,Third entry,300,2023-01-03
4,David,invalid,2023-01-04,2023-01-04 13:00:00,13:00:00,false,Fourth entry,400,2023-01-04
5,Eve,20000000,invalid_date,2023-01-05 14:00:00,14:00:00,true,Fifth entry,500,invalid_date
6,Frank,30000000,2023-01-06,invalid_datetime,15:00:00,false,Sixth entry,600,2023-01-06
7,Grace,40000000,2023-01-07,2023-01-07 16:00:00,invalid_time,true,Seventh entry,700,2023-01-07
8,Heidi,,2023-01-08,2023-01-08 17:00:00,17:00:00,,Eighth entry,800,2023-01-08
"""


def test_pyarrow_adapter_init(sample_fieldset_for_pyarrow):
    adapter = PyArrowAdapter(sample_fieldset_for_pyarrow)
    assert isinstance(adapter, PyArrowAdapter)
    assert adapter.errors == "coerce"
    assert adapter.use_decimal_for_numeric is False


def test_pyarrow_adapter_get_schema(sample_fieldset_for_pyarrow):
    adapter = PyArrowAdapter(sample_fieldset_for_pyarrow)
    schema = adapter.get_schema()

    assert isinstance(schema, pa.Schema)
    assert schema.field("id").type == pa.int64()
    assert schema.field("name").type == pa.string()
    assert (
        schema.field("amount").type == pa.string()
    )  # Preprocessed, so string initially
    assert (
        schema.field("tx_date").type == pa.string()
    )  # Preprocessed, so string initially
    assert (
        schema.field("tx_datetime").type == pa.string()
    )  # Preprocessed, so string initially
    assert (
        schema.field("tx_time").type == pa.string()
    )  # Preprocessed, so string initially
    assert schema.field("is_active").type == pa.bool_()
    assert schema.field("description").type == pa.string()
    assert schema.field("raw_int").type == pa.int64()
    assert schema.field("raw_date").type == pa.date32()

    assert "amount" in adapter._needs_pandas_preprocessing
    assert "tx_date" in adapter._needs_pandas_preprocessing
    assert "tx_datetime" in adapter._needs_pandas_preprocessing
    assert "tx_time" in adapter._needs_pandas_preprocessing
    assert "raw_int" not in adapter._needs_pandas_preprocessing
    assert "raw_date" not in adapter._needs_pandas_preprocessing


def test_pyarrow_adapter_read_csv_pure_pyarrow(simple_fieldset, simple_csv_path):
    # Create a fieldset that doesn't require pandas preprocessing
    fs = Fieldset(name="simple_pyarrow_test")
    fs.add_field(Field("col_int", "Integer column", "integer"))
    fs.add_field(Field("col_str", "String column", "string"))
    fs.add_field(Field("col_float", "Float column", "numeric"))

    adapter = PyArrowAdapter(fs, errors="raise", verbose_warnings=False)
    table = adapter.read_csv(simple_csv_path)

    assert isinstance(table, pa.Table)
    assert table.num_rows == 3
    assert table.schema.field("col_int").type == pa.int64()
    assert table.schema.field("col_str").type == pa.string()
    assert (
        table.schema.field("col_float").type == pa.float64()
    )  # Default for numeric without dec

    assert table["col_int"].to_pylist() == [1, 2, 3]
    assert table["col_str"].to_pylist() == ["hello", "world", "test"]
    assert table["col_float"].to_pylist() == [1.23, 4.56, 7.89]


def test_pyarrow_adapter_read_csv_with_pandas_preprocessing(
    sample_fieldset_for_pyarrow, sample_csv_content_for_pyarrow
):
    adapter = PyArrowAdapter(
        sample_fieldset_for_pyarrow, errors="coerce", verbose_warnings=False
    )
    table = adapter.read_csv(io.StringIO(sample_csv_content_for_pyarrow))

    assert isinstance(table, pa.Table)
    assert table.num_rows == 8

    # Check types after preprocessing and casting
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()
    assert (
        table.schema.field("amount").type == pa.float64()
    )  # Numeric(dec=4) preprocessed
    assert (
        table.schema.field("tx_date").type == pa.date32()
    )  # Custom date format preprocessed
    assert table.schema.field("tx_datetime").type == pa.timestamp(
        "us"
    )  # Custom datetime format preprocessed
    assert table.schema.field("tx_time").type == pa.time64(
        "us"
    )  # Custom time format preprocessed
    assert table.schema.field("is_active").type == pa.bool_()
    assert table.schema.field("description").type == pa.string()
    assert table.schema.field("raw_int").type == pa.int64()
    assert table.schema.field("raw_date").type == pa.date32()  # Simple date

    # Check values (especially coerced ones)
    assert table["id"].to_pylist() == [1, 2, 3, 4, 5, 6, 7, 8]
    assert table["amount"].to_pylist()[0] == 1234.5678
    assert table["amount"].to_pylist()[3] is None  # Invalid value coerced
    assert table["tx_date"].to_pylist()[0] == date(2023, 1, 1)
    assert table["tx_date"].to_pylist()[4] is None  # Invalid date coerced
    assert table["tx_datetime"].to_pylist()[0] == datetime(2023, 1, 1, 10, 0, 0)
    assert table["tx_datetime"].to_pylist()[5] is None  # Invalid datetime coerced
    assert table["tx_time"].to_pylist()[0] == time(10, 0, 0)
    assert table["tx_time"].to_pylist()[6] is None  # Invalid time coerced
    assert table["is_active"].to_pylist()[7] is None  # Missing boolean coerced


def test_pyarrow_adapter_read_csv_raise_errors(
    sample_fieldset_for_pyarrow, sample_csv_content_for_pyarrow
):
    adapter = PyArrowAdapter(
        sample_fieldset_for_pyarrow, errors="raise", verbose_warnings=False
    )
    with pytest.raises(
        ValueError, match="Error parsing field 'amount'"
    ):  # Error from pandas preprocessing
        adapter.read_csv(io.StringIO(sample_csv_content_for_pyarrow))


def test_pyarrow_adapter_read_csv_ignore_errors(
    sample_fieldset_for_pyarrow, sample_csv_content_for_pyarrow
):
    adapter = PyArrowAdapter(
        sample_fieldset_for_pyarrow, errors="ignore", verbose_warnings=False
    )
    table = adapter.read_csv(io.StringIO(sample_csv_content_for_pyarrow))

    # Should behave like coerce but without warnings
    assert table["amount"].to_pylist()[3] is None
    assert table["tx_date"].to_pylist()[4] is None


def test_pyarrow_adapter_use_decimal_for_numeric_false(
    sample_fieldset_for_pyarrow, sample_csv_content_for_pyarrow
):
    adapter = PyArrowAdapter(
        sample_fieldset_for_pyarrow,
        use_decimal_for_numeric=False,
        verbose_warnings=False,
    )
    table = adapter.read_csv(io.StringIO(sample_csv_content_for_pyarrow))

    assert (
        table.schema.field("amount").type == pa.float64()
    )  # Should be float64 if decimal is false


def test_pyarrow_adapter_missing_column_in_csv(sample_fieldset_for_pyarrow):
    adapter = PyArrowAdapter(
        sample_fieldset_for_pyarrow, errors="coerce", verbose_warnings=False
    )
    csv_content = "id,name\n1,Test"  # Missing many columns
    table = adapter.read_csv(io.StringIO(csv_content))

    assert table.num_columns == len(sample_fieldset_for_pyarrow)
    assert table.schema.field("amount").type == pa.float64()
    assert table["amount"].to_pylist() == [None]  # Should be null
    assert table["tx_date"].to_pylist() == [None]  # Should be null
