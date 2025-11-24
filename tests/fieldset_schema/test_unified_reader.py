import io
import pytest
import pandas as pd
import pyarrow as pa
from datetime import date

from brasa.fieldset_schema import Field, Fieldset
from brasa.fieldset_schema.adapters.unified_reader import FieldsetReader


@pytest.fixture
def sample_fieldset_for_unified_reader():
    fs = Fieldset(name="unified_test_schema")
    fs.add_field(Field("id", "ID", "integer", required=True))
    fs.add_field(Field("name", "Name", "string"))
    fs.add_field(Field("amount", "Amount", "numeric(dec=2)")) # Custom numeric
    fs.add_field(Field("tx_date", "Transaction Date", "date(format='%Y-%m-%d')")) # Custom date
    fs.add_field(Field("raw_date", "Raw Date", "date")) # Simple date
    return fs


@pytest.fixture
def sample_csv_content_for_unified_reader():
    return """id,name,amount,tx_date,raw_date
1,Alice,12345,2023-01-01,2023-01-01
2,Bob,67890,2023-01-02,2023-01-02
3,Charlie,invalid,2023-01-03,2023-01-03
4,David,10000,invalid_date,2023-01-04
"""


def test_unified_reader_init(sample_fieldset_for_unified_reader):
    reader = FieldsetReader(sample_fieldset_for_unified_reader)
    assert isinstance(reader, FieldsetReader)
    assert reader.errors == 'coerce'
    assert reader.verbose_warnings is True
    assert isinstance(reader.pandas_adapter, reader.pandas_adapter.__class__)
    assert isinstance(reader.pyarrow_adapter, reader.pyarrow_adapter.__class__)

def test_unified_reader_select_engine_small_file_no_custom_formats(tmp_path):
    # Create a fieldset with only simple types
    fs = Fieldset()
    fs.add_field(Field("col1", "Col1", "integer"))
    fs.add_field(Field("col2", "Col2", "string"))
    reader = FieldsetReader(fs, verbose_warnings=False)
    
    # Create a small dummy file
    small_file = tmp_path / "small.csv"
    small_file.write_text("col1,col2\n1,a\n")
    
    # Temporarily set threshold high to ensure it's considered "small"
    original_threshold = FieldsetReader.PYARROW_THRESHOLD_MB
    FieldsetReader.PYARROW_THRESHOLD_MB = 1000 # Effectively makes all files small
    
    engine = reader._select_engine(small_file)
    assert engine == 'pandas'
    
    FieldsetReader.PYARROW_THRESHOLD_MB = original_threshold # Reset

def test_unified_reader_select_engine_large_file_no_custom_formats(tmp_path):
    # Create a fieldset with only simple types
    fs = Fieldset()
    fs.add_field(Field("col1", "Col1", "integer"))
    fs.add_field(Field("col2", "Col2", "string"))
    reader = FieldsetReader(fs, verbose_warnings=False)
    
    # Create a large dummy file (larger than threshold)
    large_file = tmp_path / "large.csv"
    large_file.write_text("col1,col2\n" + "1,a\n" * 100000) # ~2MB
    
    # Temporarily set threshold low to ensure it's considered "large"
    original_threshold = FieldsetReader.PYARROW_THRESHOLD_MB
    FieldsetReader.PYARROW_THRESHOLD_MB = 0.1 # Makes ~2MB file "large"
    
    engine = reader._select_engine(large_file)
    assert engine == 'pyarrow'
    
    FieldsetReader.PYARROW_THRESHOLD_MB = original_threshold # Reset

def test_unified_reader_select_engine_with_custom_formats(sample_fieldset_for_unified_reader, tmp_path):
    reader = FieldsetReader(sample_fieldset_for_unified_reader, verbose_warnings=False)
    
    # Create a large dummy file (size doesn't matter for custom formats)
    file_with_custom = tmp_path / "custom_format.csv"
    file_with_custom.write_text("id,name,amount,tx_date,raw_date\n1,A,100,2023-01-01,2023-01-01\n")
    
    # Temporarily set threshold low to ensure it's considered "large"
    original_threshold = FieldsetReader.PYARROW_THRESHOLD_MB
    FieldsetReader.PYARROW_THRESHOLD_MB = 0.1 # Makes file "large"
    
    engine = reader._select_engine(file_with_custom)
    assert engine == 'pandas' # Should always pick pandas if custom formats are present
    
    FieldsetReader.PYARROW_THRESHOLD_MB = original_threshold # Reset

def test_unified_reader_read_csv_pandas_return_pandas(sample_fieldset_for_unified_reader, sample_csv_content_for_unified_reader):
    reader = FieldsetReader(sample_fieldset_for_unified_reader, errors='coerce', verbose_warnings=False)
    df = reader.read_csv(io.StringIO(sample_csv_content_for_unified_reader), engine='pandas', return_type='pandas')
    
    assert isinstance(df, pd.DataFrame)
    assert df['id'].dtype == 'Int64'
    assert df['amount'].iloc[0] == 123.45
    assert pd.isna(df['amount'].iloc[2])
    assert df['tx_date'].iloc[0] == date(2023, 1, 1)
    assert pd.isna(df['tx_date'].iloc[3])
    # raw_date uses converter when errors='coerce', returns date objects
    assert df['raw_date'].iloc[0] == date(2023, 1, 1)

def test_unified_reader_read_csv_pyarrow_return_pyarrow(sample_fieldset_for_unified_reader, sample_csv_content_for_unified_reader):
    reader = FieldsetReader(sample_fieldset_for_unified_reader, errors='coerce', verbose_warnings=False)
    table = reader.read_csv(io.StringIO(sample_csv_content_for_unified_reader), engine='pyarrow', return_type='pyarrow')
    
    assert isinstance(table, pa.Table)
    assert table.schema.field('id').type == pa.int64()
    assert table.schema.field('amount').type == pa.float64()
    # PyArrow float64 values are Python floats when converted to pylist
    assert table['amount'].to_pylist()[0] == 123.45
    assert table['amount'].to_pylist()[2] is None
    assert table.schema.field('tx_date').type == pa.date32()
    assert table['tx_date'].to_pylist()[0] == date(2023, 1, 1)
    assert table['tx_date'].to_pylist()[3] is None
    assert table.schema.field('raw_date').type == pa.date32()

def test_unified_reader_read_csv_pyarrow_return_pandas(sample_fieldset_for_unified_reader, sample_csv_content_for_unified_reader):
    reader = FieldsetReader(sample_fieldset_for_unified_reader, errors='coerce', verbose_warnings=False)
    df = reader.read_csv(io.StringIO(sample_csv_content_for_unified_reader), engine='pyarrow', return_type='pandas')
    
    assert isinstance(df, pd.DataFrame)
    assert df['id'].dtype == 'int64' # PyArrow to pandas default int64
    # PyArrow float64 converts to Python floats in pandas
    assert df['amount'].iloc[0] == 123.45
    assert pd.isna(df['amount'].iloc[2])
    assert df['tx_date'].iloc[0] == date(2023, 1, 1)
    assert pd.isna(df['tx_date'].iloc[3])
    # PyArrow date32 converts to date objects in pandas (object dtype)
    assert df['raw_date'].iloc[0] == date(2023, 1, 1)

def test_unified_reader_read_csv_auto_engine_return_pandas(sample_fieldset_for_unified_reader, sample_csv_content_for_unified_reader, tmp_path):
    reader = FieldsetReader(sample_fieldset_for_unified_reader, errors='coerce', verbose_warnings=False)
    
    # Small file, should use pandas
    small_file = tmp_path / "small_auto.csv"
    small_file.write_text(sample_csv_content_for_unified_reader)
    
    df = reader.read_csv(small_file, engine='auto', return_type='pandas')
    assert isinstance(df, pd.DataFrame)
    assert df['id'].dtype == 'Int64' # Pandas adapter default
    assert df['amount'].iloc[0] == 123.45

def test_unified_reader_read_csv_auto_engine_large_file_return_pyarrow(tmp_path):
    # Create a fieldset with only simple types (no custom formats)
    fs = Fieldset()
    fs.add_field(Field("col_int", "Col1", "integer"))
    fs.add_field(Field("col_float", "Col2", "numeric"))
    reader = FieldsetReader(fs, errors='coerce', verbose_warnings=False)
    
    # Create a large dummy file
    large_file = tmp_path / "large_auto.csv"
    content = "col_int,col_float\n" + "\n".join([f"{i},{i*1.0}" for i in range(100000)]) # ~2MB
    large_file.write_text(content)
    
    # Temporarily set threshold low to ensure it's considered "large"
    original_threshold = FieldsetReader.PYARROW_THRESHOLD_MB
    FieldsetReader.PYARROW_THRESHOLD_MB = 0.1
    
    table = reader.read_csv(large_file, engine='auto', return_type='pyarrow')
    assert isinstance(table, pa.Table)
    assert table.schema.field('col_int').type == pa.int64()
    # numeric without dec parameter defaults to float64 when use_decimal_for_numeric=False (new default)
    assert table.schema.field('col_float').type == pa.float64()
    
    FieldsetReader.PYARROW_THRESHOLD_MB = original_threshold

def test_unified_reader_file_not_found():
    fs = Fieldset()
    reader = FieldsetReader(fs)
    with pytest.raises(FileNotFoundError):
        reader.read_csv("non_existent_file.csv")

def test_unified_reader_invalid_engine():
    fs = Fieldset()
    reader = FieldsetReader(fs)
    with pytest.raises(ValueError, match="Invalid engine"):
        reader.read_csv(io.StringIO(""), engine='invalid', return_type='pandas') # type: ignore

def test_unified_reader_invalid_return_type():
    fs = Fieldset()
    reader = FieldsetReader(fs)
    with pytest.raises(ValueError, match="Invalid return_type"): # This error is actually caught by mypy, but for runtime safety
        reader.read_csv(io.StringIO(""), engine='pandas', return_type='invalid') # type: ignore
