import pytest

from brasa.fieldsets import (
    Field,
    Fieldset,
)


@pytest.fixture
def sample_fieldset_yaml_content():
    """YAML content for a sample fieldset."""
    return """
name: test_schema
description: A schema for testing purposes
fields:
  - name: id
    description: Unique identifier
    type: integer
    required: true
  - name: name
    description: Item name
    type: string
    max_length: 100
  - name: created_at
    description: Creation timestamp
    type: datetime(format="%Y-%m-%d %H:%M:%S")
  - name: effective_date
    description: Effective date
    type: date(format="%d/%m/%Y")
  - name: value
    description: Numeric value
    type: numeric(dec=2)
  - name: is_active
    description: Active status
    type: boolean
  - name: start_time
    description: Start time
    type: time(format="%H:%M:%S.%f")
  - name: raw_data
    description: Raw string data
    type: character
"""


@pytest.fixture
def sample_fieldset(sample_fieldset_yaml_content):
    """A Fieldset instance loaded from sample YAML content."""
    return Fieldset.from_yaml(sample_fieldset_yaml_content)


@pytest.fixture
def sample_csv_path(tmp_path):
    """Creates a temporary CSV file for testing and returns its path."""
    csv_content = """id,name,created_at,effective_date,value,is_active,start_time,raw_data
1,Item A,2023-01-01 10:00:00,01/01/2023,12345,true,10:00:00.123456,some_raw_data_1
2,Item B,2023-01-02 11:00:00,02/01/2023,67890,false,11:00:00.654321,some_raw_data_2
3,Item C,2023-01-03 12:00:00,03/01/2023,10000,true,12:00:00.000000,some_raw_data_3
4,Item D,2023-01-04 13:00:00,04/01/2023,invalid_value,false,13:00:00.000000,some_raw_data_4
5,Item E,invalid_datetime,05/01/2023,20000,true,14:00:00.000000,some_raw_data_5
6,Item F,2023-01-06 15:00:00,invalid_date,30000,false,15:00:00.000000,some_raw_data_6
7,Item G,2023-01-07 16:00:00,07/01/2023,,true,16:00:00.000000,some_raw_data_7
"""
    file_path = tmp_path / "test_data.csv"
    file_path.write_text(csv_content)
    return file_path


@pytest.fixture
def sample_csv_path_large(tmp_path):
    """Creates a larger temporary CSV file for testing PyArrow performance."""
    csv_content = "id,value\n"
    for i in range(100000):  # 100k rows
        csv_content += f"{i},{i*100}\n"
    file_path = tmp_path / "large_test_data.csv"
    file_path.write_text(csv_content)
    return file_path


@pytest.fixture
def simple_fieldset():
    """A simple fieldset for basic tests."""
    fs = Fieldset(name="simple_test")
    fs.add_field(Field("col_int", "Integer column", "integer"))
    fs.add_field(Field("col_str", "String column", "string"))
    fs.add_field(Field("col_float", "Float column", "numeric"))
    return fs


@pytest.fixture
def simple_csv_path(tmp_path):
    """A simple CSV file for basic tests."""
    csv_content = """col_int,col_str,col_float
1,hello,1.23
2,world,4.56
3,test,7.89
"""
    file_path = tmp_path / "simple_data.csv"
    file_path.write_text(csv_content)
    return file_path


@pytest.fixture
def fieldset_for_pyarrow_decimal():
    """Fieldset with numeric types for PyArrow decimal testing."""
    fs = Fieldset(name="decimal_test")
    fs.add_field(Field("amount", "Transaction amount", "numeric(dec=4)"))
    fs.add_field(Field("price", "Item price", "numeric(dec=2)"))
    fs.add_field(Field("quantity", "Quantity", "integer"))
    return fs


@pytest.fixture
def csv_for_pyarrow_decimal(tmp_path):
    """CSV for PyArrow decimal testing."""
    csv_content = """amount,price,quantity
123456,1234,10
789012,5678,20
100000,9999,30
"""
    file_path = tmp_path / "decimal_data.csv"
    file_path.write_text(csv_content)
    return file_path
