from kyd.readers.csv import CSVFile, Field, DateField, NumericField
from datetime import datetime


class MyCSVFile(CSVFile):
    _separator = ";"
    columns = [
        Field(),
        DateField("%Y%m%d"),
        NumericField(decimal=","),
        NumericField(decimal=",", thousands="."),
    ]


def test_CSVFile():
    x = MyCSVFile('data/test.csv')
    assert len(x.rows) == 3
    assert isinstance(x.rows[0]["refdate"], datetime)
    assert isinstance(x.rows[0]["rate"], float)
    assert isinstance(x.rows[0]["price"], float)
    assert isinstance(x.rows[0]["symbol"], str)
