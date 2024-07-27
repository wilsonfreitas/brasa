from itertools import dropwhile
from datetime import datetime
from collections import OrderedDict
import pandas as pd


class Field:
    _counter = 0

    def __init__(self):
        self._counter_val = Field._counter
        Field._counter += 1

    def parse(self, text):
        return text


class DateField(Field):
    def __init__(self, format):
        super(DateField, self).__init__()
        self.format = format

    def parse(self, text):
        return datetime.strptime(text, self.format)


class NumericField(Field):
    def __init__(self, decimal=".", thousands=None):
        super(NumericField, self).__init__()
        self.decimal = decimal
        self.thousands = thousands

    def parse(self, text):
        if self.thousands:
            text = text.replace(self.thousands, "")
        if self.decimal != ".":
            text = text.replace(self.decimal, ".")
        return float(text)


class CSVFileMeta(type):
    """The metaclass for the FWFRow class. We use the metaclass to sort of
    the columns defined in the table declaration.
    """

    def __new__(meta, name, bases, attrs):
        """Create the class as normal, but also iterate over the attributes
        set.
        """
        cls = type.__new__(meta, name, bases, attrs)
        cls._fields = OrderedDict()
        # Then add the columns from this class.
        sorted_fields = sorted(
            ((k, v) for k, v in attrs.items() if isinstance(v, Field)),
            key=lambda x: x[1]._counter_val,
        )
        cls._fields.update(OrderedDict(sorted_fields))
        return cls


class CSVFile(metaclass=CSVFileMeta):
    _skip_row = 1
    _separator = ","
    _encoding = "UTF8"

    def __init__(self, fname):
        if isinstance(fname, str):
            fp = open(fname, "r", encoding=self._encoding)
        else:
            fp = fname

        self.rows = []
        fields_names = [f for f in self._fields.keys()]

        _drop_first_n = dropwhile(lambda x: x[0] < self._skip_row, enumerate(fp))
        _drop_empy = filter(lambda x: x[1].strip() != "", _drop_first_n)
        for _, line in _drop_empy:
            row = line.split(self._separator)
            parsed_row = {fields_names[ix]: self._fields[fields_names[ix]].parse(field) for ix, field in enumerate(row)}
            self.rows.append(parsed_row)

        if isinstance(fname, str):
            fp.close()
