import re
from collections import OrderedDict
from pathlib import Path

import pandas as pd


def read_fwf(con, widths, colnames=None, skip=0, parse_fun=lambda x: x):
    """read and parse fixed width field files"""
    colpositions = []
    x = 0
    line_len = sum(widths)
    for w in widths:
        colpositions.append((x, x + w))
        x = x + w

    colnames = (
        [f"V{ix + 1}" for ix in range(len(widths))] if colnames is None else colnames
    )

    terms = []
    for ix, line in enumerate(con):
        if ix < skip:
            continue
        _line = line.strip()
        if len(_line) != line_len:
            continue
        fields = [_line[dx[0] : dx[1]].strip() for dx in colpositions]
        obj = dict(zip(colnames, fields, strict=False))
        terms.append(parse_fun(obj))

    return terms


class Field:
    _counter = 0

    def __init__(self, width) -> None:
        self.width = width
        self._counter_val = Field._counter
        Field._counter += 1

    def parse(self, text: pd.Series) -> pd.Series:
        return text


class DateField(Field):
    def __init__(self, width, format) -> None:
        super().__init__(width)
        self.format = format

    def parse(self, text: pd.Series) -> pd.Series:
        return pd.to_datetime(text, format=self.format, errors="coerce")


class NumericField(Field):
    def __init__(self, width, dec=0, sign="") -> None:
        super().__init__(width)
        self.dec = dec
        self.sign = sign
        self.dtype = "int64" if dec == 0 else "float64"

    def parse(self, field: pd.Series) -> pd.Series:
        m = -1 if self.sign == "-" else 1
        if self.dec == 0:
            return m * pd.to_numeric(field, errors="coerce")
        else:
            return (
                m
                * pd.to_numeric(field, errors="coerce").astype(self.dtype)
                / (10 ** int(self.dec))
            )


class FWFRowMeta(type):
    """The metaclass for the FWFRow class. We use the metaclass to sort of
    the columns defined in the table declaration.
    """

    def __new__(cls, name, bases, attrs):
        """Create the class as normal, but also iterate over the attributes
        set.
        """
        new_cls = type.__new__(cls, name, bases, attrs)
        new_cls._fields = OrderedDict()
        # Then add the columns from this class.
        sorted_fields = sorted(
            ((k, v) for k, v in attrs.items() if isinstance(v, Field)),
            key=lambda x: x[1]._counter_val,
        )
        new_cls._fields.update(OrderedDict(sorted_fields))
        return new_cls


class FWFRow(metaclass=FWFRowMeta):
    def __init__(self):
        try:
            self.pattern = re.compile(self._pattern)
        except AttributeError:
            self.pattern = re.compile(".*")
        self.names = list(self._fields.keys())
        self.widths = [self._fields[n].width for n in self._fields]
        self.row_len = sum(self.widths)
        self.colpositions = []
        x = 0
        for fn in self._fields:
            f = self._fields[fn]
            w = f.width
            self.colpositions.append((x, x + w, f.parse))
            x = x + w

    def __len__(self):
        return self.row_len


class FWFFileMeta(type):
    """The metaclass for the FWFRow class. We use the metaclass to sort of
    the columns defined in the table declaration.
    """

    def __new__(cls, name, bases, attrs):
        """Create the class as normal, but also iterate over the attributes
        set.
        """
        new_cls = type.__new__(cls, name, bases, attrs)
        new_cls._rows = [(k, v) for k, v in attrs.items() if isinstance(v, FWFRow)]
        new_cls._buckets = {r[0]: [] for r in new_cls._rows}
        return new_cls


class FWFFile(metaclass=FWFFileMeta):
    skip_row = 0

    def __init__(self, fname, encoding="UTF8"):
        for nx in self._buckets:
            self._buckets[nx] = []
        if isinstance(fname, str):
            with Path(fname).open(encoding=encoding) as fp:
                for ix, line in enumerate(fp):
                    if isinstance(line, bytes):
                        _line = line.decode(encoding)
                    if ix < self.skip_row:
                        continue
                    row_name, row_template = self._get_row_template(_line)
                    fields = [
                        _line[dx[0] : dx[1]].strip() for dx in row_template.colpositions
                    ]
                    obj = dict(zip(row_template.names, fields, strict=False))
                    self._buckets[row_name].append(obj)
        else:
            for ix, line in enumerate(fname):
                if isinstance(line, bytes):
                    _line = line.decode(encoding)
                if ix < self.skip_row:
                    continue
                row_name, row_template = self._get_row_template(_line)
                fields = [
                    _line[dx[0] : dx[1]].strip() for dx in row_template.colpositions
                ]
                obj = dict(zip(row_template.names, fields, strict=False))
                self._buckets[row_name].append(obj)
        self._tables = {n: pd.DataFrame(b) for n, b in self._buckets.items()}
        for row_name, row_template in self._rows:
            for fn, f in row_template._fields.items():
                self._tables[row_name][fn] = f.parse(self._tables[row_name][fn])

    def __getattribute__(self, name: str):
        buckets = super().__getattribute__("_buckets")
        if name in buckets:
            return buckets[name]
        else:
            return super().__getattribute__(name)

    def _get_row_template(self, line):
        for row in self._rows:
            m = row[1].pattern.match(line)
            if m:
                return row
