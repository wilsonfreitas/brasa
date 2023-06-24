from datetime import datetime
import hashlib
import itertools
import logging
import os
import pickle
import warnings
import zipfile
from tempfile import gettempdir
from typing import IO

from bizdays import Calendar
from bizdays import set_option
from regexparser import TextParser


set_option("mode.datetype", "datetime")


class SuppressUserWarnings:
    def __enter__(self):
        warnings.filterwarnings("ignore", category=UserWarning)
    
    def __exit__(self, exc_type, exc_value, traceback):
        warnings.filterwarnings("default", category=UserWarning)

    
def generate_checksum_for_template(template: str, args: dict, extra_key: str="") -> str:
    """Generates a hash for a template and its arguments.

    The hash is used to identify a template and its arguments.
    """
    t = tuple(sorted(args.items(), key=lambda x: x[0]))
    obj = (template, t)
    if extra_key:
        obj = (template, t, extra_key)
    return hashlib.md5(pickle.dumps(obj)).hexdigest()


def generate_checksum_from_file(fp: IO) -> str:
    file_hash = hashlib.md5()
    while chunk := fp.read(8192):
        file_hash.update(chunk)
    fp.seek(0)
    return file_hash.hexdigest()


def unzip_file_to(fname, dest) -> list:
    zf = zipfile.ZipFile(fname)
    names = zf.namelist()
    for name in names:
        logging.debug("zipped file %s", name)
        zf.extract(name, dest)
    zf.close()
    return [os.path.join(dest, name) for name in names]


def unzip_recursive(fname):
    if isinstance(fname, str) and fname.lower().endswith(".zip"):
        fname = unzip_file_to(fname, gettempdir())
        return unzip_recursive(fname)
    elif isinstance(fname, list) and len(fname) == 1 and fname[0].lower().endswith(".zip"):
        fname = unzip_file_to(fname[0], gettempdir())
        return unzip_recursive(fname)
    else:
        return fname


def unzip_and_get_content(fname, index=-1, encode=False, encoding="latin1"):
    zf = zipfile.ZipFile(fname)
    name = zf.namelist()[index]
    logging.debug("zipped file %s", name)
    content = zf.read(name)
    zf.close()

    if encode:
        return content.decode(encoding)
    else:
        return content


def is_iterable(i):
    try:
        iter(i)
        return not isinstance(i, str)
    except TypeError:
        return False


class KwargsIterator:
    def __init__(self, kwargs: dict) -> None:
        self.elements = [list(x) if is_iterable(x) else [x] for x in kwargs.values()]
        self.__len = max(len(x) for x in self.elements)
        self.names = kwargs.keys()

    def __len__(self) -> int:
        return self.__len

    def __iter__(self):
        for kw in itertools.product(*self.elements):
            yield dict(tuple(zip(self.names, kw)))


class DateRange:
    def __init__(self,
                 start: datetime | None=None,
                 end: datetime | None=None,
                 year: int | None=None,
                 month: int | None=None,
                 calendar: str | None=None
                 ) -> None:
        if start is None and year is None:
            raise ValueError("Either start or year must be specified")

        self.calendar = Calendar() if calendar is None else Calendar.load(calendar)
        if start is not None:
            start = self.calendar.following(start)
        if start is not None and end is None:
                end = self.calendar.offset(datetime.today(), -1)
        else:
            end = min(self.calendar.preceding(end), self.calendar.offset(datetime.today(), -1))
        if year is not None:
            start = self.calendar.getdate("first bizday", year)
            end = self.calendar.getdate("last bizday", year)
            if end > datetime.today():
                end = self.calendar.offset(datetime.today(), -1)
        if year is not None and month is not None:
            start = self.calendar.getdate("first bizday", year, month)
            end = self.calendar.getdate("last bizday", year, month)
            if end > datetime.today():
                end = self.calendar.offset(datetime.today(), -1)
        self.year = year
        self.month = month
        self.start = start
        self.end = end

        self.dates = self.calendar.seq(self.start, self.end)

    def __len__(self) -> int:
        return len(self.dates)

    def __iter__(self):
        return iter(self.dates)


class DateRangeParser(TextParser):
    def __init__(self, calendar: str):
        super().__init__()
        self.calendar_name = calendar
        self.calendar = Calendar() if calendar == "actual" else Calendar.load(calendar)

    def parse_year(self, text, match):
        r"^\d{4}$"
        year = int(match.group())
        start = self.calendar.getdate("first day", year)
        end = self.calendar.getdate("last day", year)
        return DateRange(start=start, end=end, calendar=self.calendar_name)

    def parse_year_open_range(self, text, match):
        r"^(\d{4}):$"
        start = datetime(int(match.group(1)), 1, 1)
        return DateRange(start=start, calendar=self.calendar_name)

    def parse_year_range(self, text, match):
        r"^(\d{4}):(\d{4})$"
        start = datetime(int(match.group(1)), 1, 1)
        end = datetime(int(match.group(2)), 12, 31)
        return DateRange(start=start, end=end, calendar=self.calendar_name)

    def parse_year_open_range(self, text, match):
        r"^(\d{4}):$"
        start = datetime(int(match.group(1)), 1, 1)
        return DateRange(start=start, calendar=self.calendar_name)

    def parse_month(self, text, match):
        r"^(\d{4})-(\d{2})$"
        year=int(match.group(1))
        month=int(match.group(2))
        start = self.calendar.getdate("first day", year, month)
        end = self.calendar.getdate("last day", year, month)
        return DateRange(start=start, end=end, calendar=self.calendar_name)

    def parse_month_open_range(self, text, match):
        r"^(\d{4})-(\d{2}):$"
        year=int(match.group(1))
        month=int(match.group(2))
        start = self.calendar.getdate("first day", year, month)
        return DateRange(start=start, calendar=self.calendar_name)

    def parse_date(self, text, match):
        r"^(\d{4}-\d{2}-\d{2})$"
        start = datetime.strptime(match.group(1), "%Y-%m-%d")
        end = start
        return DateRange(start=start, end=end, calendar=self.calendar_name)

    def parse_date_open_range(self, text, match):
        r"^(\d{4}-\d{2}-\d{2}):$"
        start = datetime.strptime(match.group(1), "%Y-%m-%d")
        return DateRange(start=start, calendar=self.calendar_name)

    def parse_date_range(self, text, match):
        r"^(\d{4}-\d{2}-\d{2}):(\d{4}-\d{2}-\d{2})$"
        start = datetime.strptime(match.group(1), "%Y-%m-%d")
        end = datetime.strptime(match.group(2), "%Y-%m-%d")
        return DateRange(start=start, end=end, calendar=self.calendar_name)
