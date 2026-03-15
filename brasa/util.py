import hashlib
import itertools
import logging
import pickle
import warnings
import zipfile
from datetime import date, datetime
from pathlib import Path
from tempfile import gettempdir
from typing import IO, Any

from bizdays import Calendar, set_option
from regexparser import TextParser

set_option("mode.datetype", "datetime")


class SuppressUserWarnings:
    def __enter__(self):
        warnings.filterwarnings("ignore", category=UserWarning)

    def __exit__(self, exc_type, exc_value, traceback):
        warnings.filterwarnings("default", category=UserWarning)


def generate_checksum_for_template(
    template: str, args: dict, extra_key: str = ""
) -> str:
    """Generates a hash for a template and its arguments.

    The hash is used to identify a template and its arguments.
    """
    normalized = {
        k: datetime(v.year, v.month, v.day)
        if isinstance(v, date) and not isinstance(v, datetime)
        else v
        for k, v in args.items()
    }
    t = tuple(sorted(normalized.items(), key=lambda x: x[0]))
    obj: Any = (template, t)
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
    return [str(Path(dest) / name) for name in names]


def _is_zip(fname):
    """Check if a file is a zip archive (by content, not extension)."""
    return isinstance(fname, str) and zipfile.is_zipfile(fname)


def unzip_recursive(fname):
    if _is_zip(fname):
        fname = unzip_file_to(fname, gettempdir())
        return unzip_recursive(fname)
    elif isinstance(fname, list) and len(fname) == 1 and _is_zip(fname[0]):
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
        if len(self.elements) == 0:
            self.__len = 0
            self.names = []
        else:
            self.__len = 1
            for x in self.elements:
                self.__len *= len(x)
            self.names = list(kwargs.keys())

    def __len__(self) -> int:
        return self.__len

    def __iter__(self):
        for kw in itertools.product(*self.elements):
            yield dict(tuple(zip(self.names, kw, strict=False)))


class DateRange:
    def __init__(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        year: int | None = None,
        month: int | None = None,
        calendar: str | None = None,
    ) -> None:
        if start is None and year is None:
            raise ValueError("Either start or year must be specified")

        self.calendar = Calendar() if calendar is None else Calendar.load(calendar)
        if start is not None:
            start = self.calendar.following(start)
        if start is not None and end is None:
            end = self.calendar.offset(datetime.today(), -1)
        elif start is not None and end is not None:
            end = min(
                self.calendar.preceding(end), self.calendar.offset(datetime.today(), -1)
            )
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

    def parse_year(self, _text, match):
        r"^\d{4}$"
        year = int(match.group())
        start = self.calendar.getdate("first day", year)
        end = self.calendar.getdate("last day", year)
        return DateRange(start=start, end=end, calendar=self.calendar_name)

    def parse_year_open_range(self, _text, match):
        r"^(\d{4}):$"
        start = datetime(int(match.group(1)), 1, 1)
        return DateRange(start=start, calendar=self.calendar_name)

    def parse_year_range(self, _text, match):
        r"^(\d{4}):(\d{4})$"
        start = datetime(int(match.group(1)), 1, 1)
        end = datetime(int(match.group(2)), 12, 31)
        return DateRange(start=start, end=end, calendar=self.calendar_name)

    def parse_month(self, _text, match):
        r"^(\d{4})-(\d{2})$"
        year = int(match.group(1))
        month = int(match.group(2))
        start = self.calendar.getdate("first day", year, month)
        end = self.calendar.getdate("last day", year, month)
        return DateRange(start=start, end=end, calendar=self.calendar_name)

    def parse_month_open_range(self, _text, match):
        r"^(\d{4})-(\d{2}):$"
        year = int(match.group(1))
        month = int(match.group(2))
        start = self.calendar.getdate("first day", year, month)
        return DateRange(start=start, calendar=self.calendar_name)

    def parse_date(self, _text, match):
        r"^(\d{4}-\d{2}-\d{2})$"
        start = datetime.strptime(match.group(1), "%Y-%m-%d")
        return [start]

    def parse_date_open_range(self, _text, match):
        r"^(\d{4}-\d{2}-\d{2}):$"
        start = datetime.strptime(match.group(1), "%Y-%m-%d")
        return DateRange(start=start, calendar=self.calendar_name)

    def parse_date_range(self, _text, match):
        r"^(\d{4}-\d{2}-\d{2}):(\d{4}-\d{2}-\d{2})$"
        start = datetime.strptime(match.group(1), "%Y-%m-%d")
        end = datetime.strptime(match.group(2), "%Y-%m-%d")
        return DateRange(start=start, end=end, calendar=self.calendar_name)
