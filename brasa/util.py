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


class DateRange:
    def __init__(self,
                 start: datetime | None=None,
                 end: datetime | None=None,
                 year: int | None=None,
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
            end = self.calendar.preceding(end)
        if year is not None:
            start = self.calendar.getdate("first bizday", year)
            end = self.calendar.getdate("last bizday", year)
            if end > datetime.today():
                end = self.calendar.offset(datetime.today(), -1)
        self.year = year
        self.start = start
        self.end = end

        self.dates = self.calendar.seq(self.start, self.end)

    def __len__(self) -> int:
        return len(self.dates)

    def __iter__(self):
        return iter(self.dates)


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
