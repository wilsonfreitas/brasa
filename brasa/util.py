import hashlib
import itertools
import json
import logging
import pickle
import re
import warnings
import zipfile
from datetime import date, datetime, timedelta
from io import BytesIO
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


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


def _normalize_download_arg(value: Any) -> Any:
    """Normalize a download arg value to its canonical form.

    Canonical form for date-like values is YYYY-MM-DDTHH:MM:SS.
    Other values are returned unchanged.
    """
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
    if isinstance(value, str) and _DATE_RE.match(value):
        return f"{value}T00:00:00"
    return value


def _to_download_arg_object(value: Any) -> Any:
    """Reconstruct a rich Python object from a canonical download arg value."""
    if isinstance(value, str) and _DATETIME_RE.match(value):
        return datetime.fromisoformat(value)
    return value


class DownloadArgs:
    """Canonical, serialization-stable container for download arguments.

    Values are stored in canonical form: date/datetime always as
    "YYYY-MM-DDTHH:MM:SS" strings; other primitives unchanged.
    Rich objects are reconstructed on demand via get_object().
    """

    def __init__(self, data: dict[str, Any]) -> None:
        self._data: dict[str, Any] = {
            k: _normalize_download_arg(v) for k, v in data.items()
        }

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def items(self):
        return self._data.items()

    def keys(self):
        return self._data.keys()

    def __iter__(self):
        return iter(self._data)

    def get_object(self, key: str) -> Any:
        """Return the value as a rich Python type (datetime for date strings, etc.)."""
        return _to_download_arg_object(self._data[key])

    def to_json(self) -> str:
        """Serialize to JSON — always stable, no custom encoder needed."""
        return json.dumps(self._data)

    @classmethod
    def from_json(cls, s: str) -> "DownloadArgs":
        """Deserialize from JSON — normalizes values to canonical form.

        Normalizes on load so existing DB rows with bare 'YYYY-MM-DD'
        strings are upgraded to 'YYYY-MM-DDTHH:MM:SS' on first read.
        """
        obj = cls.__new__(cls)
        raw = json.loads(s)
        obj._data = {k: _normalize_download_arg(v) for k, v in raw.items()}
        return obj

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dict copy (for **unpacking into downloaders)."""
        return {k: _to_download_arg_object(v) for k, v in self._data.items()}


def generate_checksum_for_template(
    template: str, args: "DownloadArgs", extra_key: str = ""
) -> str:
    """Generates a hash for a template and its arguments.

    The hash is used to identify a template and its arguments.
    Values in args are already canonical — no normalization needed.
    """
    t = tuple(sorted(args.items(), key=lambda x: x[0]))
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


_ZIP_CHECKSUM_MAX_DEPTH = 8


def _hash_zip_contents(fp: IO, depth: int) -> str:
    """Recursive helper for generate_checksum_from_zip."""
    if depth > _ZIP_CHECKSUM_MAX_DEPTH:
        raise RecursionError(
            f"zip nesting exceeds maximum depth ({_ZIP_CHECKSUM_MAX_DEPTH})"
        )
    file_hash = hashlib.md5()
    with zipfile.ZipFile(fp) as zf:
        for name in sorted(zf.namelist()):
            file_hash.update(name.encode("utf-8"))
            file_hash.update(b"\x00")
            content = zf.read(name)
            if zipfile.is_zipfile(BytesIO(content)):
                inner = _hash_zip_contents(BytesIO(content), depth + 1)
                file_hash.update(inner.encode("ascii"))
            else:
                file_hash.update(content)
            file_hash.update(b"\x00")
    return file_hash.hexdigest()


def generate_checksum_from_zip(fp: IO) -> str:
    """Content-based MD5 checksum of a zip archive's logical contents.

    Recursively hashes (name + content) pairs in sorted order, so identical
    logical payloads produce identical checksums regardless of
    non-deterministic zip metadata (modification timestamps, OS byte, extra
    fields, central-directory ordering). Inner zips are hashed structurally
    via recursion (cap: 8 levels) rather than by their container bytes.

    The file pointer is rewound to position 0 before returning (even on
    error), so callers can still stream the original bytes downstream.

    Args:
        fp: A seekable file-like object positioned at the start of a zip.

    Returns:
        Hex-encoded MD5 digest of the logical contents.

    Raises:
        RecursionError: If zip nesting exceeds _ZIP_CHECKSUM_MAX_DEPTH.
        zipfile.BadZipFile: If fp does not contain a valid zip.
    """
    try:
        return _hash_zip_contents(fp, 0)
    finally:
        fp.seek(0)


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

    def parse_datetime_ms(self, _text, match):
        r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)$"
        return datetime.fromisoformat(match.group(1))

    def parse_datetime(self, _text, match):
        r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})$"
        return datetime.fromisoformat(match.group(1))

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


_ISO_DATE_PATTERN = re.compile(
    r"^\d{4}-\d{2}(?:-\d{2})?(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?)?(?::(?:\d{4}-\d{2}(?:-\d{2})?)?)?(?:~\w+)?$"
)

_NAMED_DATE_VARS = {
    "today": lambda: datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0
    ),
    "yesterday": lambda: (
        datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        - timedelta(days=1)
    ),
}


def _looks_like_date(value: str) -> bool:
    """Quick check if value could be an ISO date or date range.

    Matches: YYYY-MM, YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, date ranges, etc.
    Also allows a trailing ~CALENDAR suffix.
    """
    return bool(_ISO_DATE_PATTERN.match(value))


def parse_arg_value(value: str, default_calendar: str = "B3"):
    """Parse a CLI --arg value using the prefix DSL.

    Resolution order:
        1. @ prefix — date/range via DateRangeParser, named vars (@today,
           @yesterday), @YYYY year ranges. Optional ~CALENDAR suffix.
        2. ISO date auto-detect — bare YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS,
           YYYY-MM → parsed as date/range. No @ needed.
        3. $ prefix — symbol lookup via get_symbols().
        4. Comma rule — splits into list, each element parsed individually.
        5. Integer — bare numeric string → int.
        6. String — fallback.

    Args:
        value: The raw string value from the CLI.
        default_calendar: Default calendar for @ date values.

    Returns:
        Parsed value: str, int, datetime, list, or DateRange.
    """
    # @ prefix — dates, named vars, year ranges
    if value.startswith("@"):
        date_str = value[1:]
        calendar = default_calendar
        if "~" in date_str:
            date_str, calendar = date_str.rsplit("~", 1)

        # Named date variables
        if date_str in _NAMED_DATE_VARS:
            return _NAMED_DATE_VARS[date_str]()

        return DateRangeParser(calendar).parse(date_str)

    # Symbol prefix
    if value.startswith("$"):
        from brasa.queries import get_symbols

        return get_symbols(value[1:])

    # Auto-detect ISO dates (YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, YYYY-MM, ranges)
    if _looks_like_date(value):
        date_str = value
        calendar = default_calendar
        if "~" in date_str:
            date_str, calendar = date_str.rsplit("~", 1)
        try:
            return DateRangeParser(calendar).parse(date_str)
        except Exception:
            pass

    # Comma-separated list
    if "," in value:
        return [_parse_scalar(v) for v in value.split(",")]

    return _parse_scalar(value)


def _parse_scalar(value: str):
    """Parse a single scalar value: integer if numeric, else string."""
    try:
        return int(value)
    except ValueError:
        return value
