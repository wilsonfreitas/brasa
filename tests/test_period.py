
from datetime import datetime
from brasa.util import DateRange, is_iterable
from bizdays import Calendar


def test_start_end_period() -> None:
    p = DateRange(start=datetime(2023, 1, 1), end=datetime(2023, 1, 31), calendar="B3")
    assert p.start == datetime(2023, 1, 2)
    assert p.end == datetime(2023, 1, 31)


def test_start_no_end_period() -> None:
    cal = Calendar.load("B3")
    p = DateRange(start=datetime(2023, 1, 1), calendar="B3")
    assert p.start == datetime(2023, 1, 2)
    assert p.end == cal.offset(datetime.today(), -1)

def test_year_period() -> None:
    p = DateRange(year=2022, calendar="B3")
    assert p.start == datetime(2022, 1, 3)
    assert p.end == datetime(2022, 12, 29)


def test_current_year_period() -> None:
    year = datetime.today().year
    p = DateRange(year=year, calendar="B3")
    cal = Calendar.load("B3")
    assert p.start == cal.getdate("first bizday", year)
    assert p.end == cal.offset(datetime.today(), -1)


def test_if_is_iter() -> None:
    p = DateRange(year=2022, calendar="B3")
    assert is_iterable(p)