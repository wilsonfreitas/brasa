
from brasa.util import DateRange, KwargsIterator


def test_smart_kwargs() -> None:

    kwargs = {
        "name": "test",
        "color": ["red", "blue", "green"],
    }

    args = KwargsIterator(kwargs)
    kwargs_list = list(args)
    assert isinstance(kwargs_list, list)


def test_smart_kwargs2() -> None:

    kwargs = {
        "refdate": DateRange(year=2020),
    }

    args = KwargsIterator(kwargs)
    kwargs_dict = list(args)
    assert isinstance(kwargs_dict, list)