
from brasa.util import DateRange, iterate_kwargs


def test_smart_kwargs() -> None:

    kwargs = {
        "name": "test",
        "color": ["red", "blue", "green"],
    }

    kwargs_list = list(iterate_kwargs(**kwargs))
    assert isinstance(kwargs_list, list)


def test_smart_kwargs2() -> None:

    kwargs = {
        "refdate": DateRange(year=2020),
    }

    kwargs_dict = list(iterate_kwargs(**kwargs))
    assert isinstance(kwargs_dict, list)