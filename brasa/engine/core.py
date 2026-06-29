"""Core utilities and patterns for the engine module.

This module provides foundational classes and utility functions used across
the engine package, including the Singleton pattern and JSON serialization helpers.
"""

import abc
import re
from collections.abc import Callable
from datetime import date, datetime


def json_convert_from_object(obj):
    """Convert Python objects to JSON-serializable format.

    Handles datetime and date serialization to ISO format.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return datetime(obj.year, obj.month, obj.day).isoformat()
    raise TypeError(f"Object of type '{type(obj).__name__}' is not JSON serializable")


def json_convert_to_object(obj):
    """Convert JSON objects back to Python objects.

    Handles ISO format date strings to datetime objects.
    """
    date_pattern = r"\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})?)?$"
    for key, value in obj.items():
        if isinstance(value, str) and re.match(date_pattern, value):
            obj[key] = datetime.fromisoformat(value)
    return obj


def load_function_by_name(func_name: str) -> Callable:
    """Dynamically load a function by its fully qualified name.

    Args:
        func_name: Fully qualified function name (e.g., 'module.submodule.function')

    Returns:
        The loaded function object.
    """
    module_name, func_name = func_name.rsplit(".", 1)
    module = __import__(module_name, fromlist=[func_name])
    func = getattr(module, func_name)
    return func


class Singleton(abc.ABC):
    """Abstract base class implementing the Singleton pattern.

    Subclasses must implement the `init` method instead of `__init__`.
    """

    def __new__(cls, *args, **kwds):
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        it = object.__new__(cls)
        cls.__it__ = it
        it.init(*args, **kwds)
        return it

    @abc.abstractmethod
    def init(self, *args, **kwds):
        """Initialize the singleton instance.

        This method is called only once when the singleton is first created.
        """
        ...
