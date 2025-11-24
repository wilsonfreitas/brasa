"""
fieldset-schema

A library for defining, managing, and applying data schemas with integrated
type parsing and data reading capabilities.
"""

__version__ = "0.1.0"

from .exceptions import FieldError, FieldsetError, TypeParseError, TypeDefinitionError
from .type_parser import (
    TypeParser, DateParser, DateTimeParser, TimeParser, NumericParser,
    IntegerParser, StringParser, BooleanParser, TypeDefinitionParser, TypeParserFactory
)
from .field import Field
from .fieldset import Fieldset
from .adapters.pandas_adapter import PandasAdapter
from .adapters.pyarrow_adapter import PyArrowAdapter
from .adapters.unified_reader import FieldsetReader

__all__ = [
    "FieldError",
    "FieldsetError",
    "TypeParseError",
    "TypeDefinitionError",
    "TypeParser",
    "DateParser",
    "DateTimeParser",
    "TimeParser",
    "NumericParser",
    "IntegerParser",
    "StringParser",
    "BooleanParser",
    "TypeDefinitionParser",
    "TypeParserFactory",
    "Field",
    "Fieldset",
    "PandasAdapter",
    "PyArrowAdapter",
    "FieldsetReader",
]
