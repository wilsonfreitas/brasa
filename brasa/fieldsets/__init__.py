"""
fieldset-schema

A library for defining, managing, and applying data schemas with integrated
type parsing and data reading capabilities.
"""

__version__ = "0.1.0"

from .adapters.pandas_adapter import PandasAdapter
from .adapters.pyarrow_adapter import PyArrowAdapter
from .adapters.unified_reader import FieldsetReader
from .exceptions import FieldError, FieldsetError, TypeDefinitionError, TypeParseError
from .field import Field
from .fieldset import Fieldset
from .type_parser import (
    BooleanParser,
    DateParser,
    DateTimeParser,
    IntegerParser,
    NumericParser,
    StringParser,
    TimeParser,
    TypeDefinitionParser,
    TypeParser,
    TypeParserFactory,
)

__all__ = [
    "BooleanParser",
    "DateParser",
    "DateTimeParser",
    "Field",
    "FieldError",
    "Fieldset",
    "FieldsetError",
    "FieldsetReader",
    "IntegerParser",
    "NumericParser",
    "PandasAdapter",
    "PyArrowAdapter",
    "StringParser",
    "TimeParser",
    "TypeDefinitionError",
    "TypeDefinitionParser",
    "TypeParseError",
    "TypeParser",
    "TypeParserFactory",
]
