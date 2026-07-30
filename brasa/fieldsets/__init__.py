"""fieldset-schema"""

__version__ = "0.1.0"

from .adapters.pandas_adapter import PandasAdapter
from .adapters.pyarrow_adapter import get_target_schema
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
    "IntegerParser",
    "NumericParser",
    "PandasAdapter",
    "StringParser",
    "TimeParser",
    "TypeDefinitionError",
    "TypeDefinitionParser",
    "TypeParseError",
    "TypeParser",
    "TypeParserFactory",
    "get_target_schema",
]
