"""
Rudimentary Type System - A flexible string-to-type parser system.

This module provides a type system that parses input strings into various
data types based on string type definitions with optional parameters.
"""

import ast
from abc import ABC, abstractmethod
from datetime import date, datetime, time
from typing import Any, ClassVar


class TypeParser(ABC):
    """
    Abstract base class for all type parsers.

    All concrete parser implementations must inherit from this class
    and implement the parse() method.
    """

    def __init__(self, parameters: dict[str, Any] | None = None):
        """
        Initialize the parser with optional parameters.

        Args:
            parameters: Dictionary of parameters extracted from type definition
        """
        self.parameters = parameters or {}

    @abstractmethod
    def parse(self, value: str) -> Any:
        """
        Parse the input string into the appropriate type.

        Args:
            value: The string value to parse

        Returns:
            The parsed value in the appropriate type

        Raises:
            ValueError: If parsing fails
        """
        pass

    def get_type_name(self) -> str:
        """
        Get the name of this parser's type.

        Returns:
            Type name as string
        """
        # Remove 'Parser' suffix and convert to lowercase
        return self.__class__.__name__.replace("Parser", "").lower()


class DateParser(TypeParser):
    """Parser for date type."""

    DEFAULT_FORMAT = "%Y-%m-%d"

    def parse(self, value: str) -> date:
        """
        Parse string to date object.

        Args:
            value: String representation of date

        Returns:
            date object

        Raises:
            ValueError: If date parsing fails
        """
        date_format = self.parameters.get("format", self.DEFAULT_FORMAT)

        try:
            return datetime.strptime(value, date_format).date()
        except ValueError as e:
            raise ValueError(
                f"Failed to parse '{value}' as date with format '{date_format}': {e}"
            ) from e


class DateTimeParser(TypeParser):
    """Parser for datetime type."""

    DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S"

    def parse(self, value: str) -> datetime:
        """
        Parse string to datetime object.

        Args:
            value: String representation of datetime

        Returns:
            datetime object

        Raises:
            ValueError: If datetime parsing fails
        """
        datetime_format = self.parameters.get("format", self.DEFAULT_FORMAT)

        try:
            return datetime.strptime(value, datetime_format)
        except ValueError as e:
            raise ValueError(
                f"Failed to parse '{value}' as datetime with format '{datetime_format}': {e}"
            ) from e


class TimeParser(TypeParser):
    """Parser for time type."""

    DEFAULT_FORMAT = "%H:%M:%S"

    def parse(self, value: str) -> time:
        """
        Parse string to time object.

        Args:
            value: String representation of time

        Returns:
            time object

        Raises:
            ValueError: If time parsing fails
        """
        time_format = self.parameters.get("format", self.DEFAULT_FORMAT)

        try:
            return datetime.strptime(value, time_format).time()
        except ValueError as e:
            raise ValueError(
                f"Failed to parse '{value}' as time with format '{time_format}': {e}"
            ) from e


class NumericParser(TypeParser):
    """Parser for numeric (float) type with optional decimal, sign, and separator parameters."""

    def parse(self, value: str) -> float:
        """
        Parse string to float with optional decimal places, sign, and separators.

        Parameters:
            - dec: Number of implied decimal places (default: 0)
            - sign: Sign character to apply ('+' or '-', default: '+')
            - thousands: Thousands separator character (default: None)
            - decimal: Decimal separator character (default: '.')

        Args:
            value: String representation of number

        Returns:
            float value

        Raises:
            ValueError: If numeric parsing fails

        Examples:
            - "1.234.567,89" with thousands=".", decimal="," -> 1234567.89
            - "1,234,567.89" with thousands="," -> 1234567.89
            - "1234,56" with decimal="," -> 1234.56
        """
        try:
            # Get parameters
            decimal_places = self.parameters.get("dec", 0)
            sign = self.parameters.get("sign", "+")
            thousands_sep = self.parameters.get("thousands", None)
            decimal_sep = self.parameters.get("decimal", ".")

            # Validate separators are different
            if thousands_sep and thousands_sep == decimal_sep:
                raise ValueError(
                    f"Thousands separator '{thousands_sep}' cannot be the same as "
                    f"decimal separator '{decimal_sep}'"
                )

            # Process the value string
            processed_value = value

            # Remove thousands separator if specified
            if thousands_sep:
                processed_value = processed_value.replace(thousands_sep, "")

            # Replace decimal separator with standard dot if different
            if decimal_sep != ".":
                processed_value = processed_value.replace(decimal_sep, ".")

            # Parse the numeric value
            numeric_value = float(processed_value)

            # Apply implied decimal places
            if decimal_places > 0:
                numeric_value = numeric_value / (10**decimal_places)

            # Apply sign
            if sign == "-":
                numeric_value = -numeric_value
            elif sign != "+":
                raise ValueError(
                    f"Invalid sign parameter: '{sign}'. Must be '+' or '-'"
                )

            return numeric_value

        except ValueError as e:
            raise ValueError(f"Failed to parse '{value}' as numeric: {e}") from e


class IntegerParser(TypeParser):
    """Parser for integer type."""

    def parse(self, value: str) -> int:
        """
        Parse string to integer.

        Args:
            value: String representation of integer

        Returns:
            int value

        Raises:
            ValueError: If integer parsing fails
        """
        try:
            return int(value)
        except ValueError as e:
            raise ValueError(f"Failed to parse '{value}' as integer: {e}") from e


class StringParser(TypeParser):
    """Parser for string/character type (passthrough)."""

    def parse(self, value: str) -> str:
        """
        Return the value as-is (string).

        Args:
            value: String value

        Returns:
            The same string value
        """
        return value


class BooleanParser(TypeParser):
    """Parser for boolean type."""

    TRUE_VALUES: ClassVar[set[str]] = {"true", "t", "yes", "y", "1", "on"}
    FALSE_VALUES: ClassVar[set[str]] = {"false", "f", "no", "n", "0", "off"}

    def parse(self, value: str) -> bool:
        """
        Parse string to boolean.

        Recognizes: true/false, t/f, yes/no, y/n, 1/0, on/off (case-insensitive)

        Args:
            value: String representation of boolean

        Returns:
            bool value

        Raises:
            ValueError: If boolean parsing fails
        """
        normalized_value = value.lower().strip()

        if normalized_value in self.TRUE_VALUES:
            return True
        elif normalized_value in self.FALSE_VALUES:
            return False
        else:
            raise ValueError(
                f"Failed to parse '{value}' as boolean. "
                f"Valid values: {self.TRUE_VALUES | self.FALSE_VALUES}"
            )


class TypeDefinitionParser:
    """
    Parser for type definition strings.

    Extracts type name and parameters from strings like:
    - "date"
    - "date(format = '%Y%m%d')"
    - "numeric(dec = 2, sign = '-')"
    """

    @classmethod
    def parse(cls, type_definition: str) -> tuple[str, dict[str, Any]]:
        """Parse a type definition string into (type_name, parameters).

        Accepts a bare name ("date") or a call with keyword arguments
        ("date(format='%Y%m%d')") — the expression must be valid Python.
        """
        s = type_definition.strip()
        try:
            node = ast.parse(s, mode="eval").body
        except SyntaxError as e:
            raise ValueError(f"Invalid type definition format: '{s}'") from e
        if isinstance(node, ast.Name):
            return node.id, {}
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and not node.args
        ):
            try:
                params = {kw.arg: ast.literal_eval(kw.value) for kw in node.keywords}
            except ValueError as e:
                raise ValueError(f"Invalid parameters in '{s}'") from e
            return node.func.id, params
        raise ValueError(f"Invalid type definition format: '{s}'")


class TypeParserFactory:
    """
    Factory class for creating type parser instances.

    Maintains a registry of available type parsers and creates
    instances based on type definitions.
    """

    # Registry of available type parsers
    _registry: ClassVar[dict[str, type[TypeParser]]] = {
        "date": DateParser,
        "datetime": DateTimeParser,
        "time": TimeParser,
        "numeric": NumericParser,
        "number": NumericParser,  # Alias for numeric
        "integer": IntegerParser,
        "int": IntegerParser,  # Alias for integer
        "string": StringParser,
        "character": StringParser,  # Alias for string
        "boolean": BooleanParser,
    }

    @classmethod
    def create_parser(cls, type_definition: str) -> TypeParser:
        """
        Create a parser instance based on type definition string.

        Args:
            type_definition: Type definition string (e.g., "date(format = '%Y%m%d')")

        Returns:
            Instance of appropriate TypeParser subclass

        Raises:
            ValueError: If type is not recognized or definition is invalid
        """
        # Parse the type definition
        type_name, parameters = TypeDefinitionParser.parse(type_definition)

        # Look up parser class
        parser_class = cls._registry.get(type_name.lower())
        if parser_class is None:
            raise ValueError(
                f"Unrecognized type: '{type_name}'. "
                f"Available types: {list(cls._registry.keys())}"
            )

        # Create and return parser instance
        return parser_class(parameters)
