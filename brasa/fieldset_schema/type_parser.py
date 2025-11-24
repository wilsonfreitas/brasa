"""
Rudimentary Type System - A flexible string-to-type parser system.

This module provides a type system that parses input strings into various
data types based on string type definitions with optional parameters.
"""

from abc import ABC, abstractmethod
from datetime import datetime, date, time
from typing import Any, Dict, Optional, Type
import re

from .exceptions import TypeParseError, TypeDefinitionError


class TypeParser(ABC):
    """
    Abstract base class for all type parsers.
    
    All concrete parser implementations must inherit from this class
    and implement the parse() method.
    """
    
    def __init__(self, parameters: Optional[Dict[str, Any]] = None):
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
            TypeParseError: If parsing fails
        """
        pass
    
    def get_type_name(self) -> str:
        """
        Get the name of this parser's type.
        
        Returns:
            Type name as string
        """
        # Remove 'Parser' suffix and convert to lowercase
        return self.__class__.__name__.replace('Parser', '').lower()


class DateParser(TypeParser):
    """Parser for date type."""
    
    DEFAULT_FORMAT = '%Y-%m-%d'
    
    def parse(self, value: str) -> date:
        """
        Parse string to date object.
        
        Args:
            value: String representation of date
            
        Returns:
            date object
            
        Raises:
            TypeParseError: If date parsing fails
        """
        date_format = self.parameters.get('format', self.DEFAULT_FORMAT)
        
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError as e:
            raise TypeParseError(
                f"Failed to parse '{value}' as date with format '{date_format}': {e}"
            )


class DateTimeParser(TypeParser):
    """Parser for datetime type."""
    
    DEFAULT_FORMAT = '%Y-%m-%d %H:%M:%S'
    
    def parse(self, value: str) -> datetime:
        """
        Parse string to datetime object.
        
        Args:
            value: String representation of datetime
            
        Returns:
            datetime object
            
        Raises:
            TypeParseError: If datetime parsing fails
        """
        datetime_format = self.parameters.get('format', self.DEFAULT_FORMAT)
        
        try:
            return datetime.strptime(value, datetime_format)
        except ValueError as e:
            raise TypeParseError(
                f"Failed to parse '{value}' as datetime with format '{datetime_format}': {e}"
            )


class TimeParser(TypeParser):
    """Parser for time type."""
    
    DEFAULT_FORMAT = '%H:%M:%S'
    
    def parse(self, value: str) -> time:
        """
        Parse string to time object.
        
        Args:
            value: String representation of time
            
        Returns:
            time object
            
        Raises:
            TypeParseError: If time parsing fails
        """
        time_format = self.parameters.get('format', self.DEFAULT_FORMAT)
        
        try:
            return datetime.strptime(value, time_format).time()
        except ValueError as e:
            raise TypeParseError(
                f"Failed to parse '{value}' as time with format '{time_format}': {e}"
            )


class NumericParser(TypeParser):
    """Parser for numeric (float) type with optional decimal and sign parameters."""
    
    def parse(self, value: str) -> float:
        """
        Parse string to float with optional decimal places and sign.
        
        Parameters:
            - dec: Number of decimal places (default: 0)
            - sign: Sign character to apply ('+' or '-', default: '+')
        
        Args:
            value: String representation of number
            
        Returns:
            float value
            
        Raises:
            TypeParseError: If numeric parsing fails
        """
        try:
            # Get parameters
            decimal_places = self.parameters.get('dec', 0)
            sign = self.parameters.get('sign', '+')
            
            # Parse the numeric value
            numeric_value = float(value)
            
            # Apply decimal places
            if decimal_places > 0:
                numeric_value = numeric_value / (10 ** decimal_places)
            
            # Apply sign
            if sign == '-':
                numeric_value = -numeric_value
            elif sign != '+':
                raise TypeParseError(f"Invalid sign parameter: '{sign}'. Must be '+' or '-'")
            
            return numeric_value
            
        except ValueError as e:
            raise TypeParseError(
                f"Failed to parse '{value}' as numeric: {e}"
            )


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
            TypeParseError: If integer parsing fails
        """
        try:
            return int(value)
        except ValueError as e:
            raise TypeParseError(
                f"Failed to parse '{value}' as integer: {e}"
            )


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
    
    TRUE_VALUES = {'true', 't', 'yes', 'y', '1', 'on'}
    FALSE_VALUES = {'false', 'f', 'no', 'n', '0', 'off'}
    
    def parse(self, value: str) -> bool:
        """
        Parse string to boolean.
        
        Recognizes: true/false, t/f, yes/no, y/n, 1/0, on/off (case-insensitive)
        
        Args:
            value: String representation of boolean
            
        Returns:
            bool value
            
        Raises:
            TypeParseError: If boolean parsing fails
        """
        normalized_value = value.lower().strip()
        
        if normalized_value in self.TRUE_VALUES:
            return True
        elif normalized_value in self.FALSE_VALUES:
            return False
        else:
            raise TypeParseError(
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
    
    # Regex pattern to match type definitions with optional parameters
    PATTERN = re.compile(
        r"^(\w+)(?:\((.*)\))?$"
    )
    
    @classmethod
    def parse(cls, type_definition: str) -> tuple[str, Dict[str, Any]]:
        """
        Parse a type definition string into type name and parameters.
        
        Args:
            type_definition: String like "date(format = '%Y%m%d')"
            
        Returns:
            Tuple of (type_name, parameters_dict)
            
        Raises:
            TypeDefinitionError: If definition format is invalid
        """
        type_definition = type_definition.strip()
        
        match = cls.PATTERN.match(type_definition)
        if not match:
            raise TypeDefinitionError(
                f"Invalid type definition format: '{type_definition}'"
            )
        
        type_name = match.group(1)
        params_string = match.group(2)
        
        parameters = {}
        if params_string:
            parameters = cls._parse_parameters(params_string)
        
        return type_name, parameters
    
    @classmethod
    def _parse_parameters(cls, params_string: str) -> Dict[str, Any]:
        """
        Parse parameter string into dictionary.
        
        Args:
            params_string: String like "format = '%Y%m%d', dec = 2"
            
        Returns:
            Dictionary of parameters
            
        Raises:
            TypeDefinitionError: If parameter format is invalid
        """
        parameters = {}
        
        # Split by comma, but be careful with quoted strings
        param_pairs = cls._split_parameters(params_string)
        
        for pair in param_pairs:
            if '=' not in pair:
                raise TypeDefinitionError(
                    f"Invalid parameter format: '{pair}'. Expected 'key = value'"
                )
            
            key, value = pair.split('=', 1)
            key = key.strip()
            value = value.strip()
            
            # Remove quotes if present
            if (value.startswith("'") and value.endswith("'")) or \
               (value.startswith('"') and value.endswith('"')):
                value = value[1:-1]
            else:
                # Try to convert to appropriate type
                value = cls._convert_value(value)
            
            parameters[key] = value
        
        return parameters
    
    @classmethod
    def _split_parameters(cls, params_string: str) -> list[str]:
        """
        Split parameter string by comma, respecting quoted strings.
        
        Args:
            params_string: Parameter string
            
        Returns:
            List of parameter pairs
        """
        params = []
        current_param = []
        in_quotes = False
        quote_char = None
        
        for char in params_string:
            if char in ('"', "'") and not in_quotes:
                in_quotes = True
                quote_char = char
                current_param.append(char)
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
                current_param.append(char)
            elif char == ',' and not in_quotes:
                params.append(''.join(current_param))
                current_param = []
            else:
                current_param.append(char)
        
        if current_param:
            params.append(''.join(current_param))
        
        return params
    
    @classmethod
    def _convert_value(cls, value: str) -> Any:
        """
        Convert string value to appropriate Python type.
        
        Args:
            value: String value
            
        Returns:
            Converted value (int, float, or str)
        """
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Keep as string
        return value


class TypeParserFactory:
    """
    Factory class for creating type parser instances.
    
    Maintains a registry of available type parsers and creates
    instances based on type definitions.
    """
    
    # Registry of available type parsers
    _registry: Dict[str, Type[TypeParser]] = {
        'date': DateParser,
        'datetime': DateTimeParser,
        'time': TimeParser,
        'numeric': NumericParser,
        'number': NumericParser,  # Alias for numeric
        'integer': IntegerParser,
        'int': IntegerParser,  # Alias for integer
        'string': StringParser,
        'character': StringParser,  # Alias for string
        'char': StringParser,  # Alias for string
        'boolean': BooleanParser,
        'bool': BooleanParser,  # Alias for boolean
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
            TypeDefinitionError: If type is not recognized or definition is invalid
        """
        # Parse the type definition
        type_name, parameters = TypeDefinitionParser.parse(type_definition)
        
        # Look up parser class
        parser_class = cls._registry.get(type_name.lower())
        if parser_class is None:
            raise TypeDefinitionError(
                f"Unrecognized type: '{type_name}'. "
                f"Available types: {list(cls._registry.keys())}"
            )
        
        # Create and return parser instance
        return parser_class(parameters)
    
    @classmethod
    def register_parser(cls, type_name: str, parser_class: Type[TypeParser]) -> None:
        """
        Register a new type parser.
        
        This allows extending the type system with custom parsers.
        
        Args:
            type_name: Name of the type
            parser_class: Parser class (must inherit from TypeParser)
            
        Raises:
            ValueError: If parser_class doesn't inherit from TypeParser
        """
        if not issubclass(parser_class, TypeParser):
            raise ValueError(
                f"Parser class must inherit from TypeParser. "
                f"Got: {parser_class.__name__}"
            )
        
        cls._registry[type_name.lower()] = parser_class
    
    @classmethod
    def get_available_types(cls) -> list[str]:
        """
        Get list of available type names.
        
        Returns:
            List of registered type names
        """
        return list(cls._registry.keys())
