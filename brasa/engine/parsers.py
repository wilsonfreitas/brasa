"""Field parsing classes for converting raw text to typed values.

This module provides a hierarchy of field handlers that parse text data
into appropriate Python types (strings, numbers, dates). It uses the
Strategy pattern for different parsing strategies and a Factory pattern
for handler creation.
"""

import re
from datetime import datetime

import pandas as pd
import regexparser


class NumericParser(regexparser.NumberParser):
    """Parser for numeric values in standard (English) format."""

    def parseText(self, _text: str) -> str | None:
        return None


class PtBRNumericParser(regexparser.TextParser):
    """Parser for numeric values in Brazilian Portuguese format.

    Handles:
    - Integers
    - Numbers with thousands separator (.) and decimal separator (,)
    - Numbers with only decimal separator (,)
    """

    def parseInteger(self, text: str, _match: re.Match) -> int:
        r"^-?\s*\d+$"
        return eval(text)

    def parse_number_with_thousands_ptBR(self, text: str, _match: re.Match) -> float:
        r"^-?\s*(\d+\.)+\d+,\d+?$"
        text = text.replace(".", "")
        text = text.replace(",", ".")
        return eval(text)

    def parse_number_decimal_ptBR(self, text: str, _match: re.Match) -> float:
        r"^-?\s*\d+,\d+?$"
        text = text.replace(",", ".")
        return eval(text)

    def parseText(self, _text: str) -> str | None:
        return None


class FieldHandler:
    """Base handler for parsing field values.

    Uses a generic parser that attempts to automatically detect the value type.
    """

    def __init__(self, handler: dict | None) -> None:
        self.format = ""
        if handler is not None:
            self.__dict__.update(handler)
        self.is_empty = handler is None
        self.parser = regexparser.GenericParser()

    def parse(self, value: str | pd.Series) -> str | int | float | datetime | pd.Series:
        """Parse a value or series of values.

        Args:
            value: A string or pandas Series to parse.

        Returns:
            Parsed value(s) in the appropriate type.
        """
        if isinstance(value, str):
            return self.parser.parse(value)
        else:
            return value.apply(self.parser.parse)


class CharacterFieldHandler(FieldHandler):
    """Handler for character/string fields.

    Simply returns values as strings without transformation.
    """

    def __init__(self, handler: dict | None) -> None:
        super().__init__(handler)

    def parse(self, value: str | pd.Series) -> str | pd.Series:
        if isinstance(value, str):
            return value
        else:
            return value.astype(str)


class NumericFieldHandler(FieldHandler):
    """Handler for numeric fields.

    Supports both standard and Brazilian Portuguese number formats
    based on the 'format' configuration.
    """

    def __init__(self, handler: dict | None) -> None:
        super().__init__(handler)
        if self.format == "pt-br":
            self.parser = PtBRNumericParser()
        else:
            self.parser = NumericParser()


class DateFieldHandler(FieldHandler):
    """Handler for date fields.

    Parses date strings using the format specified in the handler configuration.
    """

    def __init__(self, handler: dict | None) -> None:
        super().__init__(handler)

    def parse(self, value: str | pd.Series) -> datetime | pd.Series | None:
        def func(value) -> datetime | None:
            try:
                return datetime.strptime(value, self.format)
            except ValueError:
                return None

        if isinstance(value, str):
            return func(value)
        else:
            return value.apply(func)


class FieldHandlerFactory:
    """Factory for creating appropriate field handlers based on type configuration."""

    @classmethod
    def create(cls, handler: dict | None) -> FieldHandler:
        """Create a field handler based on the handler configuration.

        Args:
            handler: Dictionary containing handler configuration with 'type' key.

        Returns:
            Appropriate FieldHandler subclass instance.
        """
        if handler is None or handler.get("type") is None:
            return FieldHandler(handler)
        elif handler["type"] == "numeric":
            return NumericFieldHandler(handler)
        elif handler["type"].lower() == "date" or handler["type"].lower() == "posixct":
            return DateFieldHandler(handler)
        elif handler["type"] == "character":
            return CharacterFieldHandler(handler)
        else:
            return FieldHandler(handler)


class TemplateField:
    """Represents a single field in a template with its parsing configuration."""

    def __init__(self, **kwargs) -> None:
        self.name = kwargs["name"]
        self.description = kwargs.get("description")
        self.width = kwargs.get("width", -1)
        self.handler = FieldHandlerFactory.create(kwargs.get("handler"))

    def parse(self, value: str | pd.Series) -> str | float | datetime | pd.Series:
        """Parse a value using this field's handler."""
        return self.handler.parse(value)


class TemplateFields:
    """Collection of template fields with dictionary-like access."""

    def __init__(self, fields: list) -> None:
        self.__fields = {f["name"]: TemplateField(**f) for f in fields}
        self.names = list(self.__fields.keys())

    def __len__(self) -> int:
        return len(self.__fields)

    def __getitem__(self, key: str) -> TemplateField:
        return self.__fields[key]

    def __iter__(self):
        return iter(self.__fields.values())
