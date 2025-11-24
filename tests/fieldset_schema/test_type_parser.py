import pytest
from datetime import date, datetime, time

from brasa.fieldset_schema.type_parser import (
    DateParser, DateTimeParser, TimeParser, NumericParser,
    IntegerParser, StringParser, BooleanParser,
    TypeDefinitionParser, TypeParserFactory, TypeParser
)
from brasa.fieldset_schema.exceptions import TypeParseError, TypeDefinitionError


# --- TypeDefinitionParser Tests ---
def test_type_definition_parser_no_params():
    type_name, params = TypeDefinitionParser.parse("integer")
    assert type_name == "integer"
    assert params == {}

def test_type_definition_parser_with_params():
    type_name, params = TypeDefinitionParser.parse("date(format = '%Y-%m-%d')")
    assert type_name == "date"
    assert params == {'format': '%Y-%m-%d'}

def test_type_definition_parser_multiple_params():
    type_name, params = TypeDefinitionParser.parse("numeric(dec = 2, sign = '-')")
    assert type_name == "numeric"
    assert params == {'dec': 2, 'sign': '-'}

def test_type_definition_parser_quoted_params():
    type_name, params = TypeDefinitionParser.parse("string(pattern = '^[A-Z]+$', default='N/A')")
    assert type_name == "string"
    assert params == {'pattern': '^[A-Z]+$', 'default': 'N/A'}

def test_type_definition_parser_invalid_format():
    with pytest.raises(TypeDefinitionError):
        TypeDefinitionParser.parse("invalid(param=value")
    with pytest.raises(TypeDefinitionError):
        TypeDefinitionParser.parse("invalid_type(param)")
    with pytest.raises(TypeDefinitionError):
        TypeDefinitionParser.parse("invalid_type(param:value)")

def test_type_definition_parser_param_conversion():
    type_name, params = TypeDefinitionParser.parse("test(int_val=123, float_val=4.56, str_val='abc')")
    assert params == {'int_val': 123, 'float_val': 4.56, 'str_val': 'abc'}


# --- TypeParserFactory Tests ---
def test_type_parser_factory_create_date_parser():
    parser = TypeParserFactory.create_parser("date")
    assert isinstance(parser, DateParser)
    assert parser.parameters == {}

def test_type_parser_factory_create_date_parser_with_format():
    parser = TypeParserFactory.create_parser("date(format='%d/%m/%Y')")
    assert isinstance(parser, DateParser)
    assert parser.parameters == {'format': '%d/%m/%Y'}

def test_type_parser_factory_create_numeric_parser():
    parser = TypeParserFactory.create_parser("numeric(dec=2)")
    assert isinstance(parser, NumericParser)
    assert parser.parameters == {'dec': 2}

def test_type_parser_factory_unrecognized_type():
    with pytest.raises(TypeDefinitionError, match="Unrecognized type: 'unknown'"):
        TypeParserFactory.create_parser("unknown")

def test_type_parser_factory_aliases():
    assert isinstance(TypeParserFactory.create_parser("number"), NumericParser)
    assert isinstance(TypeParserFactory.create_parser("int"), IntegerParser)
    assert isinstance(TypeParserFactory.create_parser("character"), StringParser)
    assert isinstance(TypeParserFactory.create_parser("char"), StringParser)
    assert isinstance(TypeParserFactory.create_parser("bool"), BooleanParser)

def test_type_parser_factory_register_parser():
    class CustomParser(TypeParser):
        def parse(self, value: str) -> str:
            return "custom_" + value
    
    TypeParserFactory.register_parser("custom", CustomParser)
    parser = TypeParserFactory.create_parser("custom")
    assert isinstance(parser, CustomParser)
    assert parser.parse("test") == "custom_test"
    assert "custom" in TypeParserFactory.get_available_types()

def test_type_parser_factory_register_invalid_parser():
    class InvalidParser:
        pass
    with pytest.raises(ValueError, match="Parser class must inherit from TypeParser"):
        TypeParserFactory.register_parser("invalid", InvalidParser)  # type: ignore


# --- Concrete Parser Tests ---

# DateParser
def test_date_parser_default_format():
    parser = DateParser()
    assert parser.parse("2023-10-26") == date(2023, 10, 26)

def test_date_parser_custom_format():
    parser = DateParser(parameters={'format': '%d/%m/%Y'})
    assert parser.parse("26/10/2023") == date(2023, 10, 26)

def test_date_parser_invalid_input():
    parser = DateParser()
    with pytest.raises(TypeParseError, match="Failed to parse 'invalid-date' as date"):
        parser.parse("invalid-date")
    
    parser_custom = DateParser(parameters={'format': '%d/%m/%Y'})
    with pytest.raises(TypeParseError, match="Failed to parse '2023-10-26' as date"):
        parser_custom.parse("2023-10-26")

# DateTimeParser
def test_datetime_parser_default_format():
    parser = DateTimeParser()
    assert parser.parse("2023-10-26 15:30:00") == datetime(2023, 10, 26, 15, 30, 0)

def test_datetime_parser_custom_format():
    parser = DateTimeParser(parameters={'format': '%Y%m%d%H%M%S'})
    assert parser.parse("20231026153000") == datetime(2023, 10, 26, 15, 30, 0)

def test_datetime_parser_invalid_input():
    parser = DateTimeParser()
    with pytest.raises(TypeParseError, match="Failed to parse 'invalid-datetime' as datetime"):
        parser.parse("invalid-datetime")

# TimeParser
def test_time_parser_default_format():
    parser = TimeParser()
    assert parser.parse("15:30:00") == time(15, 30, 0)

def test_time_parser_custom_format_with_microseconds():
    parser = TimeParser(parameters={'format': '%H%M%S%f'})
    assert parser.parse("153000123456") == time(15, 30, 0, 123456)

def test_time_parser_invalid_input():
    parser = TimeParser()
    with pytest.raises(TypeParseError, match="Failed to parse 'invalid-time' as time"):
        parser.parse("invalid-time")

# NumericParser
def test_numeric_parser_basic():
    parser = NumericParser()
    assert parser.parse("123") == 123.0
    assert parser.parse("1.23") == 1.23

def test_numeric_parser_with_decimals():
    parser = NumericParser(parameters={'dec': 2})
    assert parser.parse("12345") == 123.45
    assert parser.parse("100") == 1.00

def test_numeric_parser_with_sign():
    parser = NumericParser(parameters={'sign': '-'})
    assert parser.parse("123") == -123.0
    assert parser.parse("1.23") == -1.23

def test_numeric_parser_with_decimals_and_sign():
    parser = NumericParser(parameters={'dec': 2, 'sign': '-'})
    assert parser.parse("12345") == -123.45

def test_numeric_parser_invalid_sign_param():
    parser = NumericParser(parameters={'sign': 'X'})
    with pytest.raises(TypeParseError, match="Invalid sign parameter"):
        parser.parse("100")

def test_numeric_parser_invalid_input():
    parser = NumericParser()
    with pytest.raises(TypeParseError, match="Failed to parse 'abc' as numeric"):
        parser.parse("abc")

# IntegerParser
def test_integer_parser_basic():
    parser = IntegerParser()
    assert parser.parse("123") == 123
    assert parser.parse("-45") == -45

def test_integer_parser_invalid_input():
    parser = IntegerParser()
    with pytest.raises(TypeParseError, match="Failed to parse '1.23' as integer"):
        parser.parse("1.23")
    with pytest.raises(TypeParseError, match="Failed to parse 'abc' as integer"):
        parser.parse("abc")

# StringParser
def test_string_parser_basic():
    parser = StringParser()
    assert parser.parse("hello world") == "hello world"
    assert parser.parse("123") == "123"

# BooleanParser
def test_boolean_parser_true_values():
    parser = BooleanParser()
    assert parser.parse("true") is True
    assert parser.parse("TRUE") is True
    assert parser.parse("t") is True
    assert parser.parse("yes") is True
    assert parser.parse("1") is True
    assert parser.parse("on") is True

def test_boolean_parser_false_values():
    parser = BooleanParser()
    assert parser.parse("false") is False
    assert parser.parse("FALSE") is False
    assert parser.parse("f") is False
    assert parser.parse("no") is False
    assert parser.parse("0") is False
    assert parser.parse("off") is False

def test_boolean_parser_invalid_input():
    parser = BooleanParser()
    with pytest.raises(TypeParseError, match="Failed to parse 'maybe' as boolean"):
        parser.parse("maybe")
    with pytest.raises(TypeParseError, match="Failed to parse '2' as boolean"):
        parser.parse("2")

def test_type_parser_get_type_name():
    assert DateParser().get_type_name() == 'date'
    assert NumericParser().get_type_name() == 'numeric'
    assert StringParser().get_type_name() == 'string'
