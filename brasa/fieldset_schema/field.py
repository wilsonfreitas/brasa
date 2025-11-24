"""
Field class for defining data field metadata and parsing capabilities.
"""

from typing import Any, Dict

from .exceptions import FieldError, TypeParseError
from .type_parser import TypeParser, TypeParserFactory


class Field:
    """
    Represents a data field with metadata and type parsing capability.
    
    A Field combines metadata (name, description) with an actual type parser
    that can validate and transform string values into proper Python types.
    
    Attributes:
        name: The unique identifier for the field
        description: A human-readable description of the field's purpose
        type_definition: The original type definition string
        parser: The TypeParser instance for this field
    """
    
    def __init__(
        self,
        name: str,
        description: str,
        type_definition: str,
        **kwargs: Any
    ):
        """
        Initialize a Field instance.
        
        Args:
            name: The field's name (must be non-empty string)
            description: A description of the field
            type_definition: Type definition string (e.g., "date(format='%Y-%m-%d')")
            **kwargs: Additional arbitrary attributes to set on the field
            
        Raises:
            FieldError: If name is empty or not a string
            FieldError: If type_definition is invalid
            
        Example:
            field = Field(
                name="birth_date",
                description="Customer's date of birth",
                type_definition="date(format='%Y-%m-%d')",
                required=True
            )
        """
        if not isinstance(name, str) or not name.strip():
            raise FieldError("Field name must be a non-empty string")
        
        self._name = name.strip()
        self._description = description if isinstance(description, str) else str(description)
        self._type_definition = type_definition.strip()
        
        # Create parser from type definition
        try:
            self._parser = TypeParserFactory.create_parser(self._type_definition)
        except Exception as e:
            raise FieldError(f"Invalid type definition for field '{self._name}': {e}")
        
        # Store additional attributes
        self._extra_attributes: Dict[str, Any] = {}
        for key, value in kwargs.items():
            self.set_attribute(key, value)
    
    @property
    def name(self) -> str:
        """Get the field's name."""
        return self._name
    
    @property
    def description(self) -> str:
        """Get the field's description."""
        return self._description
    
    @description.setter
    def description(self, value: str) -> None:
        """Set the field's description."""
        self._description = value if isinstance(value, str) else str(value)
    
    @property
    def type_definition(self) -> str:
        """Get the original type definition string."""
        return self._type_definition
    
    @property
    def type_name(self) -> str:
        """Get the base type name without parameters."""
        return self._parser.get_type_name()
    
    @property
    def parser(self) -> TypeParser:
        """Get the type parser instance."""
        return self._parser
    
    def parse(self, value: str) -> Any:
        """
        Parse a string value using the field's type parser.
        
        Args:
            value: String value to parse
            
        Returns:
            Parsed value in the appropriate Python type
            
        Raises:
            TypeParseError: If parsing fails
            
        Example:
            field = Field("birth_date", "Date of birth", "date(format='%d/%m/%Y')")
            parsed_date = field.parse("25/12/1990")
            # Returns: datetime.date(1990, 12, 25)
        """
        try:
            return self._parser.parse(value)
        except TypeParseError as e:
            raise TypeParseError(f"Error parsing field '{self.name}': {e}")
    
    def validate(self, value: str) -> bool:
        """
        Check if a value can be parsed without raising an exception.
        
        Args:
            value: String value to validate
            
        Returns:
            True if value can be parsed, False otherwise
        """
        try:
            self.parse(value)
            return True
        except TypeParseError:
            return False
    
    def set_attribute(self, key: str, value: Any) -> None:
        r"""
        Set a custom attribute on the field.
        
        This method allows adding arbitrary metadata to the field beyond
        the core attributes (name, description, type_definition).
        
        Args:
            key: The attribute name
            value: The attribute value
            
        Raises:
            FieldError: If key is empty or not a string
            FieldError: If key conflicts with protected attribute names
            
        Example:
            field.set_attribute("max_length", 100)
            field.set_attribute("validation_regex", r"^\d{3}-\d{2}-\d{4}$")
        """
        if not isinstance(key, str) or not key.strip():
            raise FieldError("Attribute key must be a non-empty string")
        
        key = key.strip()
        
        # Prevent overwriting protected attributes
        protected_attrs = {'_name', '_description', '_type_definition', '_parser', '_extra_attributes'}
        if key in protected_attrs or key.startswith('_'):
            raise FieldError(
                f"Cannot set attribute '{key}': conflicts with protected attribute"
            )
        
        self._extra_attributes[key] = value
    
    def get_attribute(self, key: str, default: Any = None) -> Any:
        """
        Get a custom attribute from the field.
        
        Args:
            key: The attribute name to retrieve
            default: Value to return if attribute doesn't exist
            
        Returns:
            The attribute value, or default if not found
            
        Example:
            max_length = field.get_attribute("max_length", 255)
        """
        return self._extra_attributes.get(key, default)
    
    def has_attribute(self, key: str) -> bool:
        """
        Check if a custom attribute exists.
        
        Args:
            key: The attribute name to check
            
        Returns:
            True if the attribute exists, False otherwise
        """
        return key in self._extra_attributes
    
    def remove_attribute(self, key: str) -> None:
        """
        Remove a custom attribute from the field.
        
        Args:
            key: The attribute name to remove
            
        Raises:
            FieldError: If the attribute doesn't exist
        """
        if key not in self._extra_attributes:
            raise FieldError(f"Attribute '{key}' does not exist on field '{self.name}'")
        
        del self._extra_attributes[key]
    
    def get_all_attributes(self) -> Dict[str, Any]:
        """
        Get all custom attributes as a dictionary.
        
        Returns:
            Dictionary of all extra attributes
        """
        return self._extra_attributes.copy()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the field to a dictionary representation.
        
        This includes core attributes and all custom attributes.
        
        Returns:
            Dictionary containing all field information
        """
        result = {
            'name': self.name,
            'description': self.description,
            'type': self.type_definition,
        }
        result.update(self._extra_attributes)
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Field':
        """
        Create a Field instance from a dictionary.
        
        Args:
            data: Dictionary with 'name', 'description', 'type', and optional attributes
            
        Returns:
            Field instance
            
        Raises:
            FieldError: If required keys are missing
        """
        required_keys = {'name', 'description', 'type'}
        missing_keys = required_keys - set(data.keys())
        
        if missing_keys:
            raise FieldError(f"Missing required keys for Field: {missing_keys}")
        
        # Extract core attributes
        name = data['name']
        description = data['description']
        type_definition = data['type']
        
        # Extract extra attributes
        extra_attrs = {k: v for k, v in data.items() if k not in required_keys}
        
        return cls(name, description, type_definition, **extra_attrs)
    
    def __repr__(self) -> str:
        """
        Return a string representation of the field.
        
        Returns:
            String representation showing name and type
        """
        extra_count = len(self._extra_attributes)
        extra_info = f", {extra_count} custom attrs" if extra_count > 0 else ""
        return f"Field(name='{self.name}', type='{self.type_definition}'{extra_info})"
    
    def __str__(self) -> str:
        """
        Return a human-readable string representation.
        
        Returns:
            Formatted string with field details
        """
        return f"{self.name} ({self.type_definition}): {self.description}"
    
    def __eq__(self, other: object) -> bool:
        """
        Check equality based on name.
        
        Args:
            other: Object to compare with
            
        Returns:
            True if names are equal, False otherwise
        """
        if not isinstance(other, Field):
            return False
        return self.name == other.name
