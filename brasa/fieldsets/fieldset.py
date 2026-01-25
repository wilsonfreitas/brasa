"""
Fieldset class for grouping and managing Field instances.
"""

from collections.abc import Iterator
from typing import Any

import yaml

from .exceptions import FieldsetError
from .field import Field


class Fieldset:
    """
    A container for grouping related Field instances.

    Fieldset provides structured organization of fields with parsing capabilities,
    offering methods for adding, retrieving, managing fields, and parsing data records.

    Attributes:
        name: Optional name for the fieldset
        description: Optional description of the fieldset's purpose
    """

    def __init__(self, name: str | None = None, description: str | None = None):
        """
        Initialize a Fieldset instance.

        Args:
            name: Optional name for the fieldset
            description: Optional description of the fieldset

        Example:
            fieldset = Fieldset(
                name="customer_info",
                description="Customer information fields"
            )
        """
        self._name = name
        self._description = description
        self._fields: dict[str, Field] = {}
        self._field_order: list[str] = []  # Maintain insertion order

    @property
    def name(self) -> str | None:
        """Get the fieldset's name."""
        return self._name

    @name.setter
    def name(self, value: str | None) -> None:
        """Set the fieldset's name."""
        self._name = value

    @property
    def description(self) -> str | None:
        """Get the fieldset's description."""
        return self._description

    @description.setter
    def description(self, value: str | None) -> None:
        """Set the fieldset's description."""
        self._description = value

    def add_field(self, field: Field) -> None:
        """
        Add a field to the fieldset.

        If a field with the same name already exists, it will be replaced
        but the original insertion order position will be maintained.

        Args:
            field: The Field instance to add

        Raises:
            FieldsetError: If field is not a Field instance

        Example:
            field = Field("email", "Email address", "string")
            fieldset.add_field(field)
        """
        if not isinstance(field, Field):
            raise FieldsetError(f"Expected Field instance, got {type(field).__name__}")

        # Add field to dictionary
        if field.name not in self._fields:
            # New field - add to order list
            self._field_order.append(field.name)

        self._fields[field.name] = field

    def add_fields(self, *fields: Field) -> None:
        """
        Add multiple fields to the fieldset at once.

        Args:
            *fields: Variable number of Field instances

        Raises:
            FieldsetError: If any argument is not a Field instance

        Example:
            fieldset.add_fields(field1, field2, field3)
        """
        for field in fields:
            self.add_field(field)

    def get_field(self, name: str) -> Field:
        """
        Retrieve a field by name.

        Args:
            name: The name of the field to retrieve

        Returns:
            The Field instance with the specified name

        Raises:
            FieldsetError: If no field with the given name exists

        Example:
            email_field = fieldset.get_field("email")
        """
        if name not in self._fields:
            raise FieldsetError(
                f"Field '{name}' not found in fieldset. "
                f"Available fields: {list(self._fields.keys())}"
            )

        return self._fields[name]

    def has_field(self, name: str) -> bool:
        """
        Check if a field with the given name exists.

        Args:
            name: The field name to check

        Returns:
            True if the field exists, False otherwise
        """
        return name in self._fields

    def remove_field(self, name: str) -> Field:
        """
        Remove a field from the fieldset.

        Args:
            name: The name of the field to remove

        Returns:
            The removed Field instance

        Raises:
            FieldsetError: If no field with the given name exists
        """
        if name not in self._fields:
            raise FieldsetError(f"Field '{name}' not found in fieldset")

        field = self._fields.pop(name)
        self._field_order.remove(name)
        return field

    def get_all_fields(self) -> list[Field]:
        """
        Get all fields in the fieldset.

        Fields are returned in the order they were added.

        Returns:
            List of all Field instances in insertion order
        """
        return [self._fields[name] for name in self._field_order]

    def get_field_names(self) -> list[str]:
        """
        Get names of all fields in the fieldset.

        Returns:
            List of field names in insertion order
        """
        return self._field_order.copy()

    @property
    def names(self) -> list[str]:
        """
        Compatibility property for legacy code that uses .names attribute.

        Returns:
            List of field names in insertion order
        """
        return self.get_field_names()

    def get_fields_by_type(self, type_name: str) -> list[Field]:
        """
        Get all fields of a specific type.

        Args:
            type_name: The base type name to filter by (e.g., 'date', 'numeric')

        Returns:
            List of Field instances matching the specified type

        Example:
            date_fields = fieldset.get_fields_by_type("date")
        """
        return [
            field
            for field in self.get_all_fields()
            if field.type_name == type_name.lower()
        ]

    def parse_record(self, record: dict[str, str]) -> dict[str, Any]:
        """
        Parse a record (dictionary of strings) using field parsers.

        Args:
            record: Dictionary mapping field names to string values

        Returns:
            Dictionary mapping field names to parsed values

        Raises:
            TypeParseError: If any field fails to parse
            FieldsetError: If record contains unknown fields

        Example:
            record = {"birth_date": "25/12/1990", "age": "33"}
            parsed = fieldset.parse_record(record)
            # Returns: {"birth_date": date(1990, 12, 25), "age": 33}
        """
        parsed_record = {}

        for field_name, value in record.items():
            if not self.has_field(field_name):
                raise FieldsetError(
                    f"Unknown field '{field_name}' in record. "
                    f"Available fields: {self.get_field_names()}"
                )

            field = self.get_field(field_name)
            parsed_record[field_name] = field.parse(value)

        return parsed_record

    def validate_record(self, record: dict[str, str]) -> dict[str, bool]:
        """
        Validate all fields in a record without raising exceptions.

        Args:
            record: Dictionary mapping field names to string values

        Returns:
            Dictionary mapping field names to validation results (True/False)
        """
        validation_results = {}

        for field_name, value in record.items():
            if not self.has_field(field_name):
                validation_results[field_name] = False
            else:
                field = self.get_field(field_name)
                validation_results[field_name] = field.validate(value)

        return validation_results

    def clear(self) -> None:
        """
        Remove all fields from the fieldset.
        """
        self._fields.clear()
        self._field_order.clear()

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the fieldset to a dictionary representation.

        Returns:
            Dictionary containing fieldset information and all fields
        """
        return {
            "name": self.name,
            "description": self.description,
            "fields": [field.to_dict() for field in self.get_all_fields()],
        }

    def to_yaml(self) -> str:
        """
        Export the fieldset to YAML format.

        Returns:
            YAML string representation of the fieldset
        """
        data = self.to_dict()
        return yaml.dump(data, default_flow_style=False, allow_unicode=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Fieldset":
        """
        Create a Fieldset instance from a dictionary.

        Args:
            data: Dictionary with optional 'name', 'description', and 'fields' list

        Returns:
            Fieldset instance

        Example:
            data = {
                'name': 'customer_schema',
                'description': 'Customer data fields',
                'fields': [
                    {'name': 'id', 'description': 'ID', 'type': 'integer'},
                    {'name': 'birth_date', 'description': 'Birth date', 'type': 'date'}
                ]
            }
            fieldset = Fieldset.from_dict(data)
        """
        name = data.get("name")
        description = data.get("description")

        fieldset = cls(name=name, description=description)

        if "fields" in data:
            if not isinstance(data["fields"], list):
                raise FieldsetError("Fieldset 'fields' must be a list.")
            for field_data in data["fields"]:
                if not isinstance(field_data, dict):
                    raise FieldsetError(
                        "Each field in 'fields' list must be a dictionary."
                    )
                field = Field.from_dict(field_data)
                fieldset.add_field(field)

        return fieldset

    @classmethod
    def from_yaml(cls, yaml_content: str | dict[str, Any]) -> "Fieldset":
        """
        Create a Fieldset instance from YAML content.

        Args:
            yaml_content: YAML string or pre-parsed dictionary

        Returns:
            Fieldset instance

        Raises:
            FieldsetError: If YAML content is invalid or malformed

        Example:
            yaml_str = '''
            fields:
              - name: refdate
                description: Data de referência
                type: date(format="%d/%m/%Y")
              - name: curve_name
                description: Nome da curva
                type: character
            '''
            fieldset = Fieldset.from_yaml(yaml_str)
        """
        if isinstance(yaml_content, str):
            try:
                data = yaml.safe_load(yaml_content)
            except yaml.YAMLError as e:
                raise FieldsetError(f"Invalid YAML content: {e}") from e
        else:
            data = yaml_content

        if not isinstance(data, dict):
            raise FieldsetError(
                "YAML content must represent a dictionary for the fieldset."
            )

        return cls.from_dict(data)

    @classmethod
    def from_template_fields(
        cls, template_fields: Any, raw_fields: list | None = None
    ) -> "Fieldset":
        """
        Create a Fieldset instance from engine.TemplateFields.

        Since TemplateFields may not properly initialize handlers from 'type' field,
        this method can optionally accept the raw fields list from the template YAML
        to extract type information directly.

        Maps template field types to fieldsets type specifications:
        - 'date' -> 'date'
        - 'character' -> 'character'
        - 'integer' -> 'integer'
        - 'numeric' -> 'numeric'

        Args:
            template_fields: TemplateFields instance from engine module
            raw_fields: Optional raw fields list from template (for direct type extraction)

        Returns:
            Fieldset instance with fields configured from template

        Example:
            from brasa.engine import retrieve_template
            template = retrieve_template('b3-bvbg086')
            # Use raw fields from template for proper type detection
            fieldset = Fieldset.from_template_fields(
                template.fields,
                raw_fields=template.template.get('fields')
            )
        """
        fieldset = cls()

        # Create a lookup for raw field types if provided
        raw_type_map = {}
        if raw_fields:
            for raw_field in raw_fields:
                # Try to get type from 'type' field first, then from 'handler.type'
                raw_type = raw_field.get("type")
                if not raw_type and "handler" in raw_field and raw_field["handler"]:
                    raw_type = raw_field["handler"].get("type", "character")
                if not raw_type:
                    raw_type = "character"

                # Normalize type name (case-insensitive)
                # raw_type = raw_type.lower()

                # Map template types to fieldsets types
                if raw_type == "character":
                    raw_type = "string"
                elif raw_type == "posixct":
                    raw_type = "date"

                raw_type_map[raw_field["name"]] = raw_type

        # TemplateFields has a names attribute and __getitem__ for access
        for field_name in template_fields.names:
            # Skip fields with None/null names (placeholder templates)
            if field_name is None:
                continue

            template_field = template_fields[field_name]

            # First try to get type from raw fields if available
            if field_name in raw_type_map:
                field_type = raw_type_map[field_name]
            else:
                # Fall back to handler class inspection
                handler_class = template_field.handler.__class__.__name__

                # Map handler class names to fieldsets types
                type_map = {
                    "DateFieldHandler": "date",
                    "NumericFieldHandler": "numeric",
                    "CharacterFieldHandler": "string",
                    "FieldHandler": "string",  # default handler
                }

                field_type = type_map.get(handler_class, "string")

            # Create Field instance
            field = Field(
                name=template_field.name,
                description=template_field.description or template_field.name,
                type_definition=field_type,
            )

            fieldset.add_field(field)

        return fieldset

    def __len__(self) -> int:
        """
        Get the number of fields in the fieldset.

        Returns:
            Number of fields
        """
        return len(self._fields)

    def __contains__(self, name: str) -> bool:
        """
        Check if a field exists using 'in' operator.

        Args:
            name: Field name to check

        Returns:
            True if field exists, False otherwise
        """
        return name in self._fields

    def __iter__(self) -> Iterator[Field]:
        """
        Iterate over fields in insertion order.

        Returns:
            Iterator over Field instances
        """
        return iter(self.get_all_fields())

    def __getitem__(self, name: str) -> Field:
        """
        Get a field using bracket notation.

        Args:
            name: Field name

        Returns:
            Field instance

        Raises:
            FieldsetError: If field not found
        """
        return self.get_field(name)

    def __repr__(self) -> str:
        """
        Return a string representation of the fieldset.

        Returns:
            String representation
        """
        name_part = f"name='{self.name}', " if self.name else ""
        return f"Fieldset({name_part}{len(self)} fields)"

    def __str__(self) -> str:
        """
        Return a human-readable string representation.

        Returns:
            Formatted string with fieldset details
        """
        header = f"Fieldset: {self.name}\n" if self.name else "Fieldset\n"
        if self.description:
            header += f"Description: {self.description}\n"

        header += f"Fields ({len(self)}):\n"

        if not self._fields:
            return header + "  (empty)"

        field_lines = [f"  - {field}" for field in self.get_all_fields()]
        return header + "\n".join(field_lines)
