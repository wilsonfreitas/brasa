"""
Fieldset class for grouping and managing Field instances.
"""

from collections.abc import Iterator

from .field import Field


class Fieldset:
    """
    A container for grouping related Field instances.

    Fieldset provides structured organization of fields, offering methods
    for adding and retrieving fields in insertion order.

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
            ValueError: If field is not a Field instance

        Example:
            field = Field("email", "Email address", "string")
            fieldset.add_field(field)
        """
        if not isinstance(field, Field):
            raise ValueError(f"Expected Field instance, got {type(field).__name__}")

        self._fields[field.name] = field

    def add_fields(self, *fields: Field) -> None:
        """
        Add multiple fields to the fieldset at once.

        Args:
            *fields: Variable number of Field instances

        Raises:
            ValueError: If any argument is not a Field instance

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
            ValueError: If no field with the given name exists

        Example:
            email_field = fieldset.get_field("email")
        """
        if name not in self._fields:
            raise ValueError(
                f"Field '{name}' not found in fieldset. "
                f"Available fields: {list(self._fields.keys())}"
            )

        return self._fields[name]

    def get_all_fields(self) -> list[Field]:
        """
        Get all fields in the fieldset.

        Fields are returned in the order they were added.

        Returns:
            List of all Field instances in insertion order
        """
        return list(self._fields.values())

    def get_field_names(self) -> list[str]:
        """
        Get names of all fields in the fieldset.

        Returns:
            List of field names in insertion order
        """
        return list(self._fields.keys())

    @property
    def names(self) -> list[str]:
        """
        Compatibility property for legacy code that uses .names attribute.

        Returns:
            List of field names in insertion order
        """
        return self.get_field_names()

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
            ValueError: If field not found
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
