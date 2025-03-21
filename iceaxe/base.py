from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Protocol,
    Self,
    Type,
    dataclass_transform,
)

from pydantic import BaseModel, Field as PydanticField
from pydantic.main import _model_construction
from pydantic_core import PydanticUndefined

from iceaxe.field import DBFieldClassDefinition, DBFieldInfo, Field


# Define a protocol for models that have an ID field
class ModelWithID(Protocol):
    """Protocol for models that have an ID field."""

    id: Any


@dataclass_transform(kw_only_default=True, field_specifiers=(PydanticField,))
class DBModelMetaclass(_model_construction.ModelMetaclass):
    """
    Metaclass for database model classes that provides automatic field tracking and SQL query generation.
    Extends Pydantic's model metaclass to add database-specific functionality.

    This metaclass provides:
    - Automatic field to SQL column mapping
    - Dynamic field access that returns query-compatible field definitions
    - Registry tracking of all database model classes
    - Support for generic model instantiation

    ```python {{sticky: True}}
    class User(TableBase):  # Uses DBModelMetaclass
        id: int = Field(primary_key=True)
        name: str
        email: str | None

    # Fields can be accessed for queries
    User.id      # Returns DBFieldClassDefinition
    User.name    # Returns DBFieldClassDefinition

    # Metaclass handles model registration
    registered_models = DBModelMetaclass.get_registry()
    ```
    """

    _registry: list[Type["TableBase"]] = []
    _cached_args: dict[Type["TableBase"], dict[str, Any]] = {}
    is_constructing: bool = False

    def __new__(mcs, name, bases, namespace, **kwargs):
        """
        Create a new database model class with proper field tracking.
        Handles registration of the model and processes any table-specific arguments.
        """
        raw_kwargs = {**kwargs}

        mcs.is_constructing = True
        autodetect = mcs._extract_kwarg(kwargs, "autodetect", True)
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        mcs.is_constructing = False

        # Allow future calls to subclasses / generic instantiations to reference the same
        # kwargs as the base class
        mcs._cached_args[cls] = raw_kwargs

        # If we have already set the class's fields, we should wrap them
        if hasattr(cls, "__pydantic_fields__"):
            cls.__pydantic_fields__ = {
                field: info
                if isinstance(info, DBFieldInfo)
                else DBFieldInfo.extend_field(
                    info,
                    primary_key=False,
                    postgres_config=None,
                    foreign_key=None,
                    unique=False,
                    index=False,
                    check_expression=None,
                    is_json=False,
                )
                for field, info in cls.model_fields.items()
            }

        # Avoid registering HandlerBase itself
        if cls.__name__ not in {"TableBase", "BaseModel"} and autodetect:
            DBModelMetaclass._registry.append(cls)

        return cls

    def __getattr__(self, key: str) -> Any:
        """
        Provides dynamic access to model fields as query-compatible definitions.
        When accessing an undefined attribute, checks if it's a model field and returns
        a DBFieldClassDefinition if it is.

        :param key: The attribute name to access
        :return: Field definition or raises AttributeError
        :raises AttributeError: If the attribute doesn't exist and isn't a model field
        """
        if self.is_constructing:
            return super().__getattr__(key)  # type: ignore

        try:
            return super().__getattr__(key)  # type: ignore
        except AttributeError:
            # Determine if this field is defined within the spec
            # If so, return it
            if key in self.model_fields:
                return DBFieldClassDefinition(
                    root_model=self,  # type: ignore
                    key=key,
                    field_definition=self.model_fields[key],
                )
            raise

    @classmethod
    def get_registry(cls) -> list[Type["TableBase"]]:
        """
        Get the set of all registered database model classes.

        :return: Set of registered TableBase classes
        """
        return cls._registry

    @classmethod
    def _extract_kwarg(
        cls, kwargs: dict[str, Any], key: str, default: Any = None
    ) -> Any:
        """
        Extract a keyword argument from either standard kwargs or pydantic generic metadata.
        Handles both normal instantiation and pydantic's generic model instantiation.

        :param kwargs: Dictionary of keyword arguments
        :param key: Key to extract
        :param default: Default value if key not found
        :return: Extracted value or default
        """
        if key in kwargs:
            return kwargs.pop(key)

        if "__pydantic_generic_metadata__" in kwargs:
            origin_model = kwargs["__pydantic_generic_metadata__"]["origin"]
            if origin_model in cls._cached_args:
                return cls._cached_args[origin_model].get(key, default)

        return default

    @property
    def model_fields(self) -> dict[str, DBFieldInfo]:  # type: ignore
        """
        Get the dictionary of model fields and their definitions.
        Overrides the ClassVar typehint from TableBase for proper typing.

        :return: Dictionary of field names to field definitions
        """
        return super().model_fields  # type: ignore


class UniqueConstraint(BaseModel):
    """
    Represents a UNIQUE constraint in a database table.
    Ensures that the specified combination of columns contains unique values across all rows.

    ```python {{sticky: True}}
    class User(TableBase):
        email: str
        tenant_id: int

        table_args = [
            UniqueConstraint(columns=["email", "tenant_id"])
        ]
    ```
    """

    columns: list[str]
    """
    List of column names that should have unique values
    """


class IndexConstraint(BaseModel):
    """
    Represents an INDEX on one or more columns in a database table.
    Improves query performance for the specified columns.

    ```python {{sticky: True}}
    class User(TableBase):
        email: str
        last_login: datetime

        table_args = [
            IndexConstraint(columns=["last_login"])
        ]
    ```
    """

    columns: list[str]
    """
    List of column names to create an index on
    """


INTERNAL_TABLE_FIELDS = ["modified_attrs", "modified_attrs_callbacks"]


class TableBase(BaseModel, metaclass=DBModelMetaclass):
    """
    Base class for all database table models.
    Provides the foundation for defining database tables using Python classes with
    type hints and field definitions.

    Features:
    - Automatic table name generation from class name
    - Support for custom table names
    - Tracking of modified fields for efficient updates
    - Support for unique constraints and indexes
    - Integration with Pydantic for validation

    ```python {{sticky: True}}
    class User(TableBase):
        # Custom table name (optional)
        table_name = "users"

        # Fields with types and constraints
        id: int = Field(primary_key=True)
        email: str = Field(unique=True)
        name: str
        is_active: bool = Field(default=True)

        # Table-level constraints
        table_args = [
            UniqueConstraint(columns=["email"]),
            IndexConstraint(columns=["name"])
        ]

    # Usage in queries
    query = select(User).where(User.is_active == True)
    users = await conn.execute(query)
    ```
    """

    if TYPE_CHECKING:
        model_fields: ClassVar[dict[str, DBFieldInfo]]  # type: ignore

    table_name: ClassVar[str] = PydanticUndefined  # type: ignore
    """
    Optional custom name for the table
    """

    table_args: ClassVar[list[UniqueConstraint | IndexConstraint]] = PydanticUndefined  # type: ignore
    """
    Table constraints and indexes
    """

    # Private methods
    modified_attrs: dict[str, Any] = Field(default_factory=dict, exclude=True)
    """
    Dictionary of modified field values since instantiation or the last clear_modified_attributes() call.
    Used to construct differential update queries.
    """

    modified_attrs_callbacks: list[Callable[[Self], None]] = Field(
        default_factory=list, exclude=True
    )
    """
    List of callbacks to be called when the model is modified.
    """

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Track modified attributes when fields are updated.
        This allows for efficient database updates by only updating changed fields.

        :param name: Attribute name
        :param value: New value
        """
        if name in self.model_fields:
            self.modified_attrs[name] = value
            for callback in self.modified_attrs_callbacks:
                callback(self)
        super().__setattr__(name, value)

    def get_modified_attributes(self) -> dict[str, Any]:
        """
        Get the dictionary of attributes that have been modified since instantiation
        or the last clear_modified_attributes() call.

        :return: Dictionary of modified attribute names and their values
        """
        return self.modified_attrs

    def clear_modified_attributes(self) -> None:
        """
        Clear the tracking of modified attributes.
        Typically called after successfully saving changes to the database.
        """
        self.modified_attrs.clear()

    @classmethod
    def get_table_name(cls) -> str:
        """
        Get the table name for this model.
        Uses the custom table_name if set, otherwise converts the class name to lowercase.

        :return: Table name to use in SQL queries
        """
        if cls.table_name == PydanticUndefined:
            return cls.__name__.lower()
        return cls.table_name

    @classmethod
    def get_client_fields(cls) -> dict[str, DBFieldInfo]:
        """
        Get all fields that should be exposed to clients.
        Excludes internal fields used for model functionality.

        :return: Dictionary of field names to field definitions
        """
        return {
            field: info
            for field, info in cls.model_fields.items()
            if field not in INTERNAL_TABLE_FIELDS
        }

    def register_modified_callback(self, callback: Callable[[Self], None]) -> None:
        """
        Register a callback to be called when the model is modified.
        """
        self.modified_attrs_callbacks.append(callback)

    def __eq__(self, other: Any) -> bool:
        """
        Compare two model instances, ignoring modified_attrs_callbacks.
        This ensures that two models with the same data but different callbacks are considered equal.
        """
        if not isinstance(other, self.__class__):
            return False

        # Get all fields except modified_attrs_callbacks
        fields = {
            field: value
            for field, value in self.__dict__.items()
            if field not in INTERNAL_TABLE_FIELDS
        }
        other_fields = {
            field: value
            for field, value in other.__dict__.items()
            if field not in INTERNAL_TABLE_FIELDS
        }

        return fields == other_fields


class PolymorphicBase(TableBase):
    """
    Base class for polymorphic models that use single-table inheritance.

    This class provides the infrastructure for polymorphic model loading, where different
    model subclasses share a single database table, and the appropriate subclass is chosen
    based on a discriminator column value.

    Example:
    ```python
    class Animal(PolymorphicBase):
        id: int = Field(primary_key=True)
        type: str = Field(discriminator_type=True)
        name: str

    class Dog(Animal):
        breed: str
        bark_volume: int

    class Cat(Animal):
        fur_color: str
        lives_left: int

    # When querying, the right subclass will be instantiated based on the 'type' column value
    animals = await conn.exec(select(Animal))
    # Each animal in the result will be either a Dog or Cat instance
    ```
    """

    # Dictionary to store the mapping from discriminator value to subclass
    __poly_registry: ClassVar[Dict[str, Type["PolymorphicBase"]]] = {}
    __discriminator_field: ClassVar[str | None] = None

    @classmethod
    def get_table_name(cls) -> str:
        """
        Get the table name for this polymorphic model.

        For polymorphic models, all subclasses share the same table, so we need to use
        the base class's table name for all subclasses.

        :return: The table name of the base polymorphic class
        """
        # Get the immediate polymorphic base class (first in MRO that is a PolymorphicBase)
        base_class = None
        for base in cls.__mro__[1:]:  # Skip self
            if issubclass(base, PolymorphicBase) and base is not PolymorphicBase:
                base_class = base
                break

        # If this is a subclass, use the base class's table name
        if base_class is not None:
            return base_class.get_table_name()

        # Otherwise, use the normal table name logic
        return super().get_table_name()

    @classmethod
    def __init_subclass__(cls, **kwargs):
        """
        Register the subclass in the polymorphic registry.
        This is automatically called when a class inherits from PolymorphicBase.
        """
        super().__init_subclass__(**kwargs)

        # Skip abstract base classes or classes with no fields
        if not hasattr(cls, "model_fields") or not cls.model_fields:
            return

        # Find the base polymorphic class for this hierarchy
        base_poly_class = cls
        for base in cls.__mro__[1:]:  # Skip self
            if issubclass(base, PolymorphicBase) and base is not PolymorphicBase:
                base_poly_class = base
                break
            elif base is PolymorphicBase:
                # Reached PolymorphicBase itself, stop
                break

        # Find the discriminator field if it exists
        discriminator_field = None
        discriminator_value = cls.__name__

        # Look for a discriminator field in the model fields
        for field_name, field_info in cls.model_fields.items():
            if (
                hasattr(field_info, "discriminator_type")
                and field_info.discriminator_type
            ):
                discriminator_field = field_name
                # If the field has a default value on this class, use that as the discriminator
                if (
                    hasattr(field_info, "default")
                    and field_info.default is not None
                    and field_info.default != PydanticUndefined
                ):
                    discriminator_value = field_info.default
                break

        # Find a discriminator field from the base class if not in this class
        if discriminator_field is None and base_poly_class != cls:
            base_discriminator = base_poly_class.get_discriminator_field()
            if base_discriminator:
                discriminator_field = base_discriminator
                # Try to find a default for this field in this class
                if discriminator_field in cls.model_fields:
                    field_info = cls.model_fields[discriminator_field]
                    if (
                        hasattr(field_info, "default")
                        and field_info.default is not None
                        and field_info.default != PydanticUndefined
                    ):
                        discriminator_value = field_info.default

        # Store the discriminator field name in the base polymorphic class
        if (
            discriminator_field
            and not hasattr(base_poly_class, "__discriminator_field")
            or base_poly_class.__discriminator_field is None
        ):
            base_poly_class.__discriminator_field = discriminator_field

        # Register this class with its discriminator value if we have a discriminator field
        if discriminator_field is not None and cls is not base_poly_class:
            # Register with class name
            base_poly_class.__poly_registry[cls.__name__] = cls

            # Register with the string representation of the discriminator value
            str_value = str(discriminator_value)
            if str_value != cls.__name__:
                base_poly_class.__poly_registry[str_value] = cls

    @classmethod
    def get_subclass_for_discriminator(
        cls, discriminator_value: Any
    ) -> Type["PolymorphicBase"]:
        """
        Get the appropriate subclass for a given discriminator value.

        :param discriminator_value: The value of the discriminator field
        :return: The polymorphic subclass to instantiate
        """
        # Get the base polymorphic class for this hierarchy
        base_poly_class = cls
        for base in cls.__mro__:
            if issubclass(base, PolymorphicBase) and base is not PolymorphicBase:
                base_poly_class = base
                break
            elif base is PolymorphicBase:
                # Reached PolymorphicBase itself, stop
                break

        # Convert to string to handle enum types consistently
        str_value = str(discriminator_value)

        # Try the direct string value first
        if str_value in base_poly_class.__poly_registry:
            subclass = base_poly_class.__poly_registry[str_value]
            return subclass

        # If not found, try checking if we need to look up based on case-insensitive matching
        # This covers cases where enum values might be stored differently than class names
        for reg_value, reg_class in base_poly_class.__poly_registry.items():
            if str_value.lower() == reg_value.lower():
                return reg_class

        # If not found, try to match the first letter capitalized (for enum values to class names)
        capitalized_value = str_value.capitalize()
        if capitalized_value in base_poly_class.__poly_registry:
            subclass = base_poly_class.__poly_registry[capitalized_value]
            return subclass

        # If no exact match, return the class we were called on
        return cls

    @classmethod
    def get_discriminator_field(cls) -> str | None:
        """
        Get the name of the discriminator field for this polymorphic model.

        :return: Name of the discriminator field or None if not set
        """
        return cls.__discriminator_field

    @classmethod
    def get_all_subclasses(cls) -> list[Type["PolymorphicBase"]]:
        """
        Get all registered polymorphic subclasses.

        :return: List of all polymorphic subclass types
        """
        return list(cls.__poly_registry.values())
