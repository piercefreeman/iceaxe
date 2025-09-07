from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Self,
    Type,
    dataclass_transform,
)

from pydantic import BaseModel, Field as PydanticField
from pydantic.main import _model_construction
from pydantic_core import PydanticUndefined

from iceaxe.field import DBFieldClassDefinition, DBFieldInfo, Field


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
                    explicit_type=None,
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
        return getattr(self, "__pydantic_fields__", {})  # type: ignore


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
        if name in self.__class__.model_fields:
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
