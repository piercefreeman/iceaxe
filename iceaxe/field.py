from json import dumps as json_dumps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Concatenate,
    Generic,
    ParamSpec,
    Type,
    TypeVar,
    Unpack,
    cast,
)

from pydantic import Field as PydanticField
from pydantic.fields import FieldInfo, _FieldInfoInputs
from pydantic_core import PydanticUndefined

from iceaxe.comparison import ComparisonBase
from iceaxe.postgres import PostgresFieldBase
from iceaxe.queries_str import QueryIdentifier, QueryLiteral
from iceaxe.sql_types import ColumnType

if TYPE_CHECKING:
    from iceaxe.base import TableBase

P = ParamSpec("P")

_Unset: Any = PydanticUndefined


class DBFieldInputs(_FieldInfoInputs, total=False):
    primary_key: bool
    autoincrement: bool
    postgres_config: PostgresFieldBase | None
    foreign_key: str | None
    unique: bool
    index: bool
    check_expression: str | None
    is_json: bool
    explicit_type: ColumnType | None


class DBFieldInfo(FieldInfo):
    """
    Extended field information for database fields, building upon Pydantic's FieldInfo.
    This class adds database-specific attributes and functionality for field configuration
    in SQL databases, particularly PostgreSQL.

    This class is used internally by the Field constructor to store metadata about
    database columns, including constraints, foreign keys, and PostgreSQL-specific
    configurations.
    """

    primary_key: bool = False
    """
    Indicates if this field serves as the primary key for the table.
    When True, this field will be used as the unique identifier for rows.
    """

    autoincrement: bool = False
    """
    Controls whether the field should automatically increment.
    By default, this is True for primary key fields that have no default value set.
    """

    postgres_config: PostgresFieldBase | None = None
    """
    Custom PostgreSQL configuration for the field.
    Allows for type-specific customization of PostgreSQL parameters.
    """

    foreign_key: str | None = None
    """
    Specifies a foreign key relationship to another table.
    Format should be "table_name.column_name" if set.
    """

    unique: bool = False
    """
    When True, enforces that all values in this column must be unique.
    Creates a unique constraint in the database.
    """

    index: bool = False
    """
    When True, creates an index on this column to optimize query performance.
    """

    check_expression: str | None = None
    """
    SQL expression for a CHECK constraint on this column.
    Allows for custom validation rules at the database level.
    """

    is_json: bool = False
    """
    Indicates if this field should be stored as JSON in the database.
    When True, the field's value will be JSON serialized before storage.
    """

    explicit_type: ColumnType | None = None
    """
    Explicitly specify the SQL column type for this field.
    When set, this type takes precedence over automatic type inference.
    """

    def __init__(self, **kwargs: Unpack[DBFieldInputs]):
        """
        Initialize a new DBFieldInfo instance with the given field configuration.

        :param kwargs: Keyword arguments that configure the field's behavior.
            Includes all standard Pydantic field options plus database-specific options.

        """
        # The super call should persist all kwargs as _attributes_set
        # We're intentionally passing kwargs that we know aren't in the
        # base typehinted dict
        super().__init__(**kwargs)  # type: ignore
        self.primary_key = kwargs.pop("primary_key", False)
        self.autoincrement = kwargs.pop(
            "autoincrement", (self.primary_key and self.default is None)
        )
        self.postgres_config = kwargs.pop("postgres_config", None)
        self.foreign_key = kwargs.pop("foreign_key", None)
        self.unique = kwargs.pop("unique", False)
        self.index = kwargs.pop("index", False)
        self.check_expression = kwargs.pop("check_expression", None)
        self.is_json = kwargs.pop("is_json", False)
        self.explicit_type = kwargs.pop("explicit_type", None)

    @classmethod
    def extend_field(
        cls,
        field: FieldInfo,
        primary_key: bool,
        postgres_config: PostgresFieldBase | None,
        foreign_key: str | None,
        unique: bool,
        index: bool,
        check_expression: str | None,
        is_json: bool,
        explicit_type: ColumnType | None,
    ):
        """
        Helper function to extend a Pydantic FieldInfo with database-specific attributes.

        """
        return cls(
            primary_key=primary_key,
            postgres_config=postgres_config,
            foreign_key=foreign_key,
            unique=unique,
            index=index,
            check_expression=check_expression,
            is_json=is_json,
            explicit_type=explicit_type,
            **field._attributes_set,  # type: ignore
        )

    def to_db_value(self, value: Any):
        if self.is_json:
            return json_dumps(value)
        return value


def __get_db_field(_: Callable[Concatenate[Any, P], Any] = PydanticField):  # type: ignore
    """
    Workaround constructor to pass typehints through our function subclass
    to the original PydanticField constructor

    """

    def func(
        primary_key: bool = False,
        postgres_config: PostgresFieldBase | None = None,
        foreign_key: str | None = None,
        unique: bool = False,
        index: bool = False,
        check_expression: str | None = None,
        is_json: bool = False,
        explicit_type: ColumnType | None = None,
        default: Any = _Unset,
        default_factory: (
            Callable[[], Any] | Callable[[dict[str, Any]], Any] | None
        ) = _Unset,
        *args: P.args,
        **kwargs: P.kwargs,
    ):
        raw_field = PydanticField(
            default=default, default_factory=default_factory, **kwargs
        )  # type: ignore

        # The Any request is required for us to be able to assign fields to any
        # arbitrary type, like `value: str = Field()`
        return cast(
            Any,
            DBFieldInfo.extend_field(
                raw_field,
                primary_key=primary_key,
                postgres_config=postgres_config,
                foreign_key=foreign_key,
                unique=unique,
                index=index,
                check_expression=check_expression,
                is_json=is_json,
                explicit_type=explicit_type,
            ),
        )

    return func


T = TypeVar("T")


class DBFieldClassDefinition(Generic[T], ComparisonBase[T]):
    """
    The returned model when users access a field directly from
    the table class, e.g. `User.id`

    """

    root_model: Type["TableBase"]
    key: str
    field_definition: DBFieldInfo

    def __init__(
        self,
        root_model: Type["TableBase"],
        key: str,
        field_definition: DBFieldInfo,
    ):
        self.root_model = root_model
        self.key = key
        self.field_definition = field_definition

    def to_query(self):
        table = QueryIdentifier(self.root_model.get_table_name())
        column = QueryIdentifier(self.key)
        return QueryLiteral(f"{table}.{column}"), []


Field = __get_db_field()
"""
Create a new database field with optional database-specific configurations.

This function extends Pydantic's Field with additional database functionality. It accepts
all standard Pydantic Field parameters plus all the database-specific parameters defined
in DBFieldInfo.

```python {{sticky: True}}
from iceaxe import Field
from iceaxe.base import TableBase

class User(TableBase):
    id: int = Field(primary_key=True)
    username: str = Field(unique=True, index=True)
    settings: dict = Field(is_json=True, default_factory=dict)
    department_id: int = Field(foreign_key="departments.id")
```

"""
