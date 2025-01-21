from enum import StrEnum
from typing import Literal

from pydantic import BaseModel


class LexemePriority(StrEnum):
    """Enum representing text search lexeme priority weights in Postgres."""

    HIGHEST = "A"
    HIGH = "B"
    LOW = "C"
    LOWEST = "D"


class PostgresFieldBase(BaseModel):
    """
    Extensions to python core types that specify addition arguments
    used by Postgres.

    """

    pass


class PostgresDateTime(PostgresFieldBase):
    """
    Extension to Python's datetime type that specifies additional Postgres-specific configuration.
    Used to customize the timezone behavior of datetime fields in Postgres.

    ```python {{sticky: True}}
    from iceaxe import Field, TableBase
    class Event(TableBase):
        id: int = Field(primary_key=True)
        created_at: datetime = Field(postgres_config=PostgresDateTime(timezone=True))
    ```
    """

    timezone: bool = False
    """
    Whether the datetime field should include timezone information in Postgres.
        If True, maps to TIMESTAMP WITH TIME ZONE.
        If False, maps to TIMESTAMP WITHOUT TIME ZONE.
        Defaults to False.

    """


class PostgresTime(PostgresFieldBase):
    """
    Extension to Python's time type that specifies additional Postgres-specific configuration.
    Used to customize the timezone behavior of time fields in Postgres.

    ```python {{sticky: True}}
    from iceaxe import Field, TableBase
    class Schedule(TableBase):
        id: int = Field(primary_key=True)
        start_time: time = Field(postgres_config=PostgresTime(timezone=True))
    ```
    """

    timezone: bool = False
    """
    Whether the time field should include timezone information in Postgres.
        If True, maps to TIME WITH TIME ZONE.
        If False, maps to TIME WITHOUT TIME ZONE.
        Defaults to False.

    """


class PostgresFullText(PostgresFieldBase):
    """
    Extension to Python's string type that specifies additional Postgres-specific configuration
    for full-text search. Used to customize the behavior of text search fields in Postgres.

    ```python {{sticky: True}}
    from iceaxe import TableBase, Field
    from iceaxe.postgres import PostgresFullText, LexemePriority

    class Article(TableBase):
        id: int = Field(primary_key=True)
        title: str = Field(postgres_config=PostgresFullText(
            language="english",
            weight=LexemePriority.HIGHEST  # or "A"
        ))
        content: str = Field(postgres_config=PostgresFullText(
            language="english",
            weight=LexemePriority.HIGH  # or "B"
        ))
    ```
    """

    language: str = "english"
    """
    The language to use for text search operations.
    Defaults to 'english'.
    """
    weight: Literal["A", "B", "C", "D"] | LexemePriority = LexemePriority.HIGHEST
    """
    The weight to assign to matches in this column.
    Can be specified either as a string literal ("A", "B", "C", "D") or using LexemePriority enum.
    A/HIGHEST is highest priority, D/LOWEST is lowest priority.
    Defaults to LexemePriority.HIGHEST (A).
    """


ForeignKeyModifications = Literal[
    "RESTRICT", "NO ACTION", "CASCADE", "SET DEFAULT", "SET NULL"
]


class PostgresForeignKey(PostgresFieldBase):
    """
    Extension to Python's ForeignKey type that specifies additional Postgres-specific configuration.
    Used to customize the behavior of foreign key constraints in Postgres.

    ```python {{sticky: True}}
    from iceaxe import TableBase, Field

    class Office(TableBase):
        id: int = Field(primary_key=True)
        name: str

    class Employee(TableBase):
        id: int = Field(primary_key=True)
        name: str
        office_id: int = Field(foreign_key="office.id", postgres_config=PostgresForeignKey(on_delete="CASCADE", on_update="CASCADE"))
    ```
    """

    on_delete: ForeignKeyModifications = "NO ACTION"
    on_update: ForeignKeyModifications = "NO ACTION"
