import warnings
from datetime import date, datetime, time, timedelta
from enum import Enum, IntEnum, StrEnum
from typing import Generic, Sequence, TypeVar
from unittest.mock import ANY
from uuid import UUID

import pytest
from pydantic import create_model
from pydantic.fields import FieldInfo

from iceaxe import Field, TableBase
from iceaxe.base import IndexConstraint, UniqueConstraint
from iceaxe.field import DBFieldInfo
from iceaxe.postgres import PostgresDateTime, PostgresForeignKey, PostgresTime
from iceaxe.schemas.actions import (
    ColumnType,
    ConstraintType,
    DatabaseActions,
    DryRunAction,
    DryRunComment,
)
from iceaxe.schemas.db_memory_serializer import (
    CompositePrimaryKeyConstraintError,
    DatabaseHandler,
    DatabaseMemorySerializer,
)
from iceaxe.schemas.db_stubs import (
    DBColumn,
    DBConstraint,
    DBObject,
    DBObjectPointer,
    DBTable,
    DBType,
    DBTypePointer,
)


def compare_db_objects(
    calculated: Sequence[tuple[DBObject, Sequence[DBObject | DBObjectPointer]]],
    expected: Sequence[tuple[DBObject, Sequence[DBObject | DBObjectPointer]]],
):
    """
    Helper function to compare lists of DBObjects. The order doesn't actually matter
    for downstream uses, but we can't do a simple equality check with a set because the
    dependencies list is un-hashable.

    """
    assert sorted(calculated, key=lambda x: x[0].representation()) == sorted(
        expected, key=lambda x: x[0].representation()
    )


@pytest.mark.asyncio
async def test_from_scratch_migration():
    """
    Test a migration from scratch.

    """

    class OldValues(Enum):
        A = "A"

    class ModelA(TableBase):
        id: int = Field(primary_key=True)
        animal: OldValues
        was_nullable: str | None

    migrator = DatabaseMemorySerializer()

    db_objects = list(migrator.delegate([ModelA]))
    next_ordering = migrator.order_db_objects(db_objects)

    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor, [], {}, [obj for obj, _ in db_objects], next_ordering
    )

    assert actions == [
        DryRunAction(
            fn=actor.add_type,
            kwargs={
                "type_name": "oldvalues",
                "values": [
                    "A",
                ],
            },
        ),
        DryRunComment(
            text="\nNEW TABLE: modela\n",
            previous_line=False,
        ),
        DryRunAction(
            fn=actor.add_table,
            kwargs={
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "id",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.INTEGER,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "column_name": "id",
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "animal",
                "custom_data_type": "oldvalues",
                "explicit_data_is_list": False,
                "explicit_data_type": None,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "column_name": "animal",
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "was_nullable",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.VARCHAR,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_constraint,
            kwargs={
                "columns": [
                    "id",
                ],
                "constraint": ConstraintType.PRIMARY_KEY,
                "constraint_args": None,
                "constraint_name": "modela_pkey",
                "table_name": "modela",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_diff_migration():
    """
    Test the diff migration between two schemas.

    """

    class OldValues(Enum):
        A = "A"

    class NewValues(Enum):
        A = "A"
        B = "B"

    class ModelA(TableBase):
        id: int = Field(primary_key=True)
        animal: OldValues
        was_nullable: str | None

    class ModelANew(TableBase):
        table_name = "modela"
        id: int = Field(primary_key=True)
        name: str
        animal: NewValues
        was_nullable: str

    actor = DatabaseActions()
    migrator = DatabaseMemorySerializer()

    db_objects = list(migrator.delegate([ModelA]))
    db_objects_previous = [obj for obj, _ in db_objects]
    previous_ordering = migrator.order_db_objects(db_objects)

    db_objects_new = list(migrator.delegate([ModelANew]))
    db_objects_next = [obj for obj, _ in db_objects_new]
    next_ordering = migrator.order_db_objects(db_objects_new)

    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor, db_objects_previous, previous_ordering, db_objects_next, next_ordering
    )
    assert actions == [
        DryRunAction(
            fn=actor.add_type,
            kwargs={
                "type_name": "newvalues",
                "values": [
                    "A",
                    "B",
                ],
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "name",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.VARCHAR,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "column_name": "name",
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.modify_column_type,
            kwargs={
                "column_name": "animal",
                "custom_data_type": "newvalues",
                "explicit_data_is_list": False,
                "explicit_data_type": None,
                "table_name": "modela",
                "autocast": True,
            },
        ),
        DryRunComment(
            text="TODO: Perform a migration of values across types",
            previous_line=True,
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "column_name": "was_nullable",
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.drop_type,
            kwargs={
                "type_name": "oldvalues",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_duplicate_enum_migration():
    """
    Test that the shared reference to an enum across multiple tables results in only
    one migration action to define the type.

    """

    class EnumValues(Enum):
        A = "A"
        B = "B"

    class Model1(TableBase):
        id: int = Field(primary_key=True)
        value: EnumValues

    class Model2(TableBase):
        id: int = Field(primary_key=True)
        value: EnumValues

    migrator = DatabaseMemorySerializer()

    db_objects = list(migrator.delegate([Model1, Model2]))
    next_ordering = migrator.order_db_objects(db_objects)

    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor, [], {}, [obj for obj, _ in db_objects], next_ordering
    )

    assert actions == [
        DryRunAction(
            fn=actor.add_type,
            kwargs={
                "type_name": "enumvalues",
                "values": [
                    "A",
                    "B",
                ],
            },
        ),
        DryRunComment(
            text="\nNEW TABLE: model1\n",
            previous_line=False,
        ),
        DryRunAction(
            fn=actor.add_table,
            kwargs={
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "id",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.INTEGER,
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "column_name": "id",
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "value",
                "custom_data_type": "enumvalues",
                "explicit_data_is_list": False,
                "explicit_data_type": None,
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "column_name": "value",
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_constraint,
            kwargs={
                "columns": [
                    "id",
                ],
                "constraint": ConstraintType.PRIMARY_KEY,
                "constraint_args": None,
                "constraint_name": "model1_pkey",
                "table_name": "model1",
            },
        ),
        DryRunComment(
            text="\nNEW TABLE: model2\n",
            previous_line=False,
        ),
        DryRunAction(
            fn=actor.add_table,
            kwargs={
                "table_name": "model2",
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "id",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.INTEGER,
                "table_name": "model2",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "column_name": "id",
                "table_name": "model2",
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "value",
                "custom_data_type": "enumvalues",
                "explicit_data_is_list": False,
                "explicit_data_type": None,
                "table_name": "model2",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "column_name": "value",
                "table_name": "model2",
            },
        ),
        DryRunAction(
            fn=actor.add_constraint,
            kwargs={
                "columns": [
                    "id",
                ],
                "constraint": ConstraintType.PRIMARY_KEY,
                "constraint_args": None,
                "constraint_name": "model2_pkey",
                "table_name": "model2",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_required_db_default():
    """
    Even if we have a default value in Python, we should still force the content
    to have a value at the db level.

    """

    class Model1(TableBase):
        id: int = Field(primary_key=True)
        value: str = "ABC"
        value2: str = Field(default="ABC")

    migrator = DatabaseMemorySerializer()

    db_objects = list(migrator.delegate([Model1]))
    next_ordering = migrator.order_db_objects(db_objects)

    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor, [], {}, [obj for obj, _ in db_objects], next_ordering
    )

    assert actions == [
        DryRunComment(text="\nNEW TABLE: model1\n"),
        DryRunAction(fn=actor.add_table, kwargs={"table_name": "model1"}),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "id",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.INTEGER,
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null, kwargs={"column_name": "id", "table_name": "model1"}
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "value",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.VARCHAR,
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={"column_name": "value", "table_name": "model1"},
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "value2",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.VARCHAR,
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={"column_name": "value2", "table_name": "model1"},
        ),
        DryRunAction(
            fn=actor.add_constraint,
            kwargs={
                "columns": ["id"],
                "constraint": ConstraintType.PRIMARY_KEY,
                "constraint_args": None,
                "constraint_name": "model1_pkey",
                "table_name": "model1",
            },
        ),
    ]


def test_multiple_primary_keys(clear_all_database_objects):
    """
    Support models defined with multiple primary keys. This should
    result in a composite constraint, which has different handling internally
    than most other field-constraints that are isolated to the field itself.

    """

    class ExampleModel(TableBase):
        value_a: UUID = Field(primary_key=True)
        value_b: UUID = Field(primary_key=True)

    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([ExampleModel]))
    assert db_objects == [
        (
            DBTable(table_name="examplemodel"),
            [],
        ),
        (
            DBColumn(
                table_name="examplemodel",
                column_name="value_a",
                column_type=ColumnType.UUID,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="examplemodel"),
            ],
        ),
        (
            DBColumn(
                table_name="examplemodel",
                column_name="value_b",
                column_type=ColumnType.UUID,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="examplemodel"),
            ],
        ),
        (
            DBConstraint(
                table_name="examplemodel",
                constraint_name="examplemodel_pkey",
                columns=frozenset({"value_a", "value_b"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
            ),
            [
                DBTable(table_name="examplemodel"),
                DBColumn(
                    table_name="examplemodel",
                    column_name="value_a",
                    column_type=ColumnType.UUID,
                    column_is_list=False,
                    nullable=False,
                ),
                DBColumn(
                    table_name="examplemodel",
                    column_name="value_b",
                    column_type=ColumnType.UUID,
                    column_is_list=False,
                    nullable=False,
                ),
            ],
        ),
    ]


def test_enum_column_assignment(clear_all_database_objects):
    """
    Enum values will just yield the current column that they are assigned to even if they
    are assigned to multiple columns. It's up to the full memory serializer to combine them
    so we can properly track how we can migrate existing enum/column pairs to the
    new values.

    """

    class CommonEnum(Enum):
        A = "a"
        B = "b"

    class ExampleModel1(TableBase):
        id: UUID = Field(primary_key=True)
        value: CommonEnum

    class ExampleModel2(TableBase):
        id: UUID = Field(primary_key=True)
        value: CommonEnum

    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([ExampleModel1, ExampleModel2]))
    assert db_objects == [
        (
            DBTable(table_name="examplemodel1"),
            [],
        ),
        (
            DBColumn(
                table_name="examplemodel1",
                column_name="id",
                column_type=ColumnType.UUID,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="examplemodel1"),
            ],
        ),
        (
            DBType(
                name="commonenum",
                values=frozenset({"b", "a"}),
                reference_columns=frozenset({("examplemodel1", "value")}),
            ),
            [],
        ),
        (
            DBColumn(
                table_name="examplemodel1",
                column_name="value",
                column_type=DBTypePointer(name="commonenum"),
                column_is_list=False,
                nullable=False,
            ),
            [
                DBType(
                    name="commonenum",
                    values=frozenset({"b", "a"}),
                    reference_columns=frozenset({("examplemodel1", "value")}),
                ),
                DBTable(table_name="examplemodel1"),
            ],
        ),
        (
            DBConstraint(
                table_name="examplemodel1",
                constraint_name="examplemodel1_pkey",
                columns=frozenset({"id"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
                check_constraint=None,
            ),
            [
                DBType(
                    name="commonenum",
                    values=frozenset({"b", "a"}),
                    reference_columns=frozenset({("examplemodel1", "value")}),
                ),
                DBTable(table_name="examplemodel1"),
                DBColumn(
                    table_name="examplemodel1",
                    column_name="id",
                    column_type=ColumnType.UUID,
                    column_is_list=False,
                    nullable=False,
                ),
                DBColumn(
                    table_name="examplemodel1",
                    column_name="value",
                    column_type=DBTypePointer(name="commonenum"),
                    column_is_list=False,
                    nullable=False,
                ),
            ],
        ),
        (
            DBTable(table_name="examplemodel2"),
            [],
        ),
        (
            DBColumn(
                table_name="examplemodel2",
                column_name="id",
                column_type=ColumnType.UUID,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="examplemodel2"),
            ],
        ),
        (
            DBType(
                name="commonenum",
                values=frozenset({"b", "a"}),
                reference_columns=frozenset({("examplemodel2", "value")}),
            ),
            [],
        ),
        (
            DBColumn(
                table_name="examplemodel2",
                column_name="value",
                column_type=DBTypePointer(name="commonenum"),
                column_is_list=False,
                nullable=False,
            ),
            [
                DBType(
                    name="commonenum",
                    values=frozenset({"b", "a"}),
                    reference_columns=frozenset({("examplemodel2", "value")}),
                ),
                DBTable(table_name="examplemodel2"),
            ],
        ),
        (
            DBConstraint(
                table_name="examplemodel2",
                constraint_name="examplemodel2_pkey",
                columns=frozenset({"id"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
                check_constraint=None,
            ),
            [
                DBType(
                    name="commonenum",
                    values=frozenset({"b", "a"}),
                    reference_columns=frozenset({("examplemodel2", "value")}),
                ),
                DBTable(table_name="examplemodel2"),
                DBColumn(
                    table_name="examplemodel2",
                    column_name="id",
                    column_type=ColumnType.UUID,
                    column_is_list=False,
                    nullable=False,
                ),
                DBColumn(
                    table_name="examplemodel2",
                    column_name="value",
                    column_type=DBTypePointer(name="commonenum"),
                    column_is_list=False,
                    nullable=False,
                ),
            ],
        ),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field_name, annotation, field_info, expected_db_objects",
    [
        # datetime, default no typehinting
        (
            "standard_datetime",
            datetime,
            Field(),
            [
                (
                    DBColumn(
                        table_name="exampledbmodel",
                        column_name="standard_datetime",
                        column_type=ColumnType.TIMESTAMP_WITHOUT_TIME_ZONE,
                        column_is_list=False,
                        nullable=False,
                    ),
                    [
                        DBTable(table_name="exampledbmodel"),
                    ],
                ),
            ],
        ),
        # datetime, specified with field arguments
        (
            "standard_datetime",
            datetime,
            Field(postgres_config=PostgresDateTime(timezone=True)),
            [
                (
                    DBColumn(
                        table_name="exampledbmodel",
                        column_name="standard_datetime",
                        column_type=ColumnType.TIMESTAMP_WITH_TIME_ZONE,
                        column_is_list=False,
                        nullable=False,
                    ),
                    [
                        DBTable(table_name="exampledbmodel"),
                    ],
                ),
            ],
        ),
        # date
        (
            "standard_date",
            date,
            Field(),
            [
                (
                    DBColumn(
                        table_name="exampledbmodel",
                        column_name="standard_date",
                        column_type=ColumnType.DATE,
                        column_is_list=False,
                        nullable=False,
                    ),
                    [
                        DBTable(table_name="exampledbmodel"),
                    ],
                ),
            ],
        ),
        # time, no typehinting
        (
            "standard_time",
            time,
            Field(),
            [
                (
                    DBColumn(
                        table_name="exampledbmodel",
                        column_name="standard_time",
                        column_type=ColumnType.TIME_WITHOUT_TIME_ZONE,
                        column_is_list=False,
                        nullable=False,
                    ),
                    [
                        DBTable(table_name="exampledbmodel"),
                    ],
                ),
            ],
        ),
        # time, specified with field arguments
        (
            "standard_time",
            time,
            Field(postgres_config=PostgresTime(timezone=True)),
            [
                (
                    DBColumn(
                        table_name="exampledbmodel",
                        column_name="standard_time",
                        column_type=ColumnType.TIME_WITH_TIME_ZONE,
                        column_is_list=False,
                        nullable=False,
                    ),
                    [
                        DBTable(table_name="exampledbmodel"),
                    ],
                ),
            ],
        ),
        # timedelta
        (
            "standard_timedelta",
            timedelta,
            Field(),
            [
                (
                    DBColumn(
                        table_name="exampledbmodel",
                        column_name="standard_timedelta",
                        column_type=ColumnType.INTERVAL,
                        column_is_list=False,
                        nullable=False,
                    ),
                    [
                        DBTable(table_name="exampledbmodel"),
                    ],
                ),
            ],
        ),
    ],
)
async def test_datetimes(
    field_name: str,
    annotation: type,
    field_info: FieldInfo,
    expected_db_objects: list[tuple[DBObject, list[DBObject | DBObjectPointer]]],
):
    ExampleDBModel = create_model(  # type: ignore
        "ExampleDBModel",
        __base__=TableBase,
        **{  # type: ignore
            # Requires the ID to be specified for the model to be constructed correctly
            "id": (int, Field(primary_key=True)),
            field_name: (annotation, field_info),
        },
    )

    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([ExampleDBModel]))

    # Table and primary key are created for each model
    base_db_objects: list[tuple[DBObject, list[DBObject | DBObjectPointer]]] = [
        (
            DBTable(table_name="exampledbmodel"),
            [],
        ),
        (
            DBColumn(
                table_name="exampledbmodel",
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="exampledbmodel"),
            ],
        ),
        (
            DBConstraint(
                table_name="exampledbmodel",
                constraint_name="exampledbmodel_pkey",
                columns=frozenset({"id"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
            ),
            [
                DBTable(table_name="exampledbmodel"),
                DBColumn(
                    table_name="exampledbmodel",
                    column_name="id",
                    column_type=ColumnType.INTEGER,
                    column_is_list=False,
                    nullable=False,
                ),
                DBColumn.model_construct(
                    table_name="exampledbmodel",
                    column_name=field_name,
                    column_type=ANY,
                    column_is_list=False,
                    nullable=False,
                ),
            ],
        ),
    ]

    compare_db_objects(db_objects, base_db_objects + expected_db_objects)


def test_order_db_objects_sorts_by_table():
    """
    Unless there are some explicit cross-table dependencies, we should group
    table operations together in one code block.

    """

    class OldValues(Enum):
        A = "A"

    class ModelA(TableBase):
        id: int = Field(primary_key=True)
        animal: OldValues
        was_nullable: str | None

    class ModelB(TableBase):
        id: int = Field(primary_key=True)
        animal: OldValues
        was_nullable: str | None

    migrator = DatabaseMemorySerializer()

    db_objects = list(migrator.delegate([ModelA, ModelB]))
    next_ordering = migrator.order_db_objects(db_objects)

    sorted_actions = sorted(next_ordering.items(), key=lambda x: x[1])

    table_order = [
        action.table_name
        for action, _ in sorted_actions
        if isinstance(action, (DBTable, DBColumn, DBConstraint))
    ]

    # Table 3 columns 1 primary constraint
    assert table_order == ["modela"] * 5 + ["modelb"] * 5


@pytest.mark.asyncio
async def test_generic_field_subclass():
    class OldValues(Enum):
        A = "A"

    T = TypeVar("T")

    class GenericSuperclass(Generic[T]):
        value: T

    class ModelA(TableBase, GenericSuperclass[OldValues]):
        id: int = Field(primary_key=True)

    migrator = DatabaseMemorySerializer()

    db_objects = list(migrator.delegate([ModelA]))
    next_ordering = migrator.order_db_objects(db_objects)

    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor, [], {}, [obj for obj, _ in db_objects], next_ordering
    )

    assert actions == [
        DryRunAction(
            fn=actor.add_type,
            kwargs={
                "type_name": "oldvalues",
                "values": [
                    "A",
                ],
            },
        ),
        DryRunComment(
            text="\nNEW TABLE: modela\n",
            previous_line=False,
        ),
        DryRunAction(
            fn=actor.add_table,
            kwargs={
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "value",
                "custom_data_type": "oldvalues",
                "explicit_data_is_list": False,
                "explicit_data_type": None,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "column_name": "value",
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "id",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.INTEGER,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "column_name": "id",
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_constraint,
            kwargs={
                "columns": [
                    "id",
                ],
                "constraint": ConstraintType.PRIMARY_KEY,
                "constraint_args": None,
                "constraint_name": "modela_pkey",
                "table_name": "modela",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_serial_only_on_create():
    """
    SERIAL types should only be used during table creation. Test a synthetic
    migration where we both create an initial SERIAL and migrate from a "db" table
    schema (that won't have autoincrement set) to a "new" table schema (that will).
    Nothing should happen to the id column in this case.

    """

    class ModelA(TableBase):
        id: int | None = Field(default=None, primary_key=True)
        value: int

    class ModelADB(TableBase):
        table_name = "modela"
        id: int | None = Field(primary_key=True)
        value_b: int

    # Because "default" is omitted, this should be detected as a regular INTEGER
    # column and not a SERIAL column.
    id_definition = [field for field in ModelADB.model_fields.values()]
    assert id_definition[0].autoincrement is False

    migrator = DatabaseMemorySerializer()

    memory_objects = list(migrator.delegate([ModelA]))
    memory_ordering = migrator.order_db_objects(memory_objects)

    db_objects = list(migrator.delegate([ModelADB]))
    db_ordering = migrator.order_db_objects(db_objects)

    # At the DBColumn level, these should both be integer objects
    id_columns = [
        column
        for column, _ in memory_objects + db_objects
        if isinstance(column, DBColumn) and column.column_name == "id"
    ]
    assert [column.column_type for column in id_columns] == [
        ColumnType.INTEGER,
        ColumnType.INTEGER,
    ]

    # First, test the creation logic. We expect to see a SERIAL column here.
    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor, [], {}, [obj for obj, _ in memory_objects], memory_ordering
    )

    assert [
        action
        for action in actions
        if isinstance(action, DryRunAction) and action.kwargs.get("column_name") == "id"
    ] == [
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "id",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.SERIAL,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null, kwargs={"table_name": "modela", "column_name": "id"}
        ),
    ]

    # Now, test the migration logic. We expect to see no changes to the id
    # column here because integers should logically equal serials for the purposes
    # of migration differences.
    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor,
        [obj for obj, _ in db_objects],
        db_ordering,
        [obj for obj, _ in memory_objects],
        memory_ordering,
    )
    assert [
        action
        for action in actions
        if isinstance(action, DryRunAction) and action.kwargs.get("column_name") == "id"
    ] == []


#
# Column type parsing
#


def test_parse_enums():
    class ModelA(TableBase):
        id: int = Field(primary_key=True)

    database_handler = DatabaseHandler()

    class StrEnumDemo(StrEnum):
        A = "a"
        B = "b"

    type_declaration = database_handler.handle_column_type(
        "test_key",
        DBFieldInfo(annotation=StrEnumDemo),
        ModelA,
    )
    assert isinstance(type_declaration.custom_type, DBType)
    assert type_declaration.custom_type.name == "strenumdemo"
    assert type_declaration.custom_type.values == frozenset(["a", "b"])

    class IntEnumDemo(IntEnum):
        A = 1
        B = 2

    with pytest.raises(ValueError, match="string values are supported for enums"):
        database_handler.handle_column_type(
            "test_key",
            DBFieldInfo(annotation=IntEnumDemo),
            ModelA,
        )

    class StandardEnumDemo(Enum):
        A = "a"
        B = "b"

    type_declaration = database_handler.handle_column_type(
        "test_key",
        DBFieldInfo(annotation=StandardEnumDemo),
        ModelA,
    )
    assert isinstance(type_declaration.custom_type, DBType)
    assert type_declaration.custom_type.name == "standardenumdemo"
    assert type_declaration.custom_type.values == frozenset(["a", "b"])


def test_all_constraint_types(clear_all_database_objects):
    """
    Test that all types of constraints (foreign keys, unique constraints, indexes,
    and primary keys) are correctly serialized from TableBase schemas.
    """

    class ParentModel(TableBase):
        id: int = Field(primary_key=True)
        name: str = Field(unique=True)

    class ChildModel(TableBase):
        id: int = Field(primary_key=True)
        parent_id: int = Field(foreign_key="parentmodel.id")
        name: str
        email: str
        status: str

        table_args = [
            UniqueConstraint(columns=["name", "email"]),
            IndexConstraint(columns=["status"]),
        ]

    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([ParentModel, ChildModel]))

    # Extract all constraints for verification
    constraints = [obj for obj, _ in db_objects if isinstance(obj, DBConstraint)]

    # Verify ParentModel constraints
    parent_constraints = [c for c in constraints if c.table_name == "parentmodel"]
    assert len(parent_constraints) == 2

    # Primary key constraint
    pk_constraint = next(
        c for c in parent_constraints if c.constraint_type == ConstraintType.PRIMARY_KEY
    )
    assert pk_constraint.columns == frozenset({"id"})
    assert pk_constraint.constraint_name == "parentmodel_pkey"

    # Unique constraint on name
    unique_constraint = next(
        c for c in parent_constraints if c.constraint_type == ConstraintType.UNIQUE
    )
    assert unique_constraint.columns == frozenset({"name"})
    assert unique_constraint.constraint_name == "parentmodel_name_unique"

    # Verify ChildModel constraints
    child_constraints = [c for c in constraints if c.table_name == "childmodel"]
    assert len(child_constraints) == 4  # PK, FK, Unique, Index

    # Primary key constraint
    child_pk = next(
        c for c in child_constraints if c.constraint_type == ConstraintType.PRIMARY_KEY
    )
    assert child_pk.columns == frozenset({"id"})
    assert child_pk.constraint_name == "childmodel_pkey"

    # Foreign key constraint
    fk_constraint = next(
        c for c in child_constraints if c.constraint_type == ConstraintType.FOREIGN_KEY
    )
    assert fk_constraint.columns == frozenset({"parent_id"})
    assert fk_constraint.constraint_name == "childmodel_parent_id_fkey"
    assert fk_constraint.foreign_key_constraint is not None
    assert fk_constraint.foreign_key_constraint.target_table == "parentmodel"
    assert fk_constraint.foreign_key_constraint.target_columns == frozenset({"id"})

    # Composite unique constraint
    composite_unique = next(
        c for c in child_constraints if c.constraint_type == ConstraintType.UNIQUE
    )
    assert composite_unique.columns == frozenset({"name", "email"})
    # The order of columns in the constraint name doesn't matter for functionality
    assert composite_unique.constraint_name in [
        "childmodel_name_email_unique",
        "childmodel_email_name_unique",
    ]

    # Index constraint
    index_constraint = next(
        c for c in child_constraints if c.constraint_type == ConstraintType.INDEX
    )
    assert index_constraint.columns == frozenset({"status"})
    assert index_constraint.constraint_name == "childmodel_status_idx"


def test_primary_key_not_null(clear_all_database_objects):
    """
    Test that primary key fields are automatically marked as not-null in their
    intermediary representation, since primary keys cannot be null.

    This includes both explicitly set primary keys and auto-assigned ones.
    """

    class ExplicitModel(TableBase):
        id: int = Field(primary_key=True)
        name: str

    class AutoAssignedModel(TableBase):
        id: int | None = Field(default=None, primary_key=True)
        name: str

    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([ExplicitModel, AutoAssignedModel]))

    # Extract the column definitions
    columns = [obj for obj, _ in db_objects if isinstance(obj, DBColumn)]

    # Find the explicit primary key column
    explicit_id_column = next(
        c for c in columns if c.column_name == "id" and c.table_name == "explicitmodel"
    )
    assert not explicit_id_column.nullable

    # Find the auto-assigned primary key column
    auto_id_column = next(
        c
        for c in columns
        if c.column_name == "id" and c.table_name == "autoassignedmodel"
    )
    assert not auto_id_column.nullable
    assert auto_id_column.autoincrement


@pytest.mark.asyncio
async def test_foreign_key_table_dependency():
    """
    Test that foreign key constraints properly depend on the referenced table being created first.
    This test verifies that the foreign key constraint is ordered after both tables are created.
    """

    class TargetModel(TableBase):
        id: int = Field(primary_key=True)
        value: str

    class SourceModel(TableBase):
        id: int = Field(primary_key=True)
        target_id: int = Field(foreign_key="targetmodel.id")

    migrator = DatabaseMemorySerializer()

    # Make sure Source is parsed before Target so we can make sure our foreign-key
    # constraint actually re-orders the final objects.
    db_objects = list(migrator.delegate([SourceModel, TargetModel]))
    ordering = migrator.order_db_objects(db_objects)

    # Get all objects in their sorted order
    sorted_objects = sorted(
        [obj for obj, _ in db_objects], key=lambda obj: ordering[obj]
    )

    # Find the positions of key objects
    target_table_pos = next(
        i
        for i, obj in enumerate(sorted_objects)
        if isinstance(obj, DBTable) and obj.table_name == "targetmodel"
    )
    source_table_pos = next(
        i
        for i, obj in enumerate(sorted_objects)
        if isinstance(obj, DBTable) and obj.table_name == "sourcemodel"
    )
    target_column_pos = next(
        i
        for i, obj in enumerate(sorted_objects)
        if isinstance(obj, DBColumn)
        and obj.table_name == "targetmodel"
        and obj.column_name == "id"
    )
    target_pk_pos = next(
        i
        for i, obj in enumerate(sorted_objects)
        if isinstance(obj, DBConstraint)
        and obj.constraint_type == ConstraintType.PRIMARY_KEY
        and obj.table_name == "targetmodel"
    )
    fk_constraint_pos = next(
        i
        for i, obj in enumerate(sorted_objects)
        if isinstance(obj, DBConstraint)
        and obj.constraint_type == ConstraintType.FOREIGN_KEY
        and obj.table_name == "sourcemodel"
    )

    # The foreign key constraint should come after both tables and the target column are created
    assert target_table_pos < fk_constraint_pos, (
        "Foreign key constraint should be created after target table"
    )
    assert source_table_pos < fk_constraint_pos, (
        "Foreign key constraint should be created after source table"
    )
    assert target_column_pos < fk_constraint_pos, (
        "Foreign key constraint should be created after target column"
    )
    assert target_pk_pos < fk_constraint_pos, (
        "Foreign key constraint should be created after target primary key"
    )

    # Verify the actual migration actions
    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor, [], {}, [obj for obj, _ in db_objects], ordering
    )

    # Extract the table creation and foreign key constraint actions
    table_creations = [
        action
        for action in actions
        if isinstance(action, DryRunAction) and action.fn == actor.add_table
    ]
    fk_constraints = [
        action
        for action in actions
        if isinstance(action, DryRunAction)
        and action.fn == actor.add_constraint
        and action.kwargs.get("constraint") == ConstraintType.FOREIGN_KEY
    ]

    # Verify that table creations come before foreign key constraints
    assert len(table_creations) == 2
    assert len(fk_constraints) == 1

    table_creation_indices = [
        i for i, action in enumerate(actions) if action in table_creations
    ]
    fk_constraint_indices = [
        i for i, action in enumerate(actions) if action in fk_constraints
    ]

    assert all(
        table_idx < fk_idx
        for table_idx in table_creation_indices
        for fk_idx in fk_constraint_indices
    )


def test_foreign_key_actions():
    """
    Test that foreign key ON UPDATE/ON DELETE actions are correctly serialized from TableBase schemas.
    """

    class ParentModel(TableBase):
        id: int = Field(primary_key=True)

    class ChildModel(TableBase):
        id: int = Field(primary_key=True)
        parent_id: int = Field(
            foreign_key="parentmodel.id",
            postgres_config=PostgresForeignKey(
                on_delete="CASCADE",
                on_update="CASCADE",
            ),
        )

    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([ParentModel, ChildModel]))

    # Extract all constraints for verification
    constraints = [obj for obj, _ in db_objects if isinstance(obj, DBConstraint)]

    # Find the foreign key constraint
    fk_constraint = next(
        c for c in constraints if c.constraint_type == ConstraintType.FOREIGN_KEY
    )
    assert fk_constraint.foreign_key_constraint is not None
    assert fk_constraint.foreign_key_constraint.target_table == "parentmodel"
    assert fk_constraint.foreign_key_constraint.target_columns == frozenset({"id"})
    assert fk_constraint.foreign_key_constraint.on_delete == "CASCADE"
    assert fk_constraint.foreign_key_constraint.on_update == "CASCADE"


def test_multiple_primary_keys_foreign_key_error():
    """
    Test that when a model has multiple primary keys and foreign key constraints,
    we get a helpful error message explaining the issue.
    """

    class User(TableBase):
        id: int = Field(primary_key=True)
        tenant_id: int = Field(primary_key=True)  # Composite primary key
        name: str

    class Topic(TableBase):
        id: str = Field(primary_key=True)
        tenant_id: int = Field(primary_key=True)  # Composite primary key
        title: str

    class Rec(TableBase):
        id: int = Field(primary_key=True, default=None)
        creator_id: int = Field(
            foreign_key="user.id"
        )  # This will fail because user is leveraging our synthetic primary key
        topic_id: str = Field(
            foreign_key="topic.id"
        )  # This will fail because topic is leveraging our synthetic primary key

    migrator = DatabaseMemorySerializer()

    with pytest.raises(CompositePrimaryKeyConstraintError) as exc_info:
        db_objects = list(migrator.delegate([User, Topic, Rec]))
        migrator.order_db_objects(db_objects)

    # Check that the exception has the expected attributes
    assert exc_info.value.missing_constraints == [("user", "id")]


def test_multiple_primary_keys_warning():
    """
    Test that when a model has multiple primary keys, we get a warning.
    """

    class ExampleModel(TableBase):
        value_a: int = Field(primary_key=True)
        value_b: int = Field(primary_key=True)

    migrator = DatabaseMemorySerializer()

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        list(migrator.delegate([ExampleModel]))

        # Check that a warning was issued
        assert len(w) == 1
        assert issubclass(w[0].category, UserWarning)
        warning_message = str(w[0].message)
        assert "multiple fields marked as primary_key=True" in warning_message
        assert "composite primary key constraint" in warning_message
        assert "Consider using only one primary key field" in warning_message


def test_explicit_type_override(clear_all_database_objects):
    """
    Test that explicit_type parameter overrides automatic type inference.
    """

    class TestModel(TableBase):
        id: int = Field(primary_key=True)
        # This should be BIGINT instead of INTEGER due to explicit_type
        big_number: int = Field(explicit_type=ColumnType.BIGINT)
        # This should be TEXT instead of VARCHAR due to explicit_type
        long_text: str = Field(explicit_type=ColumnType.TEXT)
        # This should be JSONB instead of JSON due to explicit_type
        data: dict = Field(is_json=True, explicit_type=ColumnType.JSONB)
        # Normal field without explicit_type for comparison
        normal_field: str = Field()

    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([TestModel]))

    # Extract column definitions
    columns = [obj for obj, _ in db_objects if isinstance(obj, DBColumn)]

    # Find each column and verify the type
    big_number_column = next(c for c in columns if c.column_name == "big_number")
    assert big_number_column.column_type == ColumnType.BIGINT
    assert not big_number_column.nullable

    long_text_column = next(c for c in columns if c.column_name == "long_text")
    assert long_text_column.column_type == ColumnType.TEXT
    assert not long_text_column.nullable

    data_column = next(c for c in columns if c.column_name == "data")
    assert data_column.column_type == ColumnType.JSONB
    assert not data_column.nullable

    # Verify normal field still uses automatic inference
    normal_field_column = next(c for c in columns if c.column_name == "normal_field")
    assert normal_field_column.column_type == ColumnType.VARCHAR
    assert not normal_field_column.nullable

    # Verify the id field uses automatic inference (INTEGER)
    id_column = next(c for c in columns if c.column_name == "id")
    assert id_column.column_type == ColumnType.INTEGER
    assert not id_column.nullable
