from enum import Enum, IntEnum, StrEnum

import pytest

from iceaxe.migrations.actions import (
    ColumnType,
    ConstraintType,
    ForeignKeyConstraint,
)
from iceaxe.migrations.db_serializer import DatabaseSerializer
from iceaxe.migrations.db_stubs import (
    DBColumn,
    DBColumnPointer,
    DBConstraint,
    DBObject,
    DBObjectPointer,
    DBTable,
    DBType,
    DBTypePointer,
)
from iceaxe.session import DBConnection


class ValueEnumStandard(Enum):
    A = "A"


class ValueEnumStr(StrEnum):
    A = "A"


class ValueEnumInt(IntEnum):
    A = 1


def compare_db_objects(
    calculated: list[tuple[DBObject, list[DBObject | DBObjectPointer]]],
    expected: list[tuple[DBObject, list[DBObject | DBObjectPointer]]],
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
@pytest.mark.parametrize(
    "sql_text, expected_db_objects",
    [
        # Enum
        (
            """
            CREATE TYPE valueenumstandard AS ENUM ('A');
            CREATE TABLE exampledbmodel (
                id SERIAL PRIMARY KEY,
                standard_enum valueenumstandard NOT NULL
            );
            """,
            [
                (
                    DBType(
                        name="valueenumstandard",
                        values=frozenset({"A"}),
                        reference_columns=frozenset(
                            {("exampledbmodel", "standard_enum")}
                        ),
                    ),
                    [
                        DBTable(table_name="exampledbmodel"),
                    ],
                ),
                (
                    DBColumn(
                        table_name="exampledbmodel",
                        column_name="standard_enum",
                        column_type=DBTypePointer(
                            name="valueenumstandard",
                        ),
                        column_is_list=False,
                        nullable=False,
                    ),
                    [
                        DBType(
                            name="valueenumstandard",
                            values=frozenset({"A"}),
                            reference_columns=frozenset(
                                {("exampledbmodel", "standard_enum")}
                            ),
                        ),
                        DBTable(table_name="exampledbmodel"),
                    ],
                ),
            ],
        ),
        # Nullable type
        (
            """
            CREATE TABLE exampledbmodel (
                id SERIAL PRIMARY KEY,
                was_nullable VARCHAR
            );
            """,
            [
                (
                    DBColumn(
                        table_name="exampledbmodel",
                        column_name="was_nullable",
                        column_type=ColumnType.VARCHAR,
                        column_is_list=False,
                        nullable=True,
                    ),
                    [
                        DBTable(table_name="exampledbmodel"),
                    ],
                ),
            ],
        ),
        # List types
        (
            """
            CREATE TABLE exampledbmodel (
                id SERIAL PRIMARY KEY,
                array_list VARCHAR[] NOT NULL
            );
            """,
            [
                (
                    DBColumn(
                        table_name="exampledbmodel",
                        column_name="array_list",
                        column_type=ColumnType.VARCHAR,
                        column_is_list=True,
                        nullable=False,
                    ),
                    [
                        DBTable(table_name="exampledbmodel"),
                    ],
                )
            ],
        ),
    ],
)
async def test_simple_db_serializer(
    sql_text: str,
    expected_db_objects: list[tuple[DBObject, list[DBObject | DBObjectPointer]]],
    db_connection: DBConnection,
    clear_all_database_objects,
):
    # Create this new database
    await db_connection.conn.execute(sql_text)

    db_serializer = DatabaseSerializer()
    db_objects = []
    async for values in db_serializer.get_objects(db_connection):
        db_objects.append(values)

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
                DBColumnPointer(table_name="exampledbmodel", column_name="id"),
                DBTable(table_name="exampledbmodel"),
            ],
        ),
    ]

    compare_db_objects(db_objects, base_db_objects + expected_db_objects)


@pytest.mark.asyncio
async def test_db_serializer_foreign_key(
    db_connection: DBConnection,
    clear_all_database_objects,
):
    await db_connection.conn.execute(
        """
        CREATE TABLE foreignmodel (
            id SERIAL PRIMARY KEY
        );
        CREATE TABLE exampledbmodel (
            id SERIAL PRIMARY KEY,
            foreign_key_id INTEGER REFERENCES foreignmodel(id) NOT NULL
        );
        """
    )

    db_serializer = DatabaseSerializer()
    db_objects = []
    async for values in db_serializer.get_objects(db_connection):
        db_objects.append(values)

    expected_db_objects: list[tuple[DBObject, list[DBObject | DBObjectPointer]]] = [
        # Basic ExampleDBModel table
        (
            DBTable(table_name="exampledbmodel"),
            [],
        ),
        (
            DBColumn(
                table_name="exampledbmodel",
                column_name="foreign_key_id",
                column_type=ColumnType.INTEGER,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="exampledbmodel"),
            ],
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
        # ForeignModel table
        (
            DBTable(table_name="foreignmodel"),
            [],
        ),
        (
            DBConstraint(
                table_name="foreignmodel",
                constraint_name="foreignmodel_pkey",
                columns=frozenset({"id"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
            ),
            [
                DBColumnPointer(table_name="foreignmodel", column_name="id"),
                DBTable(table_name="foreignmodel"),
            ],
        ),
        (
            DBColumn(
                table_name="foreignmodel",
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="foreignmodel"),
            ],
        ),
        # Foreign key constraint to link ExampleDBModel to ForeignModel
        (
            DBConstraint(
                table_name="exampledbmodel",
                constraint_name="exampledbmodel_foreign_key_id_fkey",
                columns=frozenset({"foreign_key_id"}),
                constraint_type=ConstraintType.FOREIGN_KEY,
                foreign_key_constraint=ForeignKeyConstraint(
                    target_table="foreignmodel", target_columns=frozenset({"id"})
                ),
            ),
            [
                DBColumnPointer(
                    table_name="exampledbmodel", column_name="foreign_key_id"
                ),
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
                DBColumnPointer(table_name="exampledbmodel", column_name="id"),
                DBTable(table_name="exampledbmodel"),
            ],
        ),
    ]

    compare_db_objects(db_objects, expected_db_objects)