from enum import Enum, IntEnum, StrEnum

import pytest

from iceaxe.schemas.actions import (
    ColumnType,
    ConstraintType,
    ForeignKeyConstraint,
)
from iceaxe.schemas.db_serializer import DatabaseSerializer
from iceaxe.schemas.db_stubs import (
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


class ValueEnumStandard(Enum):
    A = "A"


class ValueEnumStr(StrEnum):
    A = "A"


class ValueEnumInt(IntEnum):
    A = 1


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
        # Test PostgreSQL's storage format for timestamp without timezone
        (
            """
            CREATE TABLE exampledbmodel (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP NOT NULL
            );
            """,
            [
                (
                    DBColumn(
                        table_name="exampledbmodel",
                        column_name="created_at",
                        column_type=ColumnType.TIMESTAMP_WITHOUT_TIME_ZONE,
                        column_is_list=False,
                        nullable=False,
                    ),
                    [
                        DBTable(table_name="exampledbmodel"),
                    ],
                )
            ],
        ),
        # Test PostgreSQL's storage format for timestamp with timezone
        (
            """
            CREATE TABLE exampledbmodel (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL
            );
            """,
            [
                (
                    DBColumn(
                        table_name="exampledbmodel",
                        column_name="created_at",
                        column_type=ColumnType.TIMESTAMP_WITH_TIME_ZONE,
                        column_is_list=False,
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


@pytest.mark.asyncio
async def test_db_serializer_foreign_key_actions(
    db_connection: DBConnection,
    clear_all_database_objects,
):
    """
    Test that foreign key ON UPDATE/ON DELETE actions are correctly deserialized from the database.
    """
    await db_connection.conn.execute(
        """
        CREATE TABLE foreignmodel (
            id SERIAL PRIMARY KEY
        );
        CREATE TABLE exampledbmodel (
            id SERIAL PRIMARY KEY,
            foreign_key_id INTEGER REFERENCES foreignmodel(id) ON DELETE CASCADE ON UPDATE CASCADE NOT NULL
        );
        """
    )

    db_serializer = DatabaseSerializer()
    db_objects = []
    async for values in db_serializer.get_objects(db_connection):
        db_objects.append(values)

    # Find the foreign key constraint
    fk_constraint = next(
        obj
        for obj, _ in db_objects
        if isinstance(obj, DBConstraint)
        and obj.constraint_type == ConstraintType.FOREIGN_KEY
    )
    assert fk_constraint.foreign_key_constraint is not None
    assert fk_constraint.foreign_key_constraint.target_table == "foreignmodel"
    assert fk_constraint.foreign_key_constraint.target_columns == frozenset({"id"})
    assert fk_constraint.foreign_key_constraint.on_delete == "CASCADE"
    assert fk_constraint.foreign_key_constraint.on_update == "CASCADE"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "index_definition, index_name, expected_columns",
    [
        # Simple DESC
        ("created_at DESC", "idx_desc", frozenset({"created_at"})),
        # Simple ASC
        ("created_at ASC", "idx_asc", frozenset({"created_at"})),
        # NULLS FIRST
        ("created_at NULLS FIRST", "idx_nulls_first", frozenset({"created_at"})),
        # NULLS LAST
        ("created_at NULLS LAST", "idx_nulls_last", frozenset({"created_at"})),
        # DESC with NULLS FIRST
        (
            "created_at DESC NULLS FIRST",
            "idx_desc_nulls_first",
            frozenset({"created_at"}),
        ),
        # ASC with NULLS LAST
        ("created_at ASC NULLS LAST", "idx_asc_nulls_last", frozenset({"created_at"})),
        # No modifier (baseline)
        ("created_at", "idx_no_modifier", frozenset({"created_at"})),
    ],
)
async def test_db_serializer_index_with_sort_direction(
    index_definition: str,
    index_name: str,
    expected_columns: frozenset[str],
    db_connection: DBConnection,
    clear_all_database_objects,
):
    """
    Test that indexes with sort direction modifiers (DESC, ASC, NULLS FIRST, NULLS LAST)
    are correctly deserialized from the database. The sort direction should be stripped
    from the column name.
    """
    await db_connection.conn.execute(
        f"""
        CREATE TABLE exampledbmodel (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX {index_name} ON exampledbmodel({index_definition});
        """
    )

    db_serializer = DatabaseSerializer()
    db_objects = []
    async for values in db_serializer.get_objects(db_connection):
        db_objects.append(values)

    # Find the index constraint
    index_constraint = next(
        obj
        for obj, deps in db_objects
        if isinstance(obj, DBConstraint)
        and obj.constraint_type == ConstraintType.INDEX
        and obj.constraint_name == index_name
    )

    # The column name should be "created_at", not "created_at DESC" etc.
    assert index_constraint.columns == expected_columns

    # Also check the dependencies reference the correct column name
    index_deps = next(
        deps
        for obj, deps in db_objects
        if isinstance(obj, DBConstraint) and obj.constraint_name == index_name
    )
    column_pointer = next(dep for dep in index_deps if isinstance(dep, DBColumnPointer))
    assert column_pointer.column_name == "created_at"


@pytest.mark.asyncio
async def test_db_serializer_index_with_multiple_columns_and_sort_directions(
    db_connection: DBConnection,
    clear_all_database_objects,
):
    """
    Test that indexes with multiple columns having different sort directions
    are correctly deserialized. Each column's sort direction should be stripped.
    """
    await db_connection.conn.execute(
        """
        CREATE TABLE exampledbmodel (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            name VARCHAR NOT NULL
        );
        CREATE INDEX idx_multi_col ON exampledbmodel(created_at DESC, updated_at ASC, name);
        """
    )

    db_serializer = DatabaseSerializer()
    db_objects = []
    async for values in db_serializer.get_objects(db_connection):
        db_objects.append(values)

    # Find the index constraint
    index_constraint = next(
        obj
        for obj, deps in db_objects
        if isinstance(obj, DBConstraint)
        and obj.constraint_type == ConstraintType.INDEX
        and obj.constraint_name == "idx_multi_col"
    )

    # All column names should be stripped of sort directions
    assert index_constraint.columns == frozenset({"created_at", "updated_at", "name"})

    # Check dependencies reference all correct column names
    index_deps = next(
        deps
        for obj, deps in db_objects
        if isinstance(obj, DBConstraint) and obj.constraint_name == "idx_multi_col"
    )
    column_pointers = [dep for dep in index_deps if isinstance(dep, DBColumnPointer)]
    column_names = {cp.column_name for cp in column_pointers}
    assert column_names == {"created_at", "updated_at", "name"}


@pytest.mark.asyncio
async def test_db_serializer_check_constraint(
    db_connection: DBConnection,
    clear_all_database_objects,
):
    """
    Test that CHECK constraints are correctly deserialized from the database.
    This tests the fix for the KeyError: 'oid' bug where pg_constraint.oid
    was not selected in the query.
    """
    await db_connection.conn.execute(
        """
        CREATE TABLE exampledbmodel (
            id SERIAL PRIMARY KEY,
            age INTEGER NOT NULL,
            CONSTRAINT age_positive CHECK (age > 0)
        );
        """
    )

    db_serializer = DatabaseSerializer()
    db_objects = []
    async for values in db_serializer.get_objects(db_connection):
        db_objects.append(values)

    # Find the check constraint
    check_constraint_obj = next(
        obj
        for obj, _ in db_objects
        if isinstance(obj, DBConstraint) and obj.constraint_type == ConstraintType.CHECK
    )
    assert check_constraint_obj.check_constraint is not None
    assert check_constraint_obj.constraint_name == "age_positive"
    # PostgreSQL returns the check condition in a normalized format
    assert "age > 0" in check_constraint_obj.check_constraint.check_condition
