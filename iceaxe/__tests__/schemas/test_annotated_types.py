import pytest
from typing import Annotated
from unittest.mock import ANY

from pydantic import EmailStr, create_model

from iceaxe import Field, TableBase
from iceaxe.schemas.actions import ColumnType, ConstraintType
from iceaxe.schemas.db_memory_serializer import DatabaseMemorySerializer
from iceaxe.schemas.db_stubs import DBColumn, DBConstraint, DBObject, DBObjectPointer, DBTable


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
async def test_annotated_email_str():
    """Test that Annotated[EmailStr] is correctly handled as a string type."""
    # Create a model with an EmailStr field
    EmailModel = create_model(
        "EmailModel",
        __base__=TableBase,
        id=(int, Field(primary_key=True)),
        email=(Annotated[str, EmailStr], Field()),
    )

    # Process the model with the serializer
    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([EmailModel]))

    # Expected objects: table, id column, email column, and primary key constraint
    expected_objects = [
        (
            DBTable(table_name="emailmodel"),
            [],
        ),
        (
            DBColumn(
                table_name="emailmodel",
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="emailmodel"),
            ],
        ),
        (
            DBColumn(
                table_name="emailmodel",
                column_name="email",
                column_type=ColumnType.VARCHAR,  # Should be treated as a string
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="emailmodel"),
            ],
        ),
        (
            DBConstraint(
                table_name="emailmodel",
                constraint_name="emailmodel_pkey",
                columns=frozenset({"id"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
                check_constraint=None,
            ),
            [
                DBTable(table_name="emailmodel"),
                DBColumn(
                    table_name="emailmodel",
                    column_name="email",
                    column_type=ColumnType.VARCHAR,
                    column_is_list=False,
                    nullable=False,
                ),
                DBColumn(
                    table_name="emailmodel",
                    column_name="id",
                    column_type=ColumnType.INTEGER,
                    column_is_list=False,
                    nullable=False,
                ),
            ],
        ),
    ]

    compare_db_objects(db_objects, expected_objects)


@pytest.mark.asyncio
async def test_direct_email_str():
    """Test that EmailStr directly is correctly handled as a string type."""
    # Create a model with an EmailStr field
    EmailModel = create_model(
        "EmailModel",
        __base__=TableBase,
        id=(int, Field(primary_key=True)),
        email=(EmailStr, Field()),
    )

    # Process the model with the serializer
    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([EmailModel]))

    # Expected objects: table, id column, email column, and primary key constraint
    expected_objects = [
        (
            DBTable(table_name="emailmodel"),
            [],
        ),
        (
            DBColumn(
                table_name="emailmodel",
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="emailmodel"),
            ],
        ),
        (
            DBColumn(
                table_name="emailmodel",
                column_name="email",
                column_type=ColumnType.VARCHAR,  # Should be treated as a string
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="emailmodel"),
            ],
        ),
        (
            DBConstraint(
                table_name="emailmodel",
                constraint_name="emailmodel_pkey",
                columns=frozenset({"id"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
                check_constraint=None,
            ),
            [
                DBTable(table_name="emailmodel"),
                DBColumn(
                    table_name="emailmodel",
                    column_name="email",
                    column_type=ColumnType.VARCHAR,
                    column_is_list=False,
                    nullable=False,
                ),
                DBColumn(
                    table_name="emailmodel",
                    column_name="id",
                    column_type=ColumnType.INTEGER,
                    column_is_list=False,
                    nullable=False,
                ),
            ],
        ),
    ]

    compare_db_objects(db_objects, expected_objects)


@pytest.mark.asyncio
async def test_nested_annotated_types():
    """Test that nested Annotated types are correctly handled."""
    # Create a model with a nested Annotated field
    NestedModel = create_model(
        "NestedModel",
        __base__=TableBase,
        id=(int, Field(primary_key=True)),
        value=(Annotated[Annotated[int, "metadata1"], "metadata2"], Field()),
    )

    # Process the model with the serializer
    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([NestedModel]))

    # Expected objects: table, id column, value column, and primary key constraint
    expected_objects = [
        (
            DBTable(table_name="nestedmodel"),
            [],
        ),
        (
            DBColumn(
                table_name="nestedmodel",
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="nestedmodel"),
            ],
        ),
        (
            DBColumn(
                table_name="nestedmodel",
                column_name="value",
                column_type=ColumnType.INTEGER,  # Should be treated as an integer
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="nestedmodel"),
            ],
        ),
        (
            DBConstraint(
                table_name="nestedmodel",
                constraint_name="nestedmodel_pkey",
                columns=frozenset({"id"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
                check_constraint=None,
            ),
            [
                DBTable(table_name="nestedmodel"),
                DBColumn(
                    table_name="nestedmodel",
                    column_name="id",
                    column_type=ColumnType.INTEGER,
                    column_is_list=False,
                    nullable=False,
                ),
                DBColumn(
                    table_name="nestedmodel",
                    column_name="value",
                    column_type=ColumnType.INTEGER,
                    column_is_list=False,
                    nullable=False,
                ),
            ],
        ),
    ]

    compare_db_objects(db_objects, expected_objects)


@pytest.mark.asyncio
async def test_list_of_annotated_types():
    """Test that a list of Annotated types is correctly handled."""
    # Create a model with a list of Annotated fields
    ListModel = create_model(
        "ListModel",
        __base__=TableBase,
        id=(int, Field(primary_key=True)),
        values=(list[Annotated[str, "metadata"]], Field(is_json=True)),
    )

    # Process the model with the serializer
    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([ListModel]))

    # Expected objects: table, id column, values column, and primary key constraint
    expected_objects = [
        (
            DBTable(table_name="listmodel"),
            [],
        ),
        (
            DBColumn(
                table_name="listmodel",
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="listmodel"),
            ],
        ),
        (
            DBColumn(
                table_name="listmodel",
                column_name="values",
                column_type=ColumnType.JSON,  # Should be treated as JSON
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="listmodel"),
            ],
        ),
        (
            DBConstraint(
                table_name="listmodel",
                constraint_name="listmodel_pkey",
                columns=frozenset({"id"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
                check_constraint=None,
            ),
            [
                DBTable(table_name="listmodel"),
                DBColumn(
                    table_name="listmodel",
                    column_name="id",
                    column_type=ColumnType.INTEGER,
                    column_is_list=False,
                    nullable=False,
                ),
                DBColumn(
                    table_name="listmodel",
                    column_name="values",
                    column_type=ColumnType.JSON,
                    column_is_list=False,
                    nullable=False,
                ),
            ],
        ),
    ]

    compare_db_objects(db_objects, expected_objects) 