from unittest.mock import AsyncMock

import asyncpg
import pytest

from iceaxe.schemas.actions import (
    CheckConstraint,
    ColumnType,
    ConstraintType,
    DatabaseActions,
    DryRunAction,
    ForeignKeyConstraint,
    assert_is_safe_sql_identifier,
    format_sql_values,
)
from iceaxe.session import DBConnection


@pytest.fixture
def db_backed_actions(
    db_connection: DBConnection,
    clear_all_database_objects,
):
    """
    Fixture that should be used for actions that should actually be executed
    against a database. We will clear all database objects before and after
    the test, so no backed objects will be available.

    """
    return DatabaseActions(dry_run=False, db_connection=db_connection)


def example_action_fn(arg_1: str):
    pass


@pytest.mark.asyncio
async def test_record_signature_dry_run():
    database_actions = DatabaseActions(dry_run=True)

    await database_actions._record_signature(
        example_action_fn, {"arg_1": "test"}, "SQL"
    )

    assert database_actions.dry_run_actions == [
        DryRunAction(fn=example_action_fn, kwargs={"arg_1": "test"})
    ]
    assert database_actions.prod_sqls == []


@pytest.mark.asyncio
async def test_record_signature_prod():
    database_actions = DatabaseActions(dry_run=False, db_connection=AsyncMock())

    await database_actions._record_signature(
        example_action_fn, {"arg_1": "test"}, "SQL"
    )

    assert database_actions.dry_run_actions == []
    assert database_actions.prod_sqls == ["SQL"]


@pytest.mark.asyncio
async def test_record_signature_incorrect_kwarg():
    database_actions = DatabaseActions(dry_run=False, db_connection=AsyncMock())

    # An extra, non-existent kwarg is provided
    with pytest.raises(ValueError):
        await database_actions._record_signature(
            example_action_fn, {"arg_1": "test", "arg_2": "test"}, "SQL"
        )

    # A required kwarg is missing
    with pytest.raises(ValueError):
        await database_actions._record_signature(example_action_fn, {}, "SQL")


@pytest.mark.parametrize(
    "identifier, expected_is_valid",
    [
        # Valid identifiers
        ("validTableName", True),
        ("_valid_table_name", True),
        ("Table123", True),
        ("_", True),
        ("t", True),
        # Invalid identifiers
        ("123table", False),
        ("table-name", False),
        ("table name", False),
        ("table$name", False),
        ("table!name", False),
        ("table@name", False),
        ("table#name", False),
        ("", False),
        (" ", False),
        (" table", False),
        ("table ", False),
        ("table\n", False),
        # SQL injection attempts
        ("table; DROP TABLE users;", False),
        ("table; SELECT * FROM users", False),
        ("1;1", False),
        (";", False),
        ("--comment", False),
        ("' OR '1'='1", False),
        ('" OR "1"="1', False),
        ("table`", False),
        ("[table]", False),
        ("{table}", False),
        ("<script>", False),
        ('"; DROP TABLE users; --', False),
        ("'; DROP TABLE users; --", False),
    ],
)
def test_is_safe_sql_identifier(identifier: str, expected_is_valid: bool):
    if expected_is_valid:
        assert_is_safe_sql_identifier(identifier)
    else:
        with pytest.raises(ValueError):
            assert_is_safe_sql_identifier(identifier)


@pytest.mark.parametrize(
    "values, expected",
    [
        # Simple strings without special characters
        (["single"], "'single'"),
        ([], ""),
        (["apple", "banana"], "'apple', 'banana'"),
        # Strings with single quotes that need escaping
        (["O'Neill", "d'Artagnan"], "'O''Neill', 'd''Artagnan'"),
        # Mixed strings, no special characters and with special characters
        (["hello", "it's a test"], "'hello', 'it''s a test'"),
        # Strings that contain SQL-like syntax
        (
            ["SELECT * FROM users;", "DROP TABLE students;"],
            "'SELECT * FROM users;', 'DROP TABLE students;'",
        ),
        # Empty strings and spaces
        (["", " ", "   "], "'', ' ', '   '"),
    ],
)
def test_format_sql_values(values, expected):
    assert format_sql_values(values) == expected


@pytest.mark.asyncio
async def test_add_table(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    await db_backed_actions.add_table("test_table")

    # We should have a table in the database
    assert await db_connection.conn.execute("SELECT * FROM test_table")


@pytest.mark.asyncio
async def test_add_table_reserved_keyword(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    """
    Confirm that table migrations will wrap the table name in double quotes
    to avoid conflicts with reserved keywords.

    """
    await db_backed_actions.add_table("user")

    # We should have a table in the database
    assert await db_connection.conn.execute("SELECT * FROM user")


@pytest.mark.asyncio
async def test_drop_table(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Set up a table for us to drop first
    await db_connection.conn.execute("CREATE TABLE test_table (id SERIAL PRIMARY KEY)")

    await db_backed_actions.drop_table("test_table")

    # We should not have a table in the database
    with pytest.raises(asyncpg.exceptions.UndefinedTableError):
        await db_connection.conn.execute("SELECT * FROM test_table")


@pytest.mark.asyncio
async def test_add_column(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Set up a table for us to drop first
    await db_connection.conn.execute("CREATE TABLE test_table (id SERIAL PRIMARY KEY)")

    # Standard type
    await db_backed_actions.add_column(
        "test_table",
        "test_column",
        explicit_data_type=ColumnType.VARCHAR,
    )

    # Standard, list type
    await db_backed_actions.add_column(
        "test_table",
        "test_column_list",
        explicit_data_type=ColumnType.VARCHAR,
        explicit_data_is_list=True,
    )

    # We should now have columns in the table
    # Insert an object with the expected columns
    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column, test_column_list) VALUES ($1, $2)",
        "test_value",
        ["value_1", "value_2"],
    )

    # Make sure that we can retrieve the object
    rows = await db_connection.conn.fetch("SELECT * FROM test_table")
    row = rows[0]
    assert row
    assert row["test_column"] == "test_value"
    assert row["test_column_list"] == ["value_1", "value_2"]


@pytest.mark.asyncio
@pytest.mark.parametrize("enum_value", [value for value in ColumnType])
async def test_add_column_any_type(
    enum_value: ColumnType,
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    """
    Simple test that all our known type enum values are formatted properly
    to be inserted into the database, since we don't otherwise validate insertion
    values here.

    """
    # Set up a table for us to drop first
    await db_connection.conn.execute("CREATE TABLE test_table (id SERIAL PRIMARY KEY)")

    await db_backed_actions.add_column(
        "test_table",
        "test_column",
        explicit_data_type=enum_value,
    )

    # Query the postgres index to see if the column was created
    rows = await db_connection.conn.fetch(
        "SELECT data_type FROM information_schema.columns WHERE table_name = 'test_table' AND column_name = 'test_column'"
    )
    row = rows[0]

    # Some values are shortcuts for other values when inserted without
    # additional parameters. We keep track of that mapping here so we allow
    # some flexibility when checking the expected value.
    # (inserted, allowed alternative value in database)
    known_equivalents = (
        (ColumnType.DECIMAL, ColumnType.NUMERIC),
        (ColumnType.SERIAL, ColumnType.INTEGER),
        (ColumnType.BIGSERIAL, ColumnType.BIGINT),
        (ColumnType.SMALLSERIAL, ColumnType.SMALLINT),
        (ColumnType.CHAR, "character"),
        (ColumnType.TIME_WITHOUT_TIME_ZONE, "time without time zone"),
        (ColumnType.TIMESTAMP_WITHOUT_TIME_ZONE, "timestamp without time zone"),
    )

    allowed_values = {enum_value.value}
    for known_value, alternative in known_equivalents:
        if enum_value == known_value:
            allowed_values.add(alternative)

    assert row
    assert row["data_type"] in allowed_values


@pytest.mark.asyncio
async def test_drop_column(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Set up a table for us to drop first
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column VARCHAR)"
    )

    await db_backed_actions.drop_column("test_table", "test_column")

    # We should not have a column in the table
    with pytest.raises(asyncpg.exceptions.UndefinedColumnError):
        await db_connection.conn.execute("SELECT test_column FROM test_table")


@pytest.mark.asyncio
async def test_rename_column(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Set up a table for us to drop first
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column VARCHAR)"
    )

    await db_backed_actions.rename_column("test_table", "test_column", "new_column")

    # We should have a column in the table
    assert await db_connection.conn.execute("SELECT new_column FROM test_table")

    # We should not have a column in the table
    with pytest.raises(asyncpg.exceptions.UndefinedColumnError):
        await db_connection.conn.execute("SELECT test_column FROM test_table")


@pytest.mark.asyncio
async def test_modify_column_type(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Set up a table with the old types
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column VARCHAR)"
    )

    # Modify the column type from VARCHAR to TEXT, which is a compatible change
    # that doesn't require explicit casting
    await db_backed_actions.modify_column_type(
        "test_table", "test_column", ColumnType.TEXT
    )

    # We should now be able to inject a text value
    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column) VALUES ($1)",
        "test_string_value",
    )

    # Make sure that we can retrieve the object
    rows = await db_connection.conn.fetch("SELECT * FROM test_table")
    row = rows[0]
    assert row
    assert row["test_column"] == "test_string_value"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "from_type,to_type,test_value,expected_value,requires_autocast",
    [
        # String conversions
        (ColumnType.VARCHAR, ColumnType.TEXT, "test", "test", False),
        (ColumnType.TEXT, ColumnType.VARCHAR, "test", "test", False),
        # Numeric conversions - these require autocast
        (ColumnType.VARCHAR, ColumnType.INTEGER, "123", 123, True),
        (ColumnType.TEXT, ColumnType.INTEGER, "456", 456, True),
        (ColumnType.INTEGER, ColumnType.BIGINT, 123, 123, False),
        (ColumnType.INTEGER, ColumnType.SMALLINT, 50, 50, False),
        (ColumnType.SMALLINT, ColumnType.INTEGER, 50, 50, False),
        (ColumnType.INTEGER, ColumnType.REAL, 123, 123.0, False),
        (ColumnType.REAL, ColumnType.DOUBLE_PRECISION, 123.5, 123.5, False),
        # Boolean conversions - require autocast
        (ColumnType.VARCHAR, ColumnType.BOOLEAN, "true", True, True),
        (ColumnType.TEXT, ColumnType.BOOLEAN, "false", False, True),
        (ColumnType.INTEGER, ColumnType.BOOLEAN, 1, True, True),
        # Timestamp conversions - require autocast for string sources
        (ColumnType.VARCHAR, ColumnType.DATE, "2023-01-01", "2023-01-01", True),
        (
            ColumnType.TEXT,
            ColumnType.TIMESTAMP_WITHOUT_TIME_ZONE,
            "2023-01-01 12:00:00",
            "2023-01-01 12:00:00",
            True,
        ),
        # JSON conversions - require autocast, return as strings
        (
            ColumnType.TEXT,
            ColumnType.JSON,
            '{"key": "value"}',
            '{"key": "value"}',
            True,
        ),
        (
            ColumnType.VARCHAR,
            ColumnType.JSONB,
            '{"key": "value"}',
            '{"key": "value"}',
            True,
        ),
        (
            ColumnType.JSON,
            ColumnType.JSONB,
            '{"key": "value"}',
            '{"key": "value"}',
            False,
        ),
    ],
)
async def test_modify_column_type_with_autocast(
    from_type: ColumnType,
    to_type: ColumnType,
    test_value,
    expected_value,
    requires_autocast: bool,
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    """
    Test column type modifications with autocast for various type conversions.
    """
    table_name = "test_table_autocast"
    column_name = "test_column"

    # Create table with source type - handle special cases
    if from_type == ColumnType.CHAR:
        # CHAR needs a length specifier
        type_spec = f"{from_type.value}(10)"
    else:
        type_spec = from_type.value

    await db_connection.conn.execute(
        f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {column_name} {type_spec})"
    )

    # Insert test data
    await db_connection.conn.execute(
        f"INSERT INTO {table_name} ({column_name}) VALUES ($1)",
        test_value,
    )

    # Modify column type with autocast if required
    await db_backed_actions.modify_column_type(
        table_name,
        column_name,
        explicit_data_type=to_type,
        autocast=requires_autocast,
    )

    # Verify the conversion worked
    rows = await db_connection.conn.fetch(f"SELECT * FROM {table_name}")
    row = rows[0]
    assert row

    # Handle different expected value types
    actual_value = row[column_name]
    if isinstance(expected_value, str) and to_type in [
        ColumnType.DATE,
        ColumnType.TIMESTAMP_WITHOUT_TIME_ZONE,
    ]:
        # For date/timestamp, convert to string for comparison
        actual_value = str(actual_value)

    assert actual_value == expected_value


@pytest.mark.asyncio
async def test_modify_column_type_autocast_without_data(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    """
    Test that autocast works even when there's no data in the column.
    """
    table_name = "test_table_empty"
    column_name = "test_column"

    # Create table with VARCHAR
    await db_connection.conn.execute(
        f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {column_name} VARCHAR)"
    )

    # Convert to INTEGER with autocast (should work even with no data)
    await db_backed_actions.modify_column_type(
        table_name,
        column_name,
        explicit_data_type=ColumnType.INTEGER,
        autocast=True,
    )

    # Insert integer data to verify it works
    await db_connection.conn.execute(
        f"INSERT INTO {table_name} ({column_name}) VALUES ($1)",
        42,
    )

    rows = await db_connection.conn.fetch(f"SELECT * FROM {table_name}")
    row = rows[0]
    assert row
    assert row[column_name] == 42


@pytest.mark.asyncio
async def test_modify_column_type_incompatible_without_autocast_fails(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    """
    Test that incompatible type changes fail without autocast.
    """
    table_name = "test_table_fail"
    column_name = "test_column"

    # Create table with VARCHAR containing non-numeric data
    await db_connection.conn.execute(
        f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {column_name} VARCHAR)"
    )

    await db_connection.conn.execute(
        f"INSERT INTO {table_name} ({column_name}) VALUES ($1)",
        "not_a_number",
    )

    # Attempt to convert to INTEGER without autocast should fail
    with pytest.raises(Exception):  # Should be DatatypeMismatchError
        await db_backed_actions.modify_column_type(
            table_name,
            column_name,
            explicit_data_type=ColumnType.INTEGER,
            autocast=False,
        )


@pytest.mark.asyncio
async def test_modify_column_type_custom_type_with_autocast(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    """
    Test autocast with custom types (enums).
    """
    table_name = "test_table_custom"
    column_name = "test_column"
    enum_name = "test_enum"

    # Create enum type
    await db_connection.conn.execute(f"CREATE TYPE {enum_name} AS ENUM ('A', 'B', 'C')")

    # Create table with VARCHAR
    await db_connection.conn.execute(
        f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {column_name} VARCHAR)"
    )

    # Insert enum-compatible string
    await db_connection.conn.execute(
        f"INSERT INTO {table_name} ({column_name}) VALUES ($1)",
        "A",
    )

    # Convert to custom enum type with autocast
    await db_backed_actions.modify_column_type(
        table_name,
        column_name,
        custom_data_type=enum_name,
        autocast=True,
    )

    # Verify the conversion worked
    rows = await db_connection.conn.fetch(f"SELECT * FROM {table_name}")
    row = rows[0]
    assert row
    assert row[column_name] == "A"

    # Verify the column type is now the custom enum
    type_info = await db_connection.conn.fetch(
        """
        SELECT data_type, udt_name 
        FROM information_schema.columns 
        WHERE table_name = $1 AND column_name = $2
        """,
        table_name,
        column_name,
    )
    assert type_info[0]["udt_name"] == enum_name


@pytest.mark.asyncio
async def test_add_constraint_foreign_key(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Set up two tables since we need a table target
    await db_connection.conn.execute(
        "CREATE TABLE external_table (id SERIAL PRIMARY KEY, external_column VARCHAR)"
    )
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column_id INTEGER)"
    )

    # Insert an existing object into the external table
    await db_connection.conn.execute(
        "INSERT INTO external_table (id, external_column) VALUES ($1, $2)",
        1,
        "test_value",
    )

    # Add a foreign_key
    await db_backed_actions.add_constraint(
        "test_table",
        ["test_column_id"],
        ConstraintType.FOREIGN_KEY,
        "test_foreign_key_constraint",
        constraint_args=ForeignKeyConstraint(
            target_table="external_table",
            target_columns=frozenset({"id"}),
        ),
    )

    # We should now have a foreign key constraint
    # Insert an object that links to our known external object
    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column_id) VALUES ($1)",
        1,
    )

    # We should not be able to insert an object that does not link to the external object
    with pytest.raises(asyncpg.exceptions.ForeignKeyViolationError):
        await db_connection.conn.execute(
            "INSERT INTO test_table (test_column_id) VALUES ($1)",
            2,
        )


@pytest.mark.asyncio
async def test_add_constraint_unique(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Add the table that should have a unique column
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column VARCHAR)"
    )

    # Add a unique constraint
    await db_backed_actions.add_constraint(
        "test_table",
        ["test_column"],
        ConstraintType.UNIQUE,
        "test_unique_constraint",
    )

    # We should now have a unique constraint, make sure that we can't
    # insert the same value twice
    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column) VALUES ($1)",
        "test_value",
    )

    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await db_connection.conn.execute(
            "INSERT INTO test_table (test_column) VALUES ($1)",
            "test_value",
        )


@pytest.mark.asyncio
async def test_add_constraint_primary_key(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Create an empty table to simulate one just created
    await db_connection.conn.execute("CREATE TABLE test_table ()")

    # Add a new column
    await db_backed_actions.add_column("test_table", "test_column", ColumnType.INTEGER)

    # Promote the column to a primary key
    await db_backed_actions.add_constraint(
        "test_table",
        ["test_column"],
        ConstraintType.PRIMARY_KEY,
        "test_primary_key_constraint",
    )

    # We should now have a primary key constraint, make sure that we can insert
    # a value into the column
    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column) VALUES ($1)",
        1,
    )

    # We should not be able to insert a duplicate primary key value
    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await db_connection.conn.execute(
            "INSERT INTO test_table (test_column) VALUES ($1)",
            1,
        )


@pytest.mark.asyncio
async def test_add_constraint_check(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Create a table with a integer price column
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, price INTEGER)"
    )

    # Now we add a check constraint that this price column should be positive
    await db_backed_actions.add_constraint(
        "test_table",
        [],
        ConstraintType.CHECK,
        "test_check_constraint",
        constraint_args=CheckConstraint(check_condition="price > 0"),
    )

    # Make sure that we can insert a positive value
    await db_connection.conn.execute(
        "INSERT INTO test_table (price) VALUES ($1)",
        1,
    )

    # We expect negative values to fail
    with pytest.raises(asyncpg.exceptions.CheckViolationError):
        await db_connection.conn.execute(
            "INSERT INTO test_table (price) VALUES ($1)",
            -1,
        )


@pytest.mark.asyncio
async def test_drop_constraint(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Manually create a table with a unique constraint
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column VARCHAR)"
    )
    await db_connection.conn.execute(
        "ALTER TABLE test_table ADD CONSTRAINT test_unique_constraint UNIQUE (test_column)"
    )

    # Drop the unique constraint
    await db_backed_actions.drop_constraint("test_table", "test_unique_constraint")

    # We should now be able to insert the same value twice
    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column) VALUES ($1)",
        "test_value",
    )

    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column) VALUES ($1)",
        "test_value",
    )


@pytest.mark.asyncio
async def test_add_not_null(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Create a table with a nullable column (default behavior for fields)
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column VARCHAR)"
    )

    await db_backed_actions.add_not_null("test_table", "test_column")

    # We should now have a not null constraint, make sure that we can't insert a null value
    with pytest.raises(asyncpg.exceptions.NotNullViolationError):
        await db_connection.conn.execute(
            "INSERT INTO test_table (test_column) VALUES ($1)",
            None,
        )


@pytest.mark.asyncio
async def test_drop_not_null(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Create a table with a not null column
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column VARCHAR NOT NULL)"
    )

    await db_backed_actions.drop_not_null("test_table", "test_column")

    # We should now be able to insert a null value
    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column) VALUES ($1)",
        None,
    )


@pytest.mark.asyncio
async def test_add_type(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    await db_backed_actions.add_type("test_type", ["A", "B"])

    # Create a new table with a column of the new type
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column test_type)"
    )

    # We should be able to insert values that match this type
    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column) VALUES ($1)",
        "A",
    )

    # Values not in the enum type definition should fail during insertion
    with pytest.raises(asyncpg.exceptions.InvalidTextRepresentationError):
        await db_connection.conn.execute(
            "INSERT INTO test_table (test_column) VALUES ($1)",
            "C",
        )


@pytest.mark.asyncio
async def test_add_type_values(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Create an existing enum
    await db_connection.conn.execute("CREATE TYPE test_type AS ENUM ('A')")

    # Create a table that uses this enum
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column test_type)"
    )

    # Add a new value to this enum
    await db_backed_actions.add_type_values("test_type", ["B"])

    # We should be able to insert values that match this type
    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column) VALUES ($1)",
        "B",
    )


@pytest.mark.asyncio
async def test_drop_type_values_no_existing_references(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Create an existing enum with two values
    await db_connection.conn.execute("CREATE TYPE test_type AS ENUM ('A', 'B')")

    # Drop a value from this enum
    await db_backed_actions.drop_type_values("test_type", ["B"], target_columns=[])

    # Create a table that uses this enum
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column test_type)"
    )

    # Fetch the values for the enum that are currently in use
    result = await db_connection.conn.fetch(
        "SELECT array_agg(unnest) AS value FROM unnest(enum_range(NULL::test_type))"
    )
    current_values = result[0]
    assert current_values["value"] == ["A"]


@pytest.mark.asyncio
async def test_drop_type_values(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Create an existing enum with two values
    await db_connection.conn.execute("CREATE TYPE test_type AS ENUM ('A', 'B')")

    # Create a table that uses this enum
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column test_type)"
    )

    # Drop a value from this enum
    await db_backed_actions.drop_type_values(
        "test_type", ["B"], target_columns=[("test_table", "test_column")]
    )

    # Fetch the values for the enum that are currently in use
    result = await db_connection.conn.fetch(
        "SELECT array_agg(unnest) AS value FROM unnest(enum_range(NULL::test_type))"
    )
    current_values = result[0]
    assert current_values["value"] == ["A"]

    # We should be able to insert values that match A but not B
    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column) VALUES ($1)",
        "A",
    )

    with pytest.raises(asyncpg.exceptions.InvalidTextRepresentationError):
        await db_connection.conn.execute(
            "INSERT INTO test_table (test_column) VALUES ($1)",
            "B",
        )


@pytest.mark.asyncio
async def test_drop_type(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Create a new type
    await db_connection.conn.execute("CREATE TYPE test_type AS ENUM ('A')")

    # Drop this type
    await db_backed_actions.drop_type("test_type")

    # We shouldn't be able to create a table with this type
    with pytest.raises(asyncpg.exceptions.UndefinedObjectError):
        await db_connection.conn.execute(
            "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column test_type)"
        )


@pytest.mark.asyncio
async def test_add_index_single_column(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Create a table with a column to index
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column VARCHAR)"
    )

    # Add an index on a single column
    await db_backed_actions.add_index(
        "test_table",
        ["test_column"],
        "test_single_column_index",
    )

    # Verify the index exists by querying the system catalog
    result = await db_connection.conn.fetch(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'test_table'
        AND indexname = 'test_single_column_index'
        """
    )
    assert len(result) == 1
    assert (
        "CREATE INDEX test_single_column_index ON public.test_table USING btree (test_column)"
        in result[0]["indexdef"]
    )


@pytest.mark.asyncio
async def test_add_index_multiple_columns(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Create a table with multiple columns to index
    await db_connection.conn.execute(
        """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            first_column VARCHAR,
            second_column INTEGER
        )
        """
    )

    # Add an index on multiple columns
    await db_backed_actions.add_index(
        "test_table",
        ["first_column", "second_column"],
        "test_multi_column_index",
    )

    # Verify the index exists and includes both columns
    result = await db_connection.conn.fetch(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'test_table'
        AND indexname = 'test_multi_column_index'
        """
    )
    assert len(result) == 1
    assert (
        "CREATE INDEX test_multi_column_index ON public.test_table USING btree (first_column, second_column)"
        in result[0]["indexdef"]
    )


@pytest.mark.asyncio
async def test_drop_index(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Create a table and add an index
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column VARCHAR)"
    )
    await db_connection.conn.execute(
        "CREATE INDEX test_index ON test_table (test_column)"
    )

    # Verify the index exists before dropping
    result = await db_connection.conn.fetch(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE tablename = 'test_table'
        AND indexname = 'test_index'
        """
    )
    assert len(result) == 1

    # Drop the index
    await db_backed_actions.drop_index("test_table", "test_index")

    # Verify the index no longer exists
    result = await db_connection.conn.fetch(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE tablename = 'test_table'
        AND indexname = 'test_index'
        """
    )
    assert len(result) == 0


@pytest.mark.asyncio
async def test_add_constraint_foreign_key_actions(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    # Set up two tables since we need a table target
    await db_connection.conn.execute(
        "CREATE TABLE external_table (id SERIAL PRIMARY KEY, external_column VARCHAR)"
    )
    await db_connection.conn.execute(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, test_column_id INTEGER)"
    )

    # Insert an existing object into the external table
    await db_connection.conn.execute(
        "INSERT INTO external_table (id, external_column) VALUES ($1, $2)",
        1,
        "test_value",
    )

    # Add a foreign_key with CASCADE actions
    await db_backed_actions.add_constraint(
        "test_table",
        ["test_column_id"],
        ConstraintType.FOREIGN_KEY,
        "test_foreign_key_constraint",
        constraint_args=ForeignKeyConstraint(
            target_table="external_table",
            target_columns=frozenset({"id"}),
            on_delete="CASCADE",
            on_update="CASCADE",
        ),
    )

    # Insert a row that references the external table
    await db_connection.conn.execute(
        "INSERT INTO test_table (test_column_id) VALUES ($1)",
        1,
    )

    # Update the external table id - should cascade to test_table
    await db_connection.conn.execute(
        "UPDATE external_table SET id = $1 WHERE id = $2",
        2,
        1,
    )

    # Verify the update cascaded
    result = await db_connection.conn.fetch(
        "SELECT test_column_id FROM test_table WHERE id = 1"
    )
    assert result[0]["test_column_id"] == 2

    # Delete from external table - should cascade to test_table
    await db_connection.conn.execute(
        "DELETE FROM external_table WHERE id = $1",
        2,
    )

    # Verify the delete cascaded
    result = await db_connection.conn.fetch("SELECT COUNT(*) FROM test_table")
    assert result[0]["count"] == 0


@pytest.mark.asyncio
async def test_modify_column_type_uuid_conversion(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    """
    Test UUID type conversions specifically.
    """
    table_name = "test_table_uuid"
    column_name = "test_column"
    uuid_string = "550e8400-e29b-41d4-a716-446655440000"

    # Create table with VARCHAR
    await db_connection.conn.execute(
        f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {column_name} VARCHAR)"
    )

    # Insert UUID string
    await db_connection.conn.execute(
        f"INSERT INTO {table_name} ({column_name}) VALUES ($1)",
        uuid_string,
    )

    # Convert to UUID with autocast
    await db_backed_actions.modify_column_type(
        table_name,
        column_name,
        explicit_data_type=ColumnType.UUID,
        autocast=True,
    )

    # Verify the conversion worked
    rows = await db_connection.conn.fetch(f"SELECT * FROM {table_name}")
    row = rows[0]
    assert row

    # UUID columns return UUID objects
    actual_value = row[column_name]
    assert str(actual_value) == uuid_string


@pytest.mark.asyncio
async def test_modify_column_type_date_to_timestamp(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    """
    Test date to timestamp conversions.
    """
    from datetime import date, datetime

    table_name = "test_table_date"
    column_name = "test_column"
    test_date = date(2023, 1, 1)

    # Create table with DATE
    await db_connection.conn.execute(
        f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {column_name} DATE)"
    )

    # Insert date value
    await db_connection.conn.execute(
        f"INSERT INTO {table_name} ({column_name}) VALUES ($1)",
        test_date,
    )

    # Convert to TIMESTAMP (no autocast needed for compatible types)
    await db_backed_actions.modify_column_type(
        table_name,
        column_name,
        explicit_data_type=ColumnType.TIMESTAMP_WITHOUT_TIME_ZONE,
        autocast=False,
    )

    # Verify the conversion worked
    rows = await db_connection.conn.fetch(f"SELECT * FROM {table_name}")
    row = rows[0]
    assert row

    # Should be a datetime object now
    actual_value = row[column_name]
    assert isinstance(actual_value, datetime)
    assert actual_value.date() == test_date


@pytest.mark.asyncio
async def test_modify_column_type_char_to_varchar(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    """
    Test CHAR to VARCHAR conversion with proper length handling.
    """
    table_name = "test_table_char"
    column_name = "test_column"
    test_value = "test"

    # Create table with CHAR(10)
    await db_connection.conn.execute(
        f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {column_name} CHAR(10))"
    )

    # Insert test value
    await db_connection.conn.execute(
        f"INSERT INTO {table_name} ({column_name}) VALUES ($1)",
        test_value,
    )

    # Convert to VARCHAR
    await db_backed_actions.modify_column_type(
        table_name,
        column_name,
        explicit_data_type=ColumnType.VARCHAR,
        autocast=False,
    )

    # Verify the conversion worked
    rows = await db_connection.conn.fetch(f"SELECT * FROM {table_name}")
    row = rows[0]
    assert row

    # CHAR pads with spaces, VARCHAR should trim them
    actual_value = row[column_name]
    assert actual_value.strip() == test_value


@pytest.mark.asyncio
async def test_modify_column_type_scalar_to_array(
    db_backed_actions: DatabaseActions,
    db_connection: DBConnection,
):
    """
    Test converting a scalar column to an array column with autocast.
    """
    table_name = "test_table_array_conversion"
    column_name = "test_column"

    # Create table with INTEGER
    await db_connection.conn.execute(
        f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {column_name} INTEGER)"
    )

    # Insert a scalar value
    await db_connection.conn.execute(
        f"INSERT INTO {table_name} ({column_name}) VALUES ($1)",
        42,
    )

    # Convert to INTEGER[] using autocast
    await db_backed_actions.modify_column_type(
        table_name,
        column_name,
        explicit_data_type=ColumnType.INTEGER,
        explicit_data_is_list=True,
        autocast=True,
    )

    # Verify the scalar value was converted to a single-element array
    rows = await db_connection.conn.fetch(f"SELECT * FROM {table_name}")
    row = rows[0]
    assert row
    assert row[column_name] == [42]

    # Verify the column type is now an array by checking the PostgreSQL catalog
    type_info = await db_connection.conn.fetch(
        """
        SELECT data_type, udt_name 
        FROM information_schema.columns 
        WHERE table_name = $1 AND column_name = $2
        """,
        table_name,
        column_name,
    )
    assert type_info[0]["data_type"] == "ARRAY"
