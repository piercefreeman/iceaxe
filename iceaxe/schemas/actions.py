from dataclasses import dataclass
from inspect import Parameter, signature
from re import fullmatch as re_fullmatch
from typing import Any, Callable, Literal, overload

from pydantic import BaseModel

from iceaxe.logging import LOGGER
from iceaxe.postgres import ForeignKeyModifications
from iceaxe.queries_str import QueryIdentifier
from iceaxe.session import DBConnection
from iceaxe.sql_types import ColumnType, ConstraintType


class ForeignKeyConstraint(BaseModel):
    target_table: str
    target_columns: frozenset[str]
    on_delete: ForeignKeyModifications = "NO ACTION"
    on_update: ForeignKeyModifications = "NO ACTION"

    model_config = {
        "frozen": True,
    }


class CheckConstraint(BaseModel):
    check_condition: str

    model_config = {
        "frozen": True,
    }


class ExcludeConstraint(BaseModel):
    exclude_operator: str

    model_config = {
        "frozen": True,
    }


@dataclass
class DryRunAction:
    fn: Callable
    kwargs: dict[str, Any]


@dataclass
class DryRunComment:
    text: str
    previous_line: bool = False


def assert_is_safe_sql_identifier(identifier: str):
    """
    Check if the provided identifier is a safe SQL identifier. Since our code
    pulls these directly from the definitions, there shouldn't
    be any issues with SQL injection, but it's good to be safe.

    """
    is_valid = re_fullmatch(r"^[A-Za-z_][A-Za-z0-9_]*$", identifier) is not None
    if not is_valid:
        raise ValueError(f"{identifier} is not a valid SQL identifier.")


def format_sql_values(values: list[str]):
    """
    Safely formats string values for SQL insertion by escaping single quotes.

    """
    escaped_values = [
        value.replace("'", "''") for value in values
    ]  # Escaping single quotes in SQL
    formatted_values = ", ".join(f"'{value}'" for value in escaped_values)
    return formatted_values


class DatabaseActions:
    """
    Track the actions that need to be executed to the database. Provides
    a shallow, typed ORM on top of the raw SQL commands that we'll execute
    through asyncpg.

    This class manually builds up the SQL strings that will be executed against
    postgres. We intentionally avoid using the ORM or variable-insertion modes
    here because most table-schema operations don't permit parameters to
    specify top-level SQL syntax. To keep things consistent, we'll use the
    same SQL string interpolation for all operations.

    """

    dry_run: bool
    """
    If True, the actions will be recorded but not executed. This is used
    internally within Iceaxe to generate a typehinted list of actions that will
    be inserted into the migration files without actually running the logic.

    """

    dry_run_actions: list[DryRunAction | DryRunComment]
    """
    A list of actions that will be executed. Each arg/kwarg passed to our action
    functions during the dryrun will be recorded here.

    """

    prod_sqls: list[str]
    """
    A list of SQL strings that will be executed against the database. This is
    only populated when dry_run is False.

    """

    def __init__(
        self,
        dry_run: bool = True,
        db_connection: DBConnection | None = None,
    ):
        self.dry_run = dry_run

        if not dry_run:
            if db_connection is None:
                raise ValueError(
                    "Must provide a db_connection when not in dry run mode."
                )

        self.dry_run_actions: list[DryRunAction | DryRunComment] = []
        self.db_connection = db_connection
        self.prod_sqls: list[str] = []

    async def add_table(self, table_name: str):
        """
        Create a new table in the database.

        """
        assert_is_safe_sql_identifier(table_name)
        table = QueryIdentifier(table_name)

        await self._record_signature(
            self.add_table,
            dict(table_name=table_name),
            f"""
            CREATE TABLE {table} ();
            """,
        )

    async def drop_table(self, table_name: str):
        """
        Delete a table and all its contents from the database. This is
        a destructive action, all data in the table will be lost.

        """
        assert_is_safe_sql_identifier(table_name)
        table = QueryIdentifier(table_name)

        await self._record_signature(
            self.drop_table,
            dict(table_name=table_name),
            f"""
            DROP TABLE {table}
            """,
        )

    async def add_column(
        self,
        table_name: str,
        column_name: str,
        explicit_data_type: ColumnType | None = None,
        explicit_data_is_list: bool = False,
        custom_data_type: str | None = None,
    ):
        """
        Add a new column to a table.

        :param table_name: The name of the table to add the column to.
        :param column_name: The name of the column to add.
        :param explicit_data_type: The explicit data type of the column.
        :param explicit_data_is_list: Whether the explicit data type is a list.
        :param custom_data_type: A custom data type for the column, like an enum
            that's registered in Postgres.

        """

        if not explicit_data_type and not custom_data_type:
            raise ValueError(
                "Must provide either an explicit data type or a custom data type."
            )
        if explicit_data_type and custom_data_type:
            raise ValueError(
                "Cannot provide both an explicit data type and a custom data type."
            )

        assert_is_safe_sql_identifier(table_name)
        assert_is_safe_sql_identifier(column_name)

        # We only need to check the custom data type, since we know
        # the explicit data types come from the enum and are safe.
        if custom_data_type:
            assert_is_safe_sql_identifier(custom_data_type)

        table = QueryIdentifier(table_name)
        column = QueryIdentifier(column_name)

        column_type = self._get_column_type(
            explicit_data_type=explicit_data_type,
            explicit_data_is_list=explicit_data_is_list,
            custom_data_type=custom_data_type,
        )

        await self._record_signature(
            self.add_column,
            dict(
                table_name=table_name,
                column_name=column_name,
                explicit_data_type=explicit_data_type,
                explicit_data_is_list=explicit_data_is_list,
                custom_data_type=custom_data_type,
            ),
            f"""
            ALTER TABLE {table}
            ADD COLUMN {column} {column_type}
            """,
        )

    async def drop_column(self, table_name: str, column_name: str):
        """
        Remove a column. This is a destructive action, all data in the column
        will be lost.

        """
        assert_is_safe_sql_identifier(table_name)
        assert_is_safe_sql_identifier(column_name)

        table = QueryIdentifier(table_name)
        column = QueryIdentifier(column_name)

        await self._record_signature(
            self.drop_column,
            dict(table_name=table_name, column_name=column_name),
            f"""
            ALTER TABLE {table}
            DROP COLUMN {column}
            """,
        )

    async def rename_column(
        self, table_name: str, old_column_name: str, new_column_name: str
    ):
        """
        Rename a column in a table.

        """
        assert_is_safe_sql_identifier(table_name)
        assert_is_safe_sql_identifier(old_column_name)
        assert_is_safe_sql_identifier(new_column_name)

        table = QueryIdentifier(table_name)
        old_column = QueryIdentifier(old_column_name)
        new_column = QueryIdentifier(new_column_name)

        await self._record_signature(
            self.rename_column,
            dict(
                table_name=table_name,
                old_column_name=old_column_name,
                new_column_name=new_column_name,
            ),
            f"""
            ALTER TABLE {table}
            RENAME COLUMN {old_column} TO {new_column}
            """,
        )

    async def modify_column_type(
        self,
        table_name: str,
        column_name: str,
        explicit_data_type: ColumnType | None = None,
        explicit_data_is_list: bool = False,
        custom_data_type: str | None = None,
        autocast: bool = False,
    ):
        """
        Modify the data type of a column. This does not inherently perform any data migrations
        of the column data types. It simply alters the table schema.

        :param table_name: The name of the table containing the column
        :param column_name: The name of the column to modify
        :param explicit_data_type: The new data type for the column
        :param explicit_data_is_list: Whether the column should be an array type
        :param custom_data_type: A custom SQL type string (mutually exclusive with explicit_data_type)
        :param autocast: If True, automatically add a USING clause to cast existing data to the new type.
                        Auto-generated migrations set this to True by default. Supports most common
                        PostgreSQL type conversions including:
                        - String to numeric (VARCHAR/TEXT → INTEGER/BIGINT/SMALLINT/REAL)
                        - String to boolean (VARCHAR/TEXT → BOOLEAN)
                        - String to date/time (VARCHAR/TEXT → DATE/TIMESTAMP/TIME)
                        - String to specialized types (VARCHAR/TEXT → UUID/JSON/JSONB)
                        - Scalar to array types (INTEGER → INTEGER[])
                        - Custom enum conversions (VARCHAR/TEXT → custom enum)
                        - Compatible numeric conversions (INTEGER → BIGINT)

                        When autocast=False, PostgreSQL will only allow the type change if it's
                        compatible without explicit casting, which may fail for many conversions.

        Example:
            # Auto-generated migration (autocast=True by default)
            await actor.modify_column_type(
                "products", "price", ColumnType.INTEGER, autocast=True
            )

            # Manual migration with custom control
            await actor.modify_column_type(
                "products", "price", ColumnType.INTEGER, autocast=False
            )
            # Then handle data conversion manually if needed

        """
        if not explicit_data_type and not custom_data_type:
            raise ValueError(
                "Must provide either an explicit data type or a custom data type."
            )
        if explicit_data_type and custom_data_type:
            raise ValueError(
                "Cannot provide both an explicit data type and a custom data type."
            )

        assert_is_safe_sql_identifier(table_name)
        assert_is_safe_sql_identifier(column_name)

        # We only need to check the custom data type, since we know
        # the explicit data types come from the enum and are safe.
        if custom_data_type:
            assert_is_safe_sql_identifier(custom_data_type)

        table = QueryIdentifier(table_name)
        column = QueryIdentifier(column_name)

        column_type = self._get_column_type(
            explicit_data_type=explicit_data_type,
            explicit_data_is_list=explicit_data_is_list,
            custom_data_type=custom_data_type,
        )

        # Build the SQL with optional USING clause for autocast
        sql = f"ALTER TABLE {table}\nALTER COLUMN {column} TYPE {column_type}"

        if autocast:
            # Add USING clause to cast the column to the new type
            cast_expression = self._get_autocast_expression(
                column_name=str(column),
                target_type=column_type,
                explicit_data_type=explicit_data_type,
                explicit_data_is_list=explicit_data_is_list,
                custom_data_type=custom_data_type,
            )
            sql += f"\nUSING {cast_expression}"

        await self._record_signature(
            self.modify_column_type,
            dict(
                table_name=table_name,
                column_name=column_name,
                explicit_data_type=explicit_data_type,
                explicit_data_is_list=explicit_data_is_list,
                custom_data_type=custom_data_type,
                autocast=autocast,
            ),
            sql,
        )

    def _get_autocast_expression(
        self,
        column_name: str,
        target_type: str,
        explicit_data_type: ColumnType | None = None,
        explicit_data_is_list: bool = False,
        custom_data_type: str | None = None,
    ) -> str:
        """
        Generate an appropriate USING expression for casting a column to a new type.
        This handles common type conversions that PostgreSQL can perform.
        """
        # For array types, we need to handle them specially
        if explicit_data_is_list:
            # For converting scalar to array, we need to wrap the value in an array
            base_type = (
                explicit_data_type.value if explicit_data_type else custom_data_type
            )
            return f"ARRAY[{column_name}::{base_type}]"

        # For custom types (like enums), use text as intermediate
        if custom_data_type:
            return f"{column_name}::text::{custom_data_type}"

        # For explicit data types, handle special cases
        if explicit_data_type:
            # Handle common conversions that might need special treatment
            if explicit_data_type in [
                ColumnType.INTEGER,
                ColumnType.BIGINT,
                ColumnType.SMALLINT,
            ]:
                # For numeric types, try direct cast first, but this will fail if source is non-numeric string
                return f"{column_name}::{explicit_data_type.value}"
            elif explicit_data_type == ColumnType.BOOLEAN:
                # Boolean conversion can be tricky, use a more flexible approach
                return f"{column_name}::boolean"
            elif explicit_data_type in [
                ColumnType.DATE,
                ColumnType.TIMESTAMP_WITHOUT_TIME_ZONE,
                ColumnType.TIME_WITHOUT_TIME_ZONE,
            ]:
                # Date/time conversions
                return f"{column_name}::{explicit_data_type.value}"
            elif explicit_data_type in [ColumnType.JSON, ColumnType.JSONB]:
                # JSON conversions - usually from text
                return f"{column_name}::{explicit_data_type.value}"
            else:
                # For most other types, a direct cast should work
                return f"{column_name}::{explicit_data_type.value}"

        # Fallback to direct cast
        return f"{column_name}::{target_type}"

    @overload
    async def add_constraint(
        self,
        table_name: str,
        columns: list[str],
        constraint: Literal[ConstraintType.FOREIGN_KEY],
        constraint_name: str,
        constraint_args: ForeignKeyConstraint,
    ): ...

    @overload
    async def add_constraint(
        self,
        table_name: str,
        columns: list[str],
        constraint: Literal[ConstraintType.PRIMARY_KEY]
        | Literal[ConstraintType.UNIQUE],
        constraint_name: str,
        constraint_args: None = None,
    ): ...

    @overload
    async def add_constraint(
        self,
        table_name: str,
        columns: list[str],
        constraint: Literal[ConstraintType.CHECK],
        constraint_name: str,
        constraint_args: CheckConstraint,
    ): ...

    async def add_constraint(
        self,
        table_name: str,
        columns: list[str],
        constraint: ConstraintType,
        constraint_name: str,
        constraint_args: BaseModel | None = None,
    ):
        """
        Adds a constraint to a table. This main entrypoint is used
        for all constraint types.

        :param table_name: The name of the table to add the constraint to.
        :param columns: The columns to link as part of the constraint.
        :param constraint: The type of constraint to add.
        :param constraint_name: The name of the constraint.
        :param constraint_args: The configuration parameters for the particular constraint
            type, if relevant.

        """
        assert_is_safe_sql_identifier(table_name)
        for column_name in columns:
            assert_is_safe_sql_identifier(column_name)

        table = QueryIdentifier(table_name)
        columns_formatted = ", ".join(str(QueryIdentifier(col)) for col in columns)
        sql = f"ALTER TABLE {table} ADD CONSTRAINT {constraint_name} "

        if constraint == ConstraintType.PRIMARY_KEY:
            sql += f"PRIMARY KEY ({columns_formatted})"
        elif constraint == ConstraintType.FOREIGN_KEY:
            if not isinstance(constraint_args, ForeignKeyConstraint):
                raise ValueError(
                    f"Constraint type FOREIGN_KEY must have ForeignKeyConstraint args, received: {constraint_args}"
                )

            assert_is_safe_sql_identifier(constraint_args.target_table)
            for column_name in constraint_args.target_columns:
                assert_is_safe_sql_identifier(column_name)

            target_table = QueryIdentifier(constraint_args.target_table)
            ref_cols_formatted = ", ".join(
                str(QueryIdentifier(col)) for col in constraint_args.target_columns
            )
            sql += f"FOREIGN KEY ({columns_formatted}) REFERENCES {target_table} ({ref_cols_formatted})"
            if constraint_args.on_delete != "NO ACTION":
                sql += f" ON DELETE {constraint_args.on_delete}"
            if constraint_args.on_update != "NO ACTION":
                sql += f" ON UPDATE {constraint_args.on_update}"
        elif constraint == ConstraintType.UNIQUE:
            sql += f"UNIQUE ({columns_formatted})"
        elif constraint == ConstraintType.CHECK:
            if not isinstance(constraint_args, CheckConstraint):
                raise ValueError(
                    f"Constraint type CHECK must have CheckConstraint args, received: {constraint_args}"
                )
            sql += f"CHECK ({constraint_args.check_condition})"
        else:
            raise ValueError("Unsupported constraint type")

        sql += ";"
        await self._record_signature(
            self.add_constraint,
            dict(
                table_name=table_name,
                columns=columns,
                constraint=constraint,
                constraint_name=constraint_name,
                constraint_args=constraint_args,
            ),
            sql,
        )

    async def drop_constraint(
        self,
        table_name: str,
        constraint_name: str,
    ):
        """
        Deletes a constraint from a table.

        """
        assert_is_safe_sql_identifier(table_name)
        assert_is_safe_sql_identifier(constraint_name)

        table = QueryIdentifier(table_name)
        constraint = QueryIdentifier(constraint_name)

        await self._record_signature(
            self.drop_constraint,
            dict(
                table_name=table_name,
                constraint_name=constraint_name,
            ),
            f"""
            ALTER TABLE {table}
            DROP CONSTRAINT {constraint}
            """,
        )

    async def add_index(
        self,
        table_name: str,
        columns: list[str],
        index_name: str,
    ):
        """
        Adds a new index to a table. Since this requires building up the augmentary data structures
        for more efficient search operations, this migration action can take some
        time on large tables.

        """
        assert_is_safe_sql_identifier(table_name)
        for column_name in columns:
            assert_is_safe_sql_identifier(column_name)

        table = QueryIdentifier(table_name)
        columns_formatted = ", ".join(str(QueryIdentifier(col)) for col in columns)
        sql = f"CREATE INDEX {index_name} ON {table} ({columns_formatted});"
        await self._record_signature(
            self.add_index,
            dict(
                table_name=table_name,
                columns=columns,
                index_name=index_name,
            ),
            sql,
        )

    async def drop_index(
        self,
        table_name: str,
        index_name: str,
    ):
        """
        Deletes an index from a table.

        """
        assert_is_safe_sql_identifier(table_name)
        assert_is_safe_sql_identifier(index_name)

        index = QueryIdentifier(index_name)

        sql = f"DROP INDEX {index};"
        await self._record_signature(
            self.drop_index,
            dict(
                table_name=table_name,
                index_name=index_name,
            ),
            sql,
        )

    async def add_not_null(self, table_name: str, column_name: str):
        """
        Requires data inserted into a column to be non-null.

        """
        assert_is_safe_sql_identifier(table_name)
        assert_is_safe_sql_identifier(column_name)

        table = QueryIdentifier(table_name)
        column = QueryIdentifier(column_name)

        await self._record_signature(
            self.add_not_null,
            dict(table_name=table_name, column_name=column_name),
            f"""
            ALTER TABLE {table}
            ALTER COLUMN {column}
            SET NOT NULL
            """,
        )

    async def drop_not_null(self, table_name: str, column_name: str):
        """
        Removes the non-null constraint from a column, which allows new values
        to be inserted as NULL.

        """
        assert_is_safe_sql_identifier(table_name)
        assert_is_safe_sql_identifier(column_name)

        table = QueryIdentifier(table_name)
        column = QueryIdentifier(column_name)

        await self._record_signature(
            self.drop_not_null,
            dict(table_name=table_name, column_name=column_name),
            f"""
            ALTER TABLE {table}
            ALTER COLUMN {column}
            DROP NOT NULL
            """,
        )

    async def add_type(self, type_name: str, values: list[str]):
        """
        Create a new enum type with the given initial values.

        """
        assert_is_safe_sql_identifier(type_name)

        type_identifier = QueryIdentifier(type_name)
        formatted_values = format_sql_values(values)
        await self._record_signature(
            self.add_type,
            dict(type_name=type_name, values=values),
            f"""
            CREATE TYPE {type_identifier} AS ENUM ({formatted_values})
            """,
        )

    async def add_type_values(self, type_name: str, values: list[str]):
        """
        Modifies the enum members of an existing type to add new values.

        """
        assert_is_safe_sql_identifier(type_name)
        type_identifier = QueryIdentifier(type_name)

        sql_commands: list[str] = []
        for value in values:
            # Use the same escape functionality as we use for lists, since
            # there's only one object it won't add any commas
            formatted_value = format_sql_values([value])
            sql_commands.append(
                f"""
            ALTER TYPE {type_identifier} ADD VALUE {formatted_value};
            """
            )

        await self._record_signature(
            self.add_type_values,
            dict(type_name=type_name, values=values),
            sql_commands,
        )

    async def drop_type_values(
        self,
        type_name: str,
        values: list[str],
        target_columns: list[tuple[str, str]],
    ):
        """
        Deletes enum members from an existing type.

        This will only succeed at runtime if you have no table rows that
        currently reference the outdated enum values.

        Note that dropping values from an existing type isn't natively supported by Postgres. We work
        around this limitation by specifying the "target_columns" that reference the
        enum type that we want to drop, so we can effectively create a new type.

        :param type_name: The name of the enum type to drop values from.
        :param values: The values to drop from the enum type.
        :param target_columns: Specified tuples of (table_name, column_name) pairs that
        should be migrated to the new enum value.

        """
        assert_is_safe_sql_identifier(type_name)
        for table_name, column_name in target_columns:
            assert_is_safe_sql_identifier(table_name)
            assert_is_safe_sql_identifier(column_name)

        type_identifier = QueryIdentifier(type_name)
        old_type_identifier = QueryIdentifier(f"{type_name}_old")
        values_to_remove = format_sql_values(values)

        column_modifications = ";\n".join(
            [
                (
                    # The "USING" param is required for enum migration
                    f"EXECUTE 'ALTER TABLE {QueryIdentifier(table_name)} ALTER COLUMN {QueryIdentifier(column_name)} TYPE {type_identifier}"
                    f" USING {QueryIdentifier(column_name)}::text::{type_identifier}'"
                )
                for table_name, column_name in target_columns
            ]
        )
        if column_modifications:
            column_modifications += ";"

        await self._record_signature(
            self.drop_type_values,
            dict(type_name=type_name, values=values, target_columns=target_columns),
            f"""
            DO $$
            DECLARE
                vals text;
            BEGIN
                -- Move the current enum to a temporary type
                EXECUTE 'ALTER TYPE {type_identifier} RENAME TO {old_type_identifier}';

                -- Retrieve all current enum values except those to be excluded
                SELECT string_agg('''' || unnest || '''', ', ' ORDER BY unnest) INTO vals
                FROM unnest(enum_range(NULL::{old_type_identifier})) AS unnest
                WHERE unnest NOT IN ({values_to_remove});

                -- Create and populate our new type with the desired changes
                EXECUTE format('CREATE TYPE {type_identifier} AS ENUM (%s)', vals);

                -- Switch over affected columns to the new type
                {column_modifications}

                -- Drop the old type
                EXECUTE 'DROP TYPE {old_type_identifier}';
            END $$;
            """,
        )

    async def drop_type(self, type_name: str):
        """
        Deletes an enum type from the database.

        """
        assert_is_safe_sql_identifier(type_name)
        type_identifier = QueryIdentifier(type_name)

        await self._record_signature(
            self.drop_type,
            dict(type_name=type_name),
            f"""
            DROP TYPE {type_identifier}
            """,
        )

    def _get_column_type(
        self,
        explicit_data_type: ColumnType | None = None,
        explicit_data_is_list: bool = False,
        custom_data_type: str | None = None,
    ) -> str:
        if explicit_data_type:
            return f"{explicit_data_type}{'[]' if explicit_data_is_list else ''}"
        elif custom_data_type:
            return custom_data_type
        else:
            raise ValueError(
                "Must provide either an explicit data type or a custom data type."
            )

    async def _record_signature(
        self,
        action: Callable,
        kwargs: dict[str, Any],
        sql: str | list[str],
    ):
        """
        If we are doing a dry-run through the migration, only record the method
        signature that was provided. Otherwise if we're actually executing the
        migration, record the SQL that was generated.

        """
        # Validate that the kwargs can populate all of the action signature arguments
        # that are not optional, and that we don't provide any kwargs that aren't specified
        # in the action signature
        # Get the signature of the action
        sig = signature(action)
        parameters = sig.parameters

        # Check for required arguments not supplied
        missing_args = [
            name
            for name, param in parameters.items()
            if param.default is Parameter.empty and name not in kwargs
        ]
        if missing_args:
            raise ValueError(f"Missing required arguments: {missing_args}")

        # Check for extraneous arguments in kwargs
        extraneous_args = [key for key in kwargs if key not in parameters]
        if extraneous_args:
            raise ValueError(f"Extraneous arguments provided: {extraneous_args}")

        if self.dry_run:
            self.dry_run_actions.append(
                DryRunAction(
                    fn=action,
                    kwargs=kwargs,
                )
            )
        else:
            if self.db_connection is None:
                raise ValueError("Cannot execute migration without a database session")

            sql_list = [sql] if isinstance(sql, str) else sql
            for sql_query in sql_list:
                LOGGER.debug(f"Executing migration SQL: {sql_query}")

                self.prod_sqls.append(sql_query)

                try:
                    await self.db_connection.conn.execute(sql_query)
                except Exception as e:
                    # Default errors typically don't include context on the failing SQL
                    LOGGER.error(f"Error executing migration SQL: {sql_query}")
                    raise e

    def add_comment(self, text: str, previous_line: bool = False):
        """
        Only used in dry-run mode to record a code-based comment that should
        be added to the migration file.

        """
        if self.dry_run:
            self.dry_run_actions.append(
                DryRunComment(text=text, previous_line=previous_line)
            )
