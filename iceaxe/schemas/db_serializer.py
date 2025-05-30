import re
from typing import cast

from iceaxe.io import lru_cache_async
from iceaxe.postgres import ForeignKeyModifications
from iceaxe.schemas.actions import (
    CheckConstraint,
    ColumnType,
    ConstraintType,
    ForeignKeyConstraint,
)
from iceaxe.schemas.db_stubs import (
    DBColumn,
    DBColumnPointer,
    DBConstraint,
    DBObject,
    DBTable,
    DBType,
    DBTypePointer,
)
from iceaxe.session import DBConnection


class DatabaseSerializer:
    """
    Convert the current database state to the intermediary DBObject representations that
    represent its current configuration properties. Used for introspection
    and comparison to the in-code definitions.

    """

    def __init__(self):
        # Internal tables used for migration management, shouldn't be managed in-memory and therefore
        # won't be mirrored by our DBMemorySerializer. We exclude them from this serialization lest there
        # be a detected conflict and we try to remove the migration metadata.
        self.ignore_tables = ["migration_info"]

    @staticmethod
    def _unwrap_db_str(value: str | bytes | bytearray | memoryview) -> str:
        """
        Helper method to handle database values that might be bytes-like or strings.
        PostgreSQL sometimes returns bytes-like objects for certain fields, this normalizes the output.

        :param value: The value from the database, either string or bytes-like object
        :return: The string representation of the value
        """
        if isinstance(value, str):
            return value

        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value).decode()

        raise ValueError(f"Unexpected type for database value: {type(value)}")

    async def get_objects(self, connection: DBConnection):
        tables = []
        async for table, dependencies in self.get_tables(connection):
            tables.append(table)
            yield table, dependencies

        for table in tables:
            async for column, dependencies in self.get_columns(
                connection, table.table_name
            ):
                yield column, dependencies + [table]

            async for constraint, dependencies in self.get_constraints(
                connection, table.table_name
            ):
                yield constraint, dependencies + [table]

            async for constraint, dependencies in self.get_indexes(
                connection, table.table_name
            ):
                yield constraint, dependencies + [table]

    async def get_tables(self, session: DBConnection):
        result = await session.conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )

        for row in result:
            if row["table_name"] in self.ignore_tables:
                continue
            yield DBTable(table_name=row["table_name"]), []

    async def get_columns(self, session: DBConnection, table_name: str):
        query = """
            SELECT
                cols.column_name,
                cols.udt_name,
                cols.data_type,
                cols.is_nullable,
                CASE
                    WHEN cols.data_type = 'ARRAY' THEN elem_type.data_type
                    ELSE NULL
                END AS element_type
            FROM information_schema.columns AS cols
            LEFT JOIN information_schema.element_types AS elem_type
                ON cols.table_catalog = elem_type.object_catalog
                AND cols.table_schema = elem_type.object_schema
                AND cols.table_name = elem_type.object_name
                AND cols.dtd_identifier = elem_type.collection_type_identifier
            WHERE cols.table_name = $1
                AND cols.table_schema = 'public';

        """
        result = await session.conn.fetch(query, table_name)

        column_dependencies: list[DBObject] = []
        for row in result:
            column_is_list = False

            if row["data_type"] == "USER-DEFINED":
                column_type, column_type_deps = await self.fetch_custom_type(
                    session, row["udt_name"]
                )
                column_dependencies.append(column_type)
                yield column_type, column_type_deps
            elif row["data_type"] == "ARRAY":
                column_is_list = True
                column_type = ColumnType(row["element_type"])
            else:
                column_type = ColumnType(row["data_type"])

            yield (
                DBColumn(
                    table_name=table_name,
                    column_name=row["column_name"],
                    column_type=(
                        DBTypePointer(name=column_type.name)
                        if isinstance(column_type, DBType)
                        else column_type
                    ),
                    column_is_list=column_is_list,
                    nullable=(row["is_nullable"] == "YES"),
                ),
                column_dependencies,
            )

    async def get_constraints(self, session: DBConnection, table_name: str):
        query = """
            SELECT 
                conname, 
                contype, 
                conrelid, 
                confrelid, 
                conkey, 
                confkey,
                confupdtype,
                confdeltype
            FROM pg_constraint
            INNER JOIN pg_class ON pg_constraint.conrelid = pg_class.oid
            WHERE pg_class.relname = $1
        """
        result = await session.conn.fetch(query, table_name)
        for row in result:
            contype = self._unwrap_db_str(row["contype"])
            # Determine type
            if contype == "p":
                ctype = ConstraintType.PRIMARY_KEY
            elif contype == "f":
                ctype = ConstraintType.FOREIGN_KEY
            elif contype == "u":
                ctype = ConstraintType.UNIQUE
            elif contype == "c":
                ctype = ConstraintType.CHECK
            else:
                raise ValueError(f"Unknown constraint type: {row['contype']}")

            columns = await self.fetch_constraint_columns(
                session, row["conkey"], table_name
            )

            # Handle foreign key specifics
            fk_constraint: ForeignKeyConstraint | None = None
            check_constraint: CheckConstraint | None = None

            if ctype == ConstraintType.FOREIGN_KEY:
                # Fetch target table
                fk_query = "SELECT relname FROM pg_class WHERE oid = $1"
                fk_result = await session.conn.fetch(fk_query, row["confrelid"])
                target_table = fk_result[0]["relname"]

                # Fetch target columns
                target_columns_query = """
                    SELECT a.attname AS column_name
                    FROM pg_attribute a
                    WHERE a.attrelid = $1 AND a.attnum = ANY($2)
                """
                target_columns_result = await session.conn.fetch(
                    target_columns_query,
                    row["confrelid"],
                    row["confkey"],
                )
                target_columns = {row["column_name"] for row in target_columns_result}

                # Map PostgreSQL action codes to action strings
                action_map = {
                    "a": "NO ACTION",
                    "r": "RESTRICT",
                    "c": "CASCADE",
                    "n": "SET NULL",
                    "d": "SET DEFAULT",
                }

                on_update = action_map.get(
                    self._unwrap_db_str(row["confupdtype"]),
                    "NO ACTION",
                )
                on_delete = action_map.get(
                    self._unwrap_db_str(row["confdeltype"]),
                    "NO ACTION",
                )

                on_update_mod = cast(ForeignKeyModifications, on_update)
                on_delete_mod = cast(ForeignKeyModifications, on_delete)

                fk_constraint = ForeignKeyConstraint(
                    target_table=target_table,
                    target_columns=frozenset(target_columns),
                    on_delete=on_delete_mod,
                    on_update=on_update_mod,
                )
            elif ctype == ConstraintType.CHECK:
                # Retrieve the check constraint expression
                check_query = """
                    SELECT pg_get_constraintdef(c.oid) AS consrc
                    FROM pg_constraint c
                    WHERE c.oid = $1
                    """
                check_result = await session.conn.fetch(check_query, row["oid"])
                check_constraint_expr = check_result[0]["consrc"]

                check_constraint = CheckConstraint(
                    check_condition=check_constraint_expr,
                )

            yield (
                DBConstraint(
                    table_name=table_name,
                    constraint_name=row["conname"],
                    columns=frozenset(columns),
                    constraint_type=ctype,
                    foreign_key_constraint=fk_constraint,
                    check_constraint=check_constraint,
                ),
                [
                    # We require the columns to be created first
                    DBColumnPointer(table_name=table_name, column_name=column)
                    for column in columns
                ],
            )

    async def get_indexes(self, session: DBConnection, table_name: str):
        # Query for indexes, excluding primary keys
        index_query = """
            SELECT i.indexname, i.indexdef
            FROM pg_indexes i
            LEFT JOIN pg_constraint c ON c.conname = i.indexname
            WHERE i.tablename = $1
            AND c.conname IS NULL
            AND i.indexdef NOT ILIKE '%UNIQUE INDEX%'
        """
        index_result = await session.conn.fetch(index_query, table_name)

        for row in index_result:
            index_name = row["indexname"]
            index_def = row["indexdef"]

            # Extract columns from index definition
            columns_match = re.search(r"\((.*?)\)", index_def)
            if columns_match:
                # Reserved names are quoted in the response body
                columns = [
                    col.strip().strip('"') for col in columns_match.group(1).split(",")
                ]
            else:
                columns = []

            yield (
                DBConstraint(
                    table_name=table_name,
                    columns=frozenset(columns),
                    constraint_name=index_name,
                    constraint_type=ConstraintType.INDEX,
                ),
                [
                    DBColumnPointer(table_name=table_name, column_name=column)
                    for column in columns
                ],
            )

    async def fetch_constraint_columns(self, session: DBConnection, conkey, table_name):
        # Assume conkey is a list of column indices; this function would fetch actual column names
        query = "SELECT attname FROM pg_attribute WHERE attnum = ANY($1) AND attrelid = (SELECT oid FROM pg_class WHERE relname = $2)"
        return [
            row["attname"]
            for row in await session.conn.fetch(query, conkey, table_name)
        ]

    # Enum values are not expected to change within one session, cache the same
    # type if we see it within the same session
    @lru_cache_async(maxsize=None)
    async def fetch_custom_type(self, session: DBConnection, type_name: str):
        # Get the values in this enum
        values_query = """
        SELECT enumlabel
        FROM pg_enum
        JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
        WHERE pg_type.typname = $1
        """
        values = frozenset(
            [
                row["enumlabel"]
                for row in await session.conn.fetch(values_query, type_name)
            ]
        )

        # Determine all the columns where this type is referenced
        reference_columns_query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                a.attname AS column_name
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            JOIN pg_catalog.pg_attribute a ON a.atttypid = t.oid
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            WHERE
                t.typname = $1
                AND a.attnum > 0
                AND NOT a.attisdropped;
            """
        reference_columns_results = await session.conn.fetch(
            reference_columns_query, type_name
        )
        reference_columns = frozenset(
            {
                (row["table_name"], row["column_name"])
                for row in reference_columns_results
            }
        )
        return DBType(
            name=type_name, values=values, reference_columns=reference_columns
        ), []
