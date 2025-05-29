from collections import defaultdict
from contextlib import asynccontextmanager
from json import loads as json_loads
from math import ceil
from typing import (
    Any,
    Literal,
    ParamSpec,
    Sequence,
    Type,
    TypeVar,
    cast,
    overload,
)

import asyncpg
from typing_extensions import TypeVarTuple

from iceaxe.base import DBFieldClassDefinition, TableBase
from iceaxe.logging import LOGGER
from iceaxe.modifications import ModificationTracker
from iceaxe.queries import (
    QueryBuilder,
    is_base_table,
    is_column,
    is_function_metadata,
)
from iceaxe.queries_str import QueryIdentifier
from iceaxe.session_optimized import optimize_exec_casting

P = ParamSpec("P")
T = TypeVar("T")
Ts = TypeVarTuple("Ts")

TableType = TypeVar("TableType", bound=TableBase)

# PostgreSQL has a limit of 32767 parameters per query (Short.MAX_VALUE)
PG_MAX_PARAMETERS = 32767

TYPE_CACHE = {}


class DBConnection:
    """
    Core class for all ORM actions against a PostgreSQL database. Provides high-level methods
    for executing queries and managing database transactions.

    The DBConnection wraps an asyncpg Connection and provides ORM functionality for:
    - Executing SELECT/INSERT/UPDATE/DELETE queries
    - Managing transactions
    - Inserting, updating, and deleting model instances
    - Refreshing model instances from the database

    ```python {{sticky: True}}
    # Create a connection
    conn = DBConnection(
        await asyncpg.connect(
            host="localhost",
            port=5432,
            user="db_user",
            password="yoursecretpassword",
            database="your_db",
        )
    )

    # Use with models
    class User(TableBase):
        id: int = Field(primary_key=True)
        name: str
        email: str

    # Insert data
    user = User(name="Alice", email="alice@example.com")
    await conn.insert([user])

    # Query data
    users = await conn.exec(
        select(User)
        .where(User.name == "Alice")
    )

    # Update data
    user.email = "newemail@example.com"
    await conn.update([user])
    ```
    """

    def __init__(
        self,
        conn: asyncpg.Connection,
        *,
        uncommitted_verbosity: Literal["ERROR", "WARNING", "INFO"] | None = None,
    ):
        """
        Initialize a new database connection wrapper.

        :param conn: An asyncpg Connection instance to wrap
        :param uncommitted_verbosity: The verbosity level if objects are modified but not committed when
            the session is closed, defaults to nothing

        """
        self.conn = conn
        self.obj_to_primary_key: dict[str, str | None] = {}
        self.in_transaction = False
        self.modification_tracker = ModificationTracker(uncommitted_verbosity)

    async def initialize_types(self, timeout: float = 60.0) -> None:
        """
        Introspect and register PostgreSQL type codecs on this connection,
        caching the result globally using the connection's DB URL as a key. These types
        are unlikely to change in the lifetime of a Python process, so this is typically
        safe to do automatically.

        This method should be called once per connection so we can leverage our own cache. If
        asyncpg is called directly on a new connection, it will result in its own duplicate
        type introspection call.

        """
        global TYPE_CACHE

        if not self.conn._protocol:
            LOGGER.warning(
                "No protocol found for connection during type introspection, will fall back to asyncpg"
            )
            return

        # Determine a unique key for this connection.
        db_url = self.get_dsn()

        # If we've already cached the type information for this DB URL, just register it.
        if db_url in TYPE_CACHE:
            self.conn._protocol.get_settings().register_data_types(TYPE_CACHE[db_url])
            return

        # Get the connection settings object (this is where type codecs are registered).
        settings = self.conn._protocol.get_settings()

        # Query PostgreSQL to get all type OIDs from non-system schemas.
        rows = await self.conn.fetch(
            """
            SELECT t.oid
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            """
        )
        # Build a set of type OIDs.
        typeoids = {row["oid"] for row in rows}

        # Introspect types – this call will recursively determine the PostgreSQL types needed.
        types, intro_stmt = await self.conn._introspect_types(typeoids, timeout)

        # Register the introspected types with the connection's settings.
        settings.register_data_types(types)

        # Cache the types globally so that future connections using the same DB URL
        # can simply register the cached codecs.
        TYPE_CACHE[db_url] = types

    def get_dsn(self) -> str:
        """
        Get the DSN (Data Source Name) string for this connection.

        :return: DSN string in the format 'postgresql://user:password@host:port/dbname'
        """
        params = self.conn._params
        addr = self.conn._addr

        # Build the DSN string with all available parameters
        dsn_parts = ["postgresql://"]

        # Add user/password if available
        if params.user:
            dsn_parts.append(params.user)
            if params.password:
                dsn_parts.append(f":{params.password}")
            dsn_parts.append("@")

        # Add host/port
        dsn_parts.append(addr[0])
        if addr[1]:
            dsn_parts.append(f":{addr[1]}")

        # Add database name
        if params.database:
            dsn_parts.append(f"/{params.database}")

        return "".join(dsn_parts)

    @asynccontextmanager
    async def transaction(self, *, ensure: bool = False):
        """
        Context manager for managing database transactions. Ensures that a series of database
        operations are executed atomically.

        :param ensure: If True and already in a transaction, the context manager will yield without creating a new transaction.
                       If False (default) and already in a transaction, raises a RuntimeError.

        ```python {{sticky: True}}
        async with conn.transaction():
            # All operations here are executed in a transaction
            user = User(name="Alice", email="alice@example.com")
            await conn.insert([user])

            post = Post(title="Hello", user_id=user.id)
            await conn.insert([post])

            # If any operation fails, all changes are rolled back
        ```
        """
        # If ensure is True and we're already in a transaction, just yield
        if self.in_transaction:
            if ensure:
                yield
                return
            else:
                raise RuntimeError(
                    "Cannot start a new transaction while already in a transaction. Use ensure=True if this is intentional."
                )

        # Otherwise, start a new transaction
        self.in_transaction = True
        async with self.conn.transaction():
            try:
                yield
            finally:
                self.in_transaction = False

    @overload
    async def exec(self, query: QueryBuilder[T, Literal["SELECT"]]) -> list[T]: ...

    @overload
    async def exec(self, query: QueryBuilder[T, Literal["INSERT"]]) -> None: ...

    @overload
    async def exec(self, query: QueryBuilder[T, Literal["UPDATE"]]) -> None: ...

    @overload
    async def exec(self, query: QueryBuilder[T, Literal["DELETE"]]) -> None: ...

    async def exec(
        self,
        query: QueryBuilder[T, Literal["SELECT"]]
        | QueryBuilder[T, Literal["INSERT"]]
        | QueryBuilder[T, Literal["UPDATE"]]
        | QueryBuilder[T, Literal["DELETE"]],
    ) -> list[T] | None:
        """
        Execute a query built with QueryBuilder and return the results.

        ```python {{sticky: True}}
        # Select query
        users = await conn.exec(
            select(User)
            .where(User.age >= 18)
            .order_by(User.name)
        )

        # Select with joins and aggregates
        results = await conn.exec(
            select((User.name, func.count(Order.id)))
            .join(Order, Order.user_id == User.id)
            .group_by(User.name)
            .having(func.count(Order.id) > 5)
        )

        # Delete query
        await conn.exec(
            delete(User)
            .where(User.is_active == False)
        )
        ```

        :param query: A QueryBuilder instance representing the query to execute
        :return: For SELECT queries, returns a list of results. For other queries, returns None

        """
        sql_text, variables = query.build()
        LOGGER.debug(f"Executing query: {sql_text} with variables: {variables}")
        try:
            values = await self.conn.fetch(sql_text, *variables)
        except Exception as e:
            LOGGER.error(
                f"Error executing query: {sql_text} with variables: {variables}"
            )
            raise e

        if query._query_type == "SELECT":
            # Pre-cache the select types for better performance
            select_types = [
                (
                    is_base_table(select_raw),
                    is_column(select_raw),
                    is_function_metadata(select_raw),
                )
                for select_raw in query._select_raw
            ]

            result_all = optimize_exec_casting(values, query._select_raw, select_types)

            # Only loop through results if we have verbosity enabled, since this logic otherwise
            # is wasted if no content will eventually be logged
            if self.modification_tracker.verbosity:
                for row in result_all:
                    elements = row if isinstance(row, tuple) else (row,)
                    for element in elements:
                        if isinstance(element, TableBase):
                            element.register_modified_callback(
                                self.modification_tracker.track_modification
                            )

            return cast(list[T], result_all)

        return None

    async def insert(self, objects: Sequence[TableBase]):
        """
        Insert one or more model instances into the database. If the model has an auto-incrementing
        primary key, it will be populated on the instances after insertion.

        ```python {{sticky: True}}
        # Insert a single object
        user = User(name="Alice", email="alice@example.com")
        await conn.insert([user])
        print(user.id)  # Auto-populated primary key

        # Insert multiple objects
        users = [
            User(name="Bob", email="bob@example.com"),
            User(name="Charlie", email="charlie@example.com")
        ]
        await conn.insert(users)
        ```

        :param objects: A sequence of TableBase instances to insert

        """
        if not objects:
            return

        # Reuse a single transaction for all inserts
        async with self.transaction(ensure=True):
            for model, model_objects in self._aggregate_models_by_table(objects):
                # For each table, build batched insert queries
                table_name = QueryIdentifier(model.get_table_name())
                fields = {
                    field: info
                    for field, info in model.model_fields.items()
                    if (not info.exclude and not info.autoincrement)
                }
                primary_key = self._get_primary_key(model)
                field_names = list(fields.keys())
                field_identifiers = ", ".join(f'"{f}"' for f in field_names)

                # Build the base query
                if primary_key:
                    query = f"""
                        INSERT INTO {table_name} ({field_identifiers})
                        VALUES ({", ".join(f"${i}" for i in range(1, len(field_names) + 1))})
                        RETURNING {primary_key}
                    """
                else:
                    query = f"""
                        INSERT INTO {table_name} ({field_identifiers})
                        VALUES ({", ".join(f"${i}" for i in range(1, len(field_names) + 1))})
                    """

                for batch_objects, values_list in self._batch_objects_and_values(
                    model_objects, field_names, fields
                ):
                    # Insert them in one go
                    if primary_key:
                        # For returning queries, we can use fetchmany to get the primary keys
                        rows = await self.conn.fetchmany(query, values_list)
                        for obj, row in zip(batch_objects, rows):
                            setattr(obj, primary_key, row[primary_key])
                    else:
                        # For non-returning queries, we can use executemany
                        await self.conn.executemany(query, values_list)

                    # Mark as unmodified
                    for obj in batch_objects:
                        obj.clear_modified_attributes()

        # Register modification callbacks outside the main insert loop
        if self.modification_tracker.verbosity:
            for obj in objects:
                obj.register_modified_callback(
                    self.modification_tracker.track_modification
                )

        # Clear modification status
        self.modification_tracker.clear_status(objects)

    @overload
    async def upsert(
        self,
        objects: Sequence[TableBase],
        *,
        conflict_fields: tuple[Any, ...],
        update_fields: tuple[Any, ...] | None = None,
        returning_fields: tuple[T, *Ts] | None = None,
    ) -> list[tuple[T, *Ts]] | None: ...

    @overload
    async def upsert(
        self,
        objects: Sequence[TableBase],
        *,
        conflict_fields: tuple[Any, ...],
        update_fields: tuple[Any, ...] | None = None,
        returning_fields: None,
    ) -> None: ...

    async def upsert(
        self,
        objects: Sequence[TableBase],
        *,
        conflict_fields: tuple[Any, ...],
        update_fields: tuple[Any, ...] | None = None,
        returning_fields: tuple[T, *Ts] | None = None,
    ) -> list[tuple[T, *Ts]] | None:
        """
        Performs an upsert (INSERT ... ON CONFLICT DO UPDATE) operation for the given objects.
        This is useful when you want to insert records but update them if they already exist.

        ```python {{sticky: True}}
        # Simple upsert based on email
        users = [
            User(email="alice@example.com", name="Alice"),
            User(email="bob@example.com", name="Bob")
        ]
        await conn.upsert(
            users,
            conflict_fields=(User.email,),
            update_fields=(User.name,)
        )

        # Upsert with returning values
        results = await conn.upsert(
            users,
            conflict_fields=(User.email,),
            update_fields=(User.name,),
            returning_fields=(User.id, User.email)
        )
        for user_id, email in results:
            print(f"Upserted user {email} with ID {user_id}")
        ```

        :param objects: Sequence of TableBase objects to upsert
        :param conflict_fields: Fields to check for conflicts (ON CONFLICT)
        :param update_fields: Fields to update on conflict. If None, updates all non-excluded fields
        :param returning_fields: Fields to return after the operation. If None, returns nothing
        :return: List of tuples containing the returned fields if returning_fields is specified

        """
        if not objects:
            return None

        # Evaluate column types
        conflict_fields_cols: list[DBFieldClassDefinition] = []
        update_fields_cols: list[DBFieldClassDefinition] = []
        returning_fields_cols: list[DBFieldClassDefinition] = []

        # Explicitly validate types of all columns
        for field in conflict_fields:
            if is_column(field):
                conflict_fields_cols.append(field)
            else:
                raise ValueError(f"Field {field} is not a column")
        for field in update_fields or []:
            if is_column(field):
                update_fields_cols.append(field)
            else:
                raise ValueError(f"Field {field} is not a column")
        for field in returning_fields or []:
            if is_column(field):
                returning_fields_cols.append(field)
            else:
                raise ValueError(f"Field {field} is not a column")

        results: list[tuple[T, *Ts]] = []
        async with self.transaction(ensure=True):
            for model, model_objects in self._aggregate_models_by_table(objects):
                table_name = QueryIdentifier(model.get_table_name())
                fields = {
                    field: info
                    for field, info in model.model_fields.items()
                    if (not info.exclude and not info.autoincrement)
                }

                field_string = ", ".join(f'"{field}"' for field in fields)
                placeholders = ", ".join(f"${i}" for i in range(1, len(fields) + 1))
                query = (
                    f"INSERT INTO {table_name} ({field_string}) VALUES ({placeholders})"
                )
                if conflict_fields_cols:
                    conflict_field_string = ", ".join(
                        f'"{field.key}"' for field in conflict_fields_cols
                    )
                    query += f" ON CONFLICT ({conflict_field_string})"

                    if update_fields_cols:
                        set_values = ", ".join(
                            f'"{field.key}" = EXCLUDED."{field.key}"'
                            for field in update_fields_cols
                        )
                        query += f" DO UPDATE SET {set_values}"
                    else:
                        query += " DO NOTHING"

                if returning_fields_cols:
                    returning_string = ", ".join(
                        f'"{field.key}"' for field in returning_fields_cols
                    )
                    query += f" RETURNING {returning_string}"

                # Execute in batches
                for batch_objects, values_list in self._batch_objects_and_values(
                    model_objects, list(fields.keys()), fields
                ):
                    if returning_fields_cols:
                        # For returning queries, we need to use fetchmany to get all results
                        rows = await self.conn.fetchmany(query, values_list)
                        for row in rows:
                            if row:
                                # Process returned values, deserializing JSON if needed
                                processed_values = []
                                for field in returning_fields_cols:
                                    value = row[field.key]
                                    if (
                                        value is not None
                                        and field.root_model.model_fields[
                                            field.key
                                        ].is_json
                                    ):
                                        value = json_loads(value)
                                    processed_values.append(value)
                                results.append(tuple(processed_values))
                    else:
                        # For non-returning queries, we can use executemany
                        await self.conn.executemany(query, values_list)

                    # Clear modified state for successfully upserted objects
                    for obj in batch_objects:
                        obj.clear_modified_attributes()

        self.modification_tracker.clear_status(objects)

        return results if returning_fields_cols else None

    async def update(self, objects: Sequence[TableBase]):
        """
        Update one or more model instances in the database. Only modified attributes will be updated.
        Updates are batched together by grouping objects with the same modified fields, then using
        executemany() for efficiency.

        ```python {{sticky: True}}
        # Update a single object
        user = await conn.exec(select(User).where(User.id == 1))
        user.name = "New Name"
        await conn.update([user])

        # Update multiple objects
        users = await conn.exec(select(User).where(User.age < 18))
        for user in users:
            user.is_minor = True
        await conn.update(users)
        ```

        :param objects: A sequence of TableBase instances to update
        """
        if not objects:
            return

        async with self.transaction(ensure=True):
            for model, model_objects in self._aggregate_models_by_table(objects):
                table_name = QueryIdentifier(model.get_table_name())
                primary_key = self._get_primary_key(model)

                if not primary_key:
                    raise ValueError(
                        f"Model {model} has no primary key, required to UPDATE with ORM objects"
                    )

                primary_key_name = QueryIdentifier(primary_key)

                # Group objects by their modified fields to batch similar updates
                updates_by_fields: defaultdict[frozenset[str], list[TableBase]] = (
                    defaultdict(list)
                )
                for obj in model_objects:
                    modified_attrs = frozenset(
                        k
                        for k, v in obj.get_modified_attributes().items()
                        if not obj.__class__.model_fields[k].exclude
                    )
                    if modified_attrs:
                        updates_by_fields[modified_attrs].append(obj)

                # Process each group of objects with the same modified fields
                for modified_fields, group_objects in updates_by_fields.items():
                    if not modified_fields:
                        continue

                    # Build the UPDATE query for this group
                    field_names = list(modified_fields)
                    fields = {field: model.model_fields[field] for field in field_names}

                    # Build the UPDATE query - note we need one extra parameter per row for the WHERE clause
                    set_clause = ", ".join(
                        f"{QueryIdentifier(key)} = ${i + 2}"
                        for i, key in enumerate(field_names)
                    )
                    query = f"UPDATE {table_name} SET {set_clause} WHERE {primary_key_name} = $1"

                    for batch_objects, values_list in self._batch_objects_and_values(
                        group_objects,
                        field_names,
                        fields,
                        extra_params_per_row=1,  # For the WHERE primary_key parameter
                    ):
                        # Add primary key as first parameter for each row
                        for i, obj in enumerate(batch_objects):
                            values_list[i].insert(0, getattr(obj, primary_key))

                        # Execute the batch update
                        await self.conn.executemany(query, values_list)

                        # Clear modified state for successfully updated objects
                        for obj in batch_objects:
                            obj.clear_modified_attributes()

        self.modification_tracker.clear_status(objects)

    async def delete(self, objects: Sequence[TableBase]):
        """
        Delete one or more model instances from the database.

        ```python {{sticky: True}}
        # Delete a single object
        user = await conn.exec(select(User).where(User.id == 1))
        await conn.delete([user])

        # Delete multiple objects
        inactive_users = await conn.exec(
            select(User).where(User.last_login < datetime.now() - timedelta(days=90))
        )
        await conn.delete(inactive_users)
        ```

        :param objects: A sequence of TableBase instances to delete

        """
        async with self.transaction(ensure=True):
            for model, model_objects in self._aggregate_models_by_table(objects):
                table_name = QueryIdentifier(model.get_table_name())
                primary_key = self._get_primary_key(model)

                if not primary_key:
                    raise ValueError(
                        f"Model {model} has no primary key, required to UPDATE with ORM objects"
                    )

                primary_key_name = QueryIdentifier(primary_key)

                for obj in model_objects:
                    query = f"DELETE FROM {table_name} WHERE {primary_key_name} = $1"
                    await self.conn.execute(query, getattr(obj, primary_key))

        self.modification_tracker.clear_status(objects)

    async def refresh(self, objects: Sequence[TableBase]):
        """
        Refresh one or more model instances from the database, updating their attributes
        with the current database values.

        ```python {{sticky: True}}
        # Refresh a single object
        user = await conn.exec(select(User).where(User.id == 1))
        # ... some time passes, database might have changed
        await conn.refresh([user])  # User now has current database values

        # Refresh multiple objects
        users = await conn.exec(select(User).where(User.department == "Sales"))
        # ... after some time
        await conn.refresh(users)  # All users now have current values
        ```

        :param objects: A sequence of TableBase instances to refresh

        """
        for model, model_objects in self._aggregate_models_by_table(objects):
            table_name = QueryIdentifier(model.get_table_name())
            primary_key = self._get_primary_key(model)
            fields = [
                field for field, info in model.model_fields.items() if not info.exclude
            ]

            if not primary_key:
                raise ValueError(
                    f"Model {model} has no primary key, required to UPDATE with ORM objects"
                )

            primary_key_name = QueryIdentifier(primary_key)
            object_ids = {getattr(obj, primary_key) for obj in model_objects}

            query = f"SELECT * FROM {table_name} WHERE {primary_key_name} = ANY($1)"
            results = {
                result[primary_key]: result
                for result in await self.conn.fetch(query, list(object_ids))
            }

            # Update the objects in-place
            for obj in model_objects:
                obj_id = getattr(obj, primary_key)
                if obj_id in results:
                    # Update field-by-field
                    for field in fields:
                        setattr(obj, field, results[obj_id][field])
                else:
                    LOGGER.error(
                        f"Object {obj} with primary key {obj_id} not found in database"
                    )

        # When an object is refreshed, it's fully overwritten with the new data so by
        # definition it's no longer modified
        for obj in objects:
            obj.clear_modified_attributes()

        self.modification_tracker.clear_status(objects)

    async def get(
        self, model: Type[TableType], primary_key_value: Any
    ) -> TableType | None:
        """
        Retrieve a single model instance by its primary key value.

        This method provides a convenient way to fetch a single record from the database using its primary key.
        It automatically constructs and executes a SELECT query with a WHERE clause matching the primary key.

        ```python {{sticky: True}}
        class User(TableBase):
            id: int = Field(primary_key=True)
            name: str
            email: str

        # Fetch a user by ID
        user = await db_connection.get(User, 1)
        if user:
            print(f"Found user: {user.name}")
        else:
            print("User not found")
        ```

        :param model: The model class to query (must be a subclass of TableBase)
        :param primary_key_value: The value of the primary key to look up
        :return: The model instance if found, None if no record matches the primary key
        :raises ValueError: If the model has no primary key defined

        """
        primary_key = self._get_primary_key(model)
        if not primary_key:
            raise ValueError(
                f"Model {model} has no primary key, required to GET with ORM objects"
            )

        query_builder = QueryBuilder()
        query = query_builder.select(model).where(
            getattr(model, primary_key) == primary_key_value
        )
        results = await self.exec(query)
        return results[0] if results else None

    async def close(self):
        """
        Close the database connection.
        """
        await self.conn.close()
        self.modification_tracker.log()

    def _aggregate_models_by_table(self, objects: Sequence[TableBase]):
        """
        Group model instances by their table class for batch operations.

        :param objects: Sequence of TableBase instances to group
        :return: Iterator of (model_class, list_of_instances) pairs
        """
        objects_by_class: defaultdict[Type[TableBase], list[TableBase]] = defaultdict(
            list
        )
        for obj in objects:
            objects_by_class[obj.__class__].append(obj)

        return objects_by_class.items()

    def _get_primary_key(self, obj: Type[TableBase]) -> str | None:
        """
        Get the primary key field name for a model class, with caching.

        :param obj: The model class to get the primary key for
        :return: The name of the primary key field, or None if no primary key exists
        """
        table_name = obj.get_table_name()
        if table_name not in self.obj_to_primary_key:
            primary_key = [
                field for field, info in obj.model_fields.items() if info.primary_key
            ]
            self.obj_to_primary_key[table_name] = (
                primary_key[0] if primary_key else None
            )
        return self.obj_to_primary_key[table_name]

    def _batch_objects_and_values(
        self,
        objects: Sequence[TableBase],
        field_names: list[str],
        fields: dict[str, Any],
        *,
        extra_params_per_row: int = 0,
    ):
        """
        Helper function to batch objects and their values for database operations.
        Handles batching to stay under PostgreSQL's parameter limits.

        :param objects: Sequence of objects to batch
        :param field_names: List of field names to process
        :param fields: Dictionary of field info
        :param extra_params_per_row: Additional parameters per row beyond the field values
        :return: Generator of (batch_objects, values_list) tuples
        """
        # Calculate max batch size based on number of fields plus any extra parameters
        # Each row uses (len(fields) + extra_params_per_row) parameters
        params_per_row = len(field_names) + extra_params_per_row
        max_batch_size = PG_MAX_PARAMETERS // params_per_row
        # Cap at 5000 rows per batch to avoid excessive memory usage
        max_batch_size = min(max_batch_size, 5000)

        total = len(objects)
        num_batches = ceil(total / max_batch_size)

        for batch_idx in range(num_batches):
            start_idx = batch_idx * max_batch_size
            end_idx = (batch_idx + 1) * max_batch_size
            batch_objects = objects[start_idx:end_idx]

            if not batch_objects:
                continue

            # Convert objects to value lists
            values_list = []
            for obj in batch_objects:
                obj_values = obj.model_dump()
                row_values = []
                for field in field_names:
                    info = fields[field]
                    row_values.append(info.to_db_value(obj_values[field]))
                values_list.append(row_values)

            yield batch_objects, values_list
