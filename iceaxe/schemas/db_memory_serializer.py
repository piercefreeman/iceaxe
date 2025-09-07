import warnings
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from inspect import isgenerator
from typing import Any, Generator, Sequence, Type, TypeVar, Union
from uuid import UUID

from pydantic_core import PydanticUndefined

from iceaxe.base import (
    DBFieldInfo,
    IndexConstraint,
    TableBase,
    UniqueConstraint,
)
from iceaxe.generics import (
    get_typevar_mapping,
    has_null_type,
    is_type_compatible,
    remove_null_type,
)
from iceaxe.migrations.action_sorter import ActionTopologicalSorter
from iceaxe.postgres import (
    PostgresDateTime,
    PostgresForeignKey,
    PostgresTime,
)
from iceaxe.schemas.actions import (
    CheckConstraint,
    ColumnType,
    ConstraintType,
    DatabaseActions,
    ForeignKeyConstraint,
)
from iceaxe.schemas.db_stubs import (
    DBColumn,
    DBColumnPointer,
    DBConstraint,
    DBConstraintPointer,
    DBObject,
    DBObjectPointer,
    DBPointerOr,
    DBTable,
    DBType,
    DBTypePointer,
)
from iceaxe.sql_types import enum_to_name
from iceaxe.typing import (
    ALL_ENUM_TYPES,
    DATE_TYPES,
    JSON_WRAPPER_FALLBACK,
    PRIMITIVE_WRAPPER_TYPES,
)

NodeYieldType = Union[DBObject, DBObjectPointer, "NodeDefinition"]


class CompositePrimaryKeyConstraintError(ValueError):
    """
    Raised when foreign key constraints cannot be resolved due to composite primary keys.

    This occurs when a table has multiple fields marked as primary_key=True, creating
    a composite primary key constraint, but foreign key constraints expect individual
    primary key constraints on the target columns.

    """

    def __init__(self, missing_constraints: list[tuple[str, str]], base_message: str):
        self.missing_constraints = missing_constraints
        self.base_message = base_message

        # Construct the detailed error message
        error_msg = base_message

        if missing_constraints:
            error_msg += "\n\nThis error commonly occurs when you have multiple fields marked as primary_key=True in your model."
            error_msg += "\nIceaxe creates a single composite primary key constraint, but foreign key constraints"
            error_msg += (
                "\nexpect individual primary key constraints on the target columns."
            )
            error_msg += "\n\nFor a detailed explanation of why this happens and how to fix it, see:"
            error_msg += "\nhttps://mountaineer.sh/iceaxe/guides/relationships#composite-primary-keys-and-foreign-key-constraints"
            error_msg += "\n\nTo fix this issue, choose one of these approaches:"
            error_msg += "\n\nRecommended: Modify the current table"
            error_msg += (
                "\n   - Keep only one field as primary_key=True (e.g., just 'id')"
            )
            error_msg += "\n   - Add a UniqueConstraint if you need uniqueness across multiple fields"
            error_msg += "\n   - This is usually the better design pattern"

            # Show specific table/column combinations that are missing
            error_msg += "\n\nCurrently missing individual primary key constraints:"
            for table_name, column_name in missing_constraints:
                error_msg += f"\n  - Table '{table_name}' needs a primary key on column '{column_name}'"

        super().__init__(error_msg)


@dataclass
class NodeDefinition:
    node: DBObject
    dependencies: list[DBObject | DBObjectPointer]
    force_no_dependencies: bool


class DatabaseMemorySerializer:
    """
    Serialize the in-memory database representations into a format that can be
    compared to the database definitions on disk.

    """

    def __init__(self):
        # Construct the directed acyclic graph of the in-memory database objects
        # that indicate what order items should be fulfilled in
        self.db_dag = []

        self.database_handler = DatabaseHandler()

    def delegate(self, tables: list[Type[TableBase]]):
        """
        Find the most specific relevant handler. For instance, if a subclass
        is a registered handler, we should use that instead of the superclass
        If multiple are found we throw, since we can't determine which one to use
        for the resolution.

        """
        yield from self.database_handler.convert(tables)

    def order_db_objects(
        self,
        db_objects: Sequence[tuple[DBObject, Sequence[DBObject | DBObjectPointer]]],
    ):
        """
        Resolve the order that the database objects should be created or modified
        by normalizing pointers/full objects and performing a sort of their defined
        DAG dependencies in the migration graph.

        """
        # First, go through and create a representative object for each of
        # the representation names
        db_objects_by_name: dict[str, DBObject] = {}
        for db_object, _ in db_objects:
            # Only perform this mapping for objects that are not pointers
            if isinstance(db_object, DBObjectPointer):
                continue

            # If the object is already in the dictionary, try to merge the two
            # different values. Otherwise this indicates that there is a conflicting
            # name with a different definition which we don't allow
            if db_object.representation() in db_objects_by_name:
                current_obj = db_objects_by_name[db_object.representation()]
                db_objects_by_name[db_object.representation()] = current_obj.merge(
                    db_object
                )
            else:
                db_objects_by_name[db_object.representation()] = db_object

        # Make sure all the pointers can be resolved by full objects
        # Otherwise we want a verbose error that gives more context
        for _, dependencies in db_objects:
            for dep in dependencies:
                if isinstance(dep, DBObjectPointer):
                    if isinstance(dep, DBPointerOr):
                        # For OR pointers, at least one of the pointers must be resolvable
                        if not any(
                            pointer.representation() in db_objects_by_name
                            for pointer in dep.pointers
                        ):
                            # Create a more helpful error message for common cases
                            missing_pointers = [
                                p.representation() for p in dep.pointers
                            ]
                            error_msg = f"None of the OR pointers {missing_pointers} found in the defined database objects"

                            # Check if this is the common case of multiple primary keys causing foreign key issues
                            primary_key_pointers = []
                            for p in dep.pointers:
                                parsed = p.parse_constraint_pointer()
                                if parsed and parsed.constraint_type == "PRIMARY KEY":
                                    primary_key_pointers.append(p)

                            if primary_key_pointers:
                                # Extract table and column info from the primary key pointers
                                primary_key_info: list[tuple[str, str]] = []
                                for pointer in primary_key_pointers:
                                    table_name = pointer.get_table_name()
                                    column_names = pointer.get_column_names()

                                    if table_name and column_names:
                                        for column_name in column_names:
                                            primary_key_info.append(
                                                (table_name, column_name)
                                            )

                                if primary_key_info:
                                    raise CompositePrimaryKeyConstraintError(
                                        primary_key_info, error_msg
                                    )
                            raise ValueError(error_msg)
                    elif dep.representation() not in db_objects_by_name:
                        raise ValueError(
                            f"Pointer {dep.representation()} not found in the defined database objects"
                        )

        # Map the potentially different objects to the same object
        graph_edges = {}
        for obj, dependencies in db_objects:
            resolved_deps = []
            for dep in dependencies:
                if isinstance(dep, DBObjectPointer):
                    if isinstance(dep, DBPointerOr):
                        # Add all resolvable pointers as dependencies
                        resolved_deps.extend(
                            db_objects_by_name[pointer.representation()]
                            for pointer in dep.pointers
                            if pointer.representation() in db_objects_by_name
                        )
                    else:
                        resolved_deps.append(db_objects_by_name[dep.representation()])
                else:
                    resolved_deps.append(dep)

            if isinstance(obj, DBObjectPointer):
                continue

            graph_edges[db_objects_by_name[obj.representation()]] = resolved_deps

        # Construct the directed acyclic graph
        ts = ActionTopologicalSorter(graph_edges)
        return {obj: i for i, obj in enumerate(ts.sort())}

    async def build_actions(
        self,
        actor: DatabaseActions,
        previous: list[DBObject],
        previous_ordering: dict[DBObject, int],
        next: list[DBObject],
        next_ordering: dict[DBObject, int],
    ):
        # Arrange each object by their representation so we can determine
        # the state of each
        previous_by_name = {obj.representation(): obj for obj in previous}
        next_by_name = {obj.representation(): obj for obj in next}

        previous_ordering_by_name = {
            obj.representation(): order for obj, order in previous_ordering.items()
        }
        next_ordering_by_name = {
            obj.representation(): order for obj, order in next_ordering.items()
        }

        # Verification that the ordering dictionaries align with the objects
        for ordering, objects in [
            (previous_ordering_by_name, previous_by_name),
            (next_ordering_by_name, next_by_name),
        ]:
            if set(ordering.keys()) != set(objects.keys()):
                unique_keys = (set(ordering.keys()) - set(objects.keys())) | (
                    set(objects.keys()) - set(ordering.keys())
                )
                raise ValueError(
                    f"Ordering dictionary keys must be the same as the objects in the list: {unique_keys}"
                )

        # Sort the objects by the order that they should be created in. Only create one object
        # for each representation value, in case we were passed duplicate objects.
        previous = sorted(
            previous_by_name.values(),
            key=lambda obj: previous_ordering_by_name[obj.representation()],
        )
        next = sorted(
            next_by_name.values(),
            key=lambda obj: next_ordering_by_name[obj.representation()],
        )

        for next_obj in next:
            previous_obj = previous_by_name.get(next_obj.representation())

            if previous_obj is None and next_obj is not None:
                await next_obj.create(actor)
            elif previous_obj is not None and next_obj is not None:
                # Only migrate if they're actually different
                if previous_obj != next_obj:
                    await next_obj.migrate(previous_obj, actor)

        # For all of the items that were in the previous state but not in the
        # next state, we should delete them
        to_delete = [
            previous_obj
            for previous_obj in previous
            if previous_obj.representation() not in next_by_name
        ]
        # We use the reversed representation to destroy objects with more dependencies
        # before the dependencies themselves
        to_delete.reverse()
        for previous_obj in to_delete:
            await previous_obj.destroy(actor)

        return actor.dry_run_actions


class TypeDeclarationResponse(DBObject):
    # Not really a db object, but we need to fulfill the yield contract
    # They'll be filtered out later
    primitive_type: ColumnType | None = None
    custom_type: DBType | None = None
    is_list: bool = False

    def representation(self) -> str:
        raise NotImplementedError()

    def create(self, actor: DatabaseActions):
        raise NotImplementedError()

    def destroy(self, actor: DatabaseActions):
        raise NotImplementedError()

    def migrate(self, previous, actor: DatabaseActions):
        raise NotImplementedError()


class DatabaseHandler:
    def __init__(self):
        self.python_to_sql = {
            int: ColumnType.INTEGER,
            float: ColumnType.DOUBLE_PRECISION,
            str: ColumnType.VARCHAR,
            bool: ColumnType.BOOLEAN,
            bytes: ColumnType.BYTEA,
            UUID: ColumnType.UUID,
            Any: ColumnType.JSON,
        }

    def convert(self, tables: list[Type[TableBase]]):
        for model in sorted(tables, key=lambda model: model.get_table_name()):
            for node in self.convert_table(model):
                yield (node.node, node.dependencies)

    def convert_table(self, table: Type[TableBase]):
        # Handle the table itself
        table_nodes = self._yield_nodes(DBTable(table_name=table.get_table_name()))
        yield from table_nodes

        # Handle the columns
        all_column_nodes: list[NodeDefinition] = []
        for field_name, field in table.get_client_fields().items():
            column_nodes = self._yield_nodes(
                self.convert_column(field_name, field, table), dependencies=table_nodes
            )
            yield from column_nodes
            all_column_nodes += column_nodes

            # Handle field-level constraints
            yield from self._yield_nodes(
                self.handle_single_constraints(field_name, field, table),
                dependencies=column_nodes,
            )

        # Primary keys must be handled after the columns are created, since multiple
        # columns can be primary keys but only one constraint can be created
        primary_keys = [
            (key, info) for key, info in table.model_fields.items() if info.primary_key
        ]
        yield from self._yield_nodes(
            self.handle_primary_keys(primary_keys, table), dependencies=all_column_nodes
        )

        if table.table_args != PydanticUndefined:
            for constraint in table.table_args:
                yield from self._yield_nodes(
                    self.handle_multiple_constraints(constraint, table),
                    dependencies=all_column_nodes,
                )

    def convert_column(self, key: str, info: DBFieldInfo, table: Type[TableBase]):
        if info.annotation is None:
            raise ValueError(f"Annotation must be provided for {table.__name__}.{key}")

        # Primary keys should never be nullable, regardless of their type annotation
        is_nullable = not info.primary_key and has_null_type(info.annotation)

        # If we need to create enums or other db-backed types, we need to do that before
        # the column itself
        db_annotation = self.handle_column_type(key, info, table)
        column_type: DBTypePointer | ColumnType
        column_dependencies: list[NodeDefinition] = []
        if db_annotation.custom_type:
            dependencies = self._yield_nodes(
                db_annotation.custom_type, force_no_dependencies=True
            )
            column_dependencies += dependencies
            yield from dependencies

            column_type = DBTypePointer(name=db_annotation.custom_type.name)
        elif db_annotation.primitive_type:
            column_type = db_annotation.primitive_type
        else:
            raise ValueError("Column type must be provided")

        # We need to create the column itself once types have been created
        yield from self._yield_nodes(
            DBColumn(
                table_name=table.get_table_name(),
                column_name=key,
                column_type=column_type,
                column_is_list=db_annotation.is_list,
                nullable=is_nullable,
                autoincrement=info.autoincrement,
            ),
            dependencies=column_dependencies,
        )

    def handle_column_type(self, key: str, info: DBFieldInfo, table: Type[TableBase]):
        if info.annotation is None:
            raise ValueError(f"Annotation must be provided for {table.__name__}.{key}")

        # If explicit_type is provided, use it directly as the preferred type
        if info.explicit_type is not None:
            return TypeDeclarationResponse(
                primitive_type=info.explicit_type,
            )

        annotation = remove_null_type(info.annotation)

        # Resolve the type of the column, if generic
        if isinstance(annotation, TypeVar):
            typevar_map = get_typevar_mapping(table)
            annotation = typevar_map[annotation]

        # Should be prioritized in terms of MRO; StrEnums should be processed
        # before the str types
        if is_type_compatible(annotation, ALL_ENUM_TYPES):
            # We only support string values for enums because postgres enums are defined
            # as name-based types
            for value in annotation:  # type: ignore
                if not isinstance(value.value, str):
                    raise ValueError(
                        f"Only string values are supported for enums, received: {value.value} (enum: {annotation})"
                    )

            return TypeDeclarationResponse(
                custom_type=DBType(
                    name=enum_to_name(annotation),  # type: ignore
                    values=frozenset([value.value for value in annotation]),  # type: ignore
                    reference_columns=frozenset({(table.get_table_name(), key)}),
                ),
            )
        elif is_type_compatible(annotation, PRIMITIVE_WRAPPER_TYPES):
            for primitive, json_type in self.python_to_sql.items():
                if annotation == primitive or annotation == list[primitive]:  # type: ignore
                    return TypeDeclarationResponse(
                        primitive_type=json_type,
                        is_list=(annotation == list[primitive]),  # type: ignore
                    )
        elif is_type_compatible(annotation, DATE_TYPES):
            if is_type_compatible(annotation, datetime):  # type: ignore
                if isinstance(info.postgres_config, PostgresDateTime):
                    return TypeDeclarationResponse(
                        primitive_type=(
                            ColumnType.TIMESTAMP_WITH_TIME_ZONE
                            if info.postgres_config.timezone
                            else ColumnType.TIMESTAMP_WITHOUT_TIME_ZONE
                        )
                    )
                # Assume no timezone if not specified
                return TypeDeclarationResponse(
                    primitive_type=ColumnType.TIMESTAMP_WITHOUT_TIME_ZONE,
                )
            elif is_type_compatible(annotation, date):  # type: ignore
                return TypeDeclarationResponse(
                    primitive_type=ColumnType.DATE,
                )
            elif is_type_compatible(annotation, time):  # type: ignore
                if isinstance(info.postgres_config, PostgresTime):
                    return TypeDeclarationResponse(
                        primitive_type=(
                            ColumnType.TIME_WITH_TIME_ZONE
                            if info.postgres_config.timezone
                            else ColumnType.TIME_WITHOUT_TIME_ZONE
                        ),
                    )
                return TypeDeclarationResponse(
                    primitive_type=ColumnType.TIME_WITHOUT_TIME_ZONE,
                )
            elif is_type_compatible(annotation, timedelta):  # type: ignore
                return TypeDeclarationResponse(
                    primitive_type=ColumnType.INTERVAL,
                )
            else:
                raise ValueError(f"Unsupported date type: {annotation}")
        elif is_type_compatible(annotation, JSON_WRAPPER_FALLBACK):
            if info.is_json:
                return TypeDeclarationResponse(
                    primitive_type=ColumnType.JSON,
                )
            else:
                raise ValueError(
                    f"JSON fields must have Field(is_json=True) specified: {annotation}\n"
                    f"Column: {table.__name__}.{key}"
                )

        raise ValueError(f"Unsupported column type: {annotation}")

    def handle_single_constraints(
        self, key: str, info: DBFieldInfo, table: Type[TableBase]
    ):
        def _build_constraint(
            constraint_type: ConstraintType,
            *,
            foreign_key_constraint: ForeignKeyConstraint | None = None,
            check_constraint: CheckConstraint | None = None,
        ):
            return DBConstraint(
                table_name=table.get_table_name(),
                constraint_type=constraint_type,
                columns=frozenset([key]),
                constraint_name=DBConstraint.new_constraint_name(
                    table.get_table_name(),
                    [key],
                    constraint_type,
                ),
                foreign_key_constraint=foreign_key_constraint,
                check_constraint=check_constraint,
            )

        if info.unique:
            yield from self._yield_nodes(_build_constraint(ConstraintType.UNIQUE))

        if info.foreign_key:
            target_table, target_column = info.foreign_key.rsplit(".", 1)
            # Extract PostgreSQL-specific foreign key options if configured
            on_delete = "NO ACTION"
            on_update = "NO ACTION"
            if isinstance(info.postgres_config, PostgresForeignKey):
                on_delete = info.postgres_config.on_delete
                on_update = info.postgres_config.on_update

            yield from self._yield_nodes(
                _build_constraint(
                    ConstraintType.FOREIGN_KEY,
                    foreign_key_constraint=ForeignKeyConstraint(
                        target_table=target_table,
                        target_columns=frozenset({target_column}),
                        on_delete=on_delete,
                        on_update=on_update,
                    ),
                ),
                dependencies=[
                    # Additional dependencies to ensure the target table/column is created first
                    DBTable(table_name=target_table),
                    DBColumnPointer(
                        table_name=target_table,
                        column_name=target_column,
                    ),
                    # Ensure the primary key constraint exists before the foreign key
                    # constraint. Postgres also accepts a unique constraint on the same.
                    DBPointerOr(
                        pointers=tuple(
                            [
                                DBConstraintPointer(
                                    table_name=target_table,
                                    columns=frozenset([target_column]),
                                    constraint_type=constraint_type,
                                )
                                for constraint_type in [
                                    ConstraintType.PRIMARY_KEY,
                                    ConstraintType.UNIQUE,
                                ]
                            ]
                        ),
                    ),
                ],
            )

        if info.index:
            yield from self._yield_nodes(_build_constraint(ConstraintType.INDEX))

        if info.check_expression:
            yield from self._yield_nodes(
                _build_constraint(
                    ConstraintType.CHECK,
                    check_constraint=CheckConstraint(
                        check_condition=info.check_expression,
                    ),
                )
            )

    def handle_multiple_constraints(
        self, constraint: UniqueConstraint | IndexConstraint, table: Type[TableBase]
    ):
        columns: list[str]
        constraint_type: ConstraintType

        if isinstance(constraint, UniqueConstraint):
            constraint_type = ConstraintType.UNIQUE
            columns = constraint.columns
        elif isinstance(constraint, IndexConstraint):
            constraint_type = ConstraintType.INDEX
            columns = constraint.columns
        else:
            raise ValueError(f"Unsupported constraint type: {constraint}")

        yield from self._yield_nodes(
            DBConstraint(
                table_name=table.get_table_name(),
                constraint_type=constraint_type,
                columns=frozenset(columns),
                constraint_name=DBConstraint.new_constraint_name(
                    table.get_table_name(),
                    columns,
                    constraint_type,
                ),
            )
        )

    def handle_primary_keys(
        self, keys: list[tuple[str, DBFieldInfo]], table: Type[TableBase]
    ):
        if not keys:
            return

        # Warn users about potential issues with multiple primary keys
        if len(keys) > 1:
            column_names = [key for key, _ in keys]
            warnings.warn(
                f"Table '{table.get_table_name()}' has multiple fields marked as primary_key=True: {column_names}. "
                f"This creates a composite primary key constraint, which may cause issues with foreign key "
                f"constraints that expect individual primary keys on target columns. "
                f"Consider using only one primary key field and adding UniqueConstraint for uniqueness instead.",
                UserWarning,
                stacklevel=3,
            )

        columns = [key for key, _ in keys]
        yield from self._yield_nodes(
            DBConstraint(
                table_name=table.get_table_name(),
                constraint_type=ConstraintType.PRIMARY_KEY,
                columns=frozenset(columns),
                constraint_name=DBConstraint.new_constraint_name(
                    table.get_table_name(),
                    columns,
                    ConstraintType.PRIMARY_KEY,
                ),
            )
        )

    def _yield_nodes(
        self,
        child: NodeYieldType | Generator[NodeYieldType, None, None],
        dependencies: Sequence[NodeYieldType] | None = None,
        force_no_dependencies: bool = False,
    ) -> list[NodeDefinition]:
        """
        Given potentially nested nodes, merge them into a flat list of nodes
        with dependencies.

        :param force_no_dependencies: If specified, we will never merge this node
            with any upstream dependencies.
        """

        def _format_dependencies(dependencies: Sequence[NodeYieldType]):
            all_dependencies: list[DBObject | DBObjectPointer] = []

            for value in dependencies:
                if isinstance(value, (DBObject, DBObjectPointer)):
                    all_dependencies.append(value)
                elif isinstance(value, NodeDefinition):
                    all_dependencies.append(value.node)
                    all_dependencies += value.dependencies
                else:
                    raise ValueError(f"Unsupported dependency type: {value}")

            # Sorting isn't required for the DAG but is useful for testing determinism
            return sorted(
                set(all_dependencies),
                key=lambda x: x.representation(),
            )

        results: list[NodeDefinition] = []

        if isinstance(child, DBObject):
            # No dependencies list is provided, let's yield a new one
            results.append(
                NodeDefinition(
                    node=child,
                    dependencies=_format_dependencies(dependencies or []),
                    force_no_dependencies=force_no_dependencies,
                )
            )
        elif isinstance(child, NodeDefinition):
            all_dependencies: list[NodeYieldType] = []
            if not child.force_no_dependencies:
                all_dependencies += dependencies or []
            all_dependencies += child.dependencies

            results.append(
                NodeDefinition(
                    node=child.node,
                    dependencies=_format_dependencies(all_dependencies),
                    force_no_dependencies=force_no_dependencies,
                )
            )
        elif isgenerator(child):
            for node in child:
                results += self._yield_nodes(node, dependencies)
        else:
            raise ValueError(f"Unsupported node type: {child}")

        return results
