from __future__ import annotations

from typing import Any, Generic, Literal, Type, TypeVar, TypeVarTuple, cast, overload

from iceaxe.base import (
    DBFieldClassDefinition,
    DBModelMetaclass,
    TableBase,
)
from iceaxe.comparison import ComparisonGroupType, FieldComparison, FieldComparisonGroup
from iceaxe.functions import FunctionMetadata
from iceaxe.queries_str import (
    QueryElementBase,
    QueryIdentifier,
    QueryLiteral,
)
from iceaxe.typing import (
    ALL_ENUM_TYPES,
    DATE_TYPES,
    JSON_WRAPPER_FALLBACK,
    PRIMITIVE_TYPES,
    PRIMITIVE_WRAPPER_TYPES,
    is_base_table,
    is_column,
    is_comparison,
    is_comparison_group,
    is_function_metadata,
)

P = TypeVar("P")

T = TypeVar(
    "T",
    bound=TableBase
    | DBModelMetaclass
    | ALL_ENUM_TYPES
    | PRIMITIVE_TYPES
    | PRIMITIVE_WRAPPER_TYPES
    | DATE_TYPES
    | JSON_WRAPPER_FALLBACK
    | None,
)
Ts = TypeVarTuple("Ts")


QueryType = TypeVar("QueryType", bound=Literal["SELECT", "INSERT", "UPDATE", "DELETE"])


JoinType = Literal["INNER", "LEFT", "RIGHT", "FULL"]
OrderDirection = Literal["ASC", "DESC"]


class QueryBuilder(Generic[P, QueryType]):
    """
    The QueryBuilder owns all construction of the SQL string given
    python method chaining. Each function call returns a reference to
    self, so you can construct as many queries as you want in a single
    line of code.

    Internally we store most input-arguments as-is. We provide runtime
    value-checking to make sure the right objects are being passed in to query
    manipulation functions so our final build() will deterministically succeed
    if the query build was successful.

    Note that this runtime check-checking validates different types than the static
    analysis. To satisfy Python logical operations (like `join(ModelA.id == ModelB.id)`) we
    have many overloaded operators that return objects at runtime but are masked to their
    Python types for the purposes of static analysis. This implementation detail should
    be transparent to the user but is noted in case you see different types through
    runtime inspection than you see during the typehints.

    """

    def __init__(self):
        self.query_type: QueryType | None = None
        self.main_model: Type[TableBase] | None = None

        self.return_typehint: P

        self.where_conditions: list[FieldComparison | FieldComparisonGroup] = []
        self.order_by_clauses: list[str] = []
        self.join_clauses: list[str] = []
        self.limit_value: int | None = None
        self.offset_value: int | None = None
        self.group_by_fields: list[DBFieldClassDefinition] = []
        self.having_conditions: list[FieldComparison] = []

        # Query specific params
        self.update_values: list[tuple[DBFieldClassDefinition, Any]] = []
        self.select_fields: list[QueryLiteral] = []
        self.select_raw: list[
            DBFieldClassDefinition | Type[TableBase] | FunctionMetadata
        ] = []
        self.select_aggregate_count = 0

        # Text
        self.text_query: str | None = None
        self.text_variables: list[Any] = []

    @overload
    def select(self, fields: T | Type[T]) -> QueryBuilder[T, Literal["SELECT"]]: ...

    @overload
    def select(
        self, fields: tuple[T | Type[T], *Ts]
    ) -> QueryBuilder[tuple[T, *Ts], Literal["SELECT"]]: ...

    def select(
        self, fields: T | Type[T] | tuple[T | Type[T], *Ts]
    ) -> (
        QueryBuilder[tuple[T, *Ts], Literal["SELECT"]]
        | QueryBuilder[T, Literal["SELECT"]]
    ):
        """
        Creates a new select query for the given fields. Returns the same
        QueryBuilder that is now flagged as a SELECT query.

        """
        all_fields: tuple[
            DBFieldClassDefinition | Type[TableBase] | FunctionMetadata, ...
        ]
        if not isinstance(fields, tuple):
            all_fields = (fields,)  # type: ignore
        else:
            all_fields = fields  # type: ignore

        # Verify the field type
        for field in all_fields:
            if (
                not is_column(field)
                and not is_base_table(field)
                and not is_function_metadata(field)
            ):
                raise ValueError(
                    f"Invalid field type {field}. Must be:\n1. A column field\n2. A table\n3. A QueryLiteral\n4. A tuple of the above."
                )

        self._select_inner(all_fields)

        return self  # type: ignore

    def _select_inner(
        self,
        fields: tuple[DBFieldClassDefinition | Type[TableBase] | FunctionMetadata, ...],
    ):
        self.query_type = "SELECT"  # type: ignore
        self.return_typehint = fields  # type: ignore

        if not fields:
            raise ValueError("At least one field must be selected")

        # We always take the default FROM table as the first element
        representative_field = fields[0]
        if is_column(representative_field):
            self.main_model = representative_field.root_model
        elif is_base_table(representative_field):
            self.main_model = representative_field
        elif is_function_metadata(representative_field):
            self.main_model = representative_field.original_field.root_model

        for field in fields:
            if is_column(field):
                literal, _ = field.to_query()
                self.select_fields.append(literal)
                self.select_raw.append(field)
            elif is_base_table(field):
                table_token = QueryIdentifier(field.get_table_name())
                field_token = QueryLiteral("*")
                self.select_fields.append(QueryLiteral(f"{table_token}.{field_token}"))
                self.select_raw.append(field)
            elif is_function_metadata(field):
                field.local_name = f"aggregate_{self.select_aggregate_count}"
                local_name_token = QueryLiteral(field.local_name)

                self.select_fields.append(
                    QueryLiteral(f"{field.literal} AS {local_name_token}")
                )
                self.select_raw.append(field)
                self.select_aggregate_count += 1

    def update(self, model: Type[TableBase]) -> QueryBuilder[None, Literal["UPDATE"]]:
        """
        Creates a new update query for the given model. Returns the same
        QueryBuilder that is now flagged as an UPDATE query.

        """
        self.query_type = "UPDATE"  # type: ignore
        self.main_model = model
        return self  # type: ignore

    def delete(self, model: Type[TableBase]) -> QueryBuilder[None, Literal["DELETE"]]:
        """
        Creates a new delete query for the given model. Returns the same
        QueryBuilder that is now flagged as a DELETE query.

        """
        self.query_type = "DELETE"  # type: ignore
        self.main_model = model
        return self  # type: ignore

    def where(self, *conditions: bool):
        """
        Adds a where condition to the query. The conditions are combined with
        AND. If you need to use OR, you can use the `and_` and `or_` constructors.

        """
        # During typechecking these seem like bool values, since they're the result
        # of the comparison set. But at runtime they will be the whole object that
        # gives the comparison. We can assert that's true here.
        validated_comparisons: list[FieldComparison | FieldComparisonGroup] = []
        for condition in conditions:
            if not is_comparison(condition) and not is_comparison_group(condition):
                raise ValueError(f"Invalid where condition: {condition}")
            validated_comparisons.append(condition)

        self.where_conditions += validated_comparisons
        return self

    def order_by(self, field: Any, direction: OrderDirection = "ASC"):
        """
        Adds an order by clause to the query. The field must be a column.

        :param field: The field to order by.
        :param direction: The direction to order by, either ASC or DESC.

        """
        if not is_column(field):
            raise ValueError(f"Invalid order by field: {field}")

        field_token, _ = field.to_query()
        self.order_by_clauses.append(f"{field_token} {direction}")
        return self

    def join(self, table: Type[TableBase], on: bool, join_type: JoinType = "INNER"):
        """
        Adds a join clause to the query. The `on` parameter should be a comparison
        between two columns.

        :param table: The table to join.
        :param on: The condition to join on, like MyTable.column == OtherTable.column.
        :param join_type: The type of join to perform, either INNER, LEFT, RIGHT, or FULL.

        """
        if not is_comparison(on):
            raise ValueError(
                f"Invalid join condition: {on}, should be MyTable.column == OtherTable.column"
            )

        table_name = QueryLiteral(table.get_table_name())
        on_left, _ = on.left.to_query()
        comparison = QueryLiteral(on.comparison.value)
        on_right, _ = on.right.to_query()

        join_sql = f"{join_type} JOIN {table_name} ON {on_left} {comparison} {on_right}"
        self.join_clauses.append(join_sql)
        return self

    def set(self, column: T, value: T | None):
        """
        Sets a column to a specific value in an update query.

        """
        if not is_column(column):
            raise ValueError(f"Invalid column for set: {column}")

        self.update_values.append((column, value))
        return self

    def limit(self, value: int):
        """
        Limit the number of rows returned by the query. Useful in pagination
        or in only selecting a subset of the results.

        """
        self.limit_value = value
        return self

    def offset(self, value: int):
        """
        Offset the number of rows returned by the query.

        """
        self.offset_value = value
        return self

    def group_by(self, *fields: Any):
        """
        Groups the results of the query by the given fields. This allows
        you to use aggregation functions in your selection queries. See `func`
        for the supported aggregation functions.

        """
        valid_fields: list[DBFieldClassDefinition] = []

        for field in fields:
            if not is_column(field):
                raise ValueError(f"Invalid field for group by: {field}")
            valid_fields.append(field)

        self.group_by_fields = valid_fields
        return self

    def having(self, *conditions: bool):
        """
        Require the result of an aggregation query like func.sum(MyTable.column)
        to be within a certain range. This is similar to the `where` clause but
        for aggregation results.

        """
        valid_conditions: list[FieldComparison] = []

        for condition in conditions:
            if not is_comparison(condition):
                raise ValueError(f"Invalid having condition: {condition}")
            valid_conditions.append(condition)

        self.having_conditions += valid_conditions
        return self

    def text(self, query: str, *variables: Any):
        """
        Override the ORM builder and use a raw SQL query instead.

        """
        self.text_query = query
        self.text_variables = list(variables)
        return self

    def build(self) -> tuple[str, list[Any]]:
        """
        Builds the query and returns the query string and the variables that
        should be sent over the wire.

        """
        if self.text_query:
            return self.text_query, self.text_variables

        query = ""
        variables: list[Any] = []

        if self.query_type == "SELECT":
            if not self.main_model:
                raise ValueError("No model selected for query")

            primary_table = QueryIdentifier(self.main_model.get_table_name())
            fields = [str(field) for field in self.select_fields]
            query = f"SELECT {', '.join(fields)} FROM {primary_table}"
        elif self.query_type == "UPDATE":
            if not self.main_model:
                raise ValueError("No model selected for query")

            primary_table = QueryIdentifier(self.main_model.get_table_name())

            set_components = []
            for column, value in self.update_values:
                column_token, _ = column.to_query()
                set_components.append(f"{column_token} = ${len(variables) + 1}")
                variables.append(value)

            set_clause = ", ".join(set_components)
            query = f"UPDATE {primary_table} SET {set_clause}"
        elif self.query_type == "DELETE":
            if not self.main_model:
                raise ValueError("No model selected for query")

            primary_table = QueryIdentifier(self.main_model.get_table_name())
            query = f"DELETE FROM {primary_table}"

        if self.join_clauses:
            query += " " + " ".join(self.join_clauses)

        if self.where_conditions:
            comparison_group = cast(FieldComparisonGroup, and_(*self.where_conditions))  # type: ignore
            comparison_literal, comparison_variables = comparison_group.to_query(
                len(variables) + 1
            )
            query += f" WHERE {comparison_literal}"
            variables += comparison_variables

        if self.group_by_fields:
            query += " GROUP BY "
            query += ", ".join(
                f"{QueryIdentifier(field.root_model.get_table_name())}.{QueryIdentifier(field.key)}"
                for field in self.group_by_fields
            )

        if self.having_conditions:
            query += " HAVING "
            for i, having_condition in enumerate(self.having_conditions):
                if i > 0:
                    query += " AND "

                having_field = having_condition.left.literal
                having_value: QueryElementBase
                if is_function_metadata(having_condition.right):
                    having_value = having_condition.right.literal
                else:
                    variables.append(having_condition.right)
                    having_value = QueryLiteral("$" + str(len(variables)))

                query += (
                    f"{having_field} {having_condition.comparison.value} {having_value}"
                )

        if self.order_by_clauses:
            query += " ORDER BY " + ", ".join(self.order_by_clauses)

        if self.limit_value is not None:
            query += f" LIMIT {self.limit_value}"

        if self.offset_value is not None:
            query += f" OFFSET {self.offset_value}"

        return query, variables


#
# Comparison chaining
#


def and_(
    *conditions: bool,
) -> bool:
    field_comparisons: list[FieldComparison | FieldComparisonGroup] = []
    for condition in conditions:
        if not is_comparison(condition) and not is_comparison_group(condition):
            raise ValueError(f"Invalid having condition: {condition}")
        field_comparisons.append(condition)
    return cast(
        bool,
        FieldComparisonGroup(type=ComparisonGroupType.AND, elements=field_comparisons),
    )


def or_(
    *conditions: bool,
) -> bool:
    field_comparisons: list[FieldComparison | FieldComparisonGroup] = []
    for condition in conditions:
        if not is_comparison(condition) and not is_comparison_group(condition):
            raise ValueError(f"Invalid having condition: {condition}")
        field_comparisons.append(condition)
    return cast(
        bool,
        FieldComparisonGroup(type=ComparisonGroupType.OR, elements=field_comparisons),
    )


#
# Shortcut entrypoints
# Instead of having to manually create a QueryBuilder object, these functions
# will create one for you and return it.
#


@overload
def select(fields: T | Type[T]) -> QueryBuilder[T, Literal["SELECT"]]: ...


@overload
def select(
    fields: tuple[T | Type[T], *Ts],
) -> QueryBuilder[tuple[T, *Ts], Literal["SELECT"]]: ...


def select(
    fields: T | Type[T] | tuple[T | Type[T], *Ts],
) -> (
    QueryBuilder[tuple[T, *Ts], Literal["SELECT"]] | QueryBuilder[T, Literal["SELECT"]]
):
    """
    Shortcut to create a SELECT query with a new QueryBuilder.

    """
    return QueryBuilder().select(fields)


def update(model: Type[TableBase]) -> QueryBuilder[None, Literal["UPDATE"]]:
    """
    Shortcut to create an UPDATE query with a new QueryBuilder.

    """
    return QueryBuilder().update(model)


def delete(model: Type[TableBase]) -> QueryBuilder[None, Literal["DELETE"]]:
    """
    Shortcut to create a DELETE query with a new QueryBuilder.

    """
    return QueryBuilder().delete(model)
