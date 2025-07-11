from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Generic, Literal, Type, TypeVar, cast

from iceaxe.base import (
    DBFieldClassDefinition,
)
from iceaxe.comparison import (
    ComparisonBase,
    ComparisonType,
    FieldComparison,
)
from iceaxe.queries_str import QueryLiteral
from iceaxe.sql_types import get_python_to_sql_mapping
from iceaxe.typing import is_column, is_function_metadata

T = TypeVar("T")

DATE_PART_FIELD = Literal[
    "century",
    "day",
    "decade",
    "dow",
    "doy",
    "epoch",
    "hour",
    "isodow",
    "isoyear",
    "microseconds",
    "millennium",
    "milliseconds",
    "minute",
    "month",
    "quarter",
    "second",
    "timezone",
    "timezone_hour",
    "timezone_minute",
    "week",
    "year",
]
DATE_PRECISION = Literal[
    "microseconds",
    "milliseconds",
    "second",
    "minute",
    "hour",
    "day",
    "week",
    "month",
    "quarter",
    "year",
    "decade",
    "century",
    "millennium",
]


class FunctionMetadata(ComparisonBase):
    """
    Represents metadata for SQL aggregate functions and other SQL function operations.
    This class bridges the gap between Python function calls and their SQL representations,
    maintaining type information and original field references.

    ```python {{sticky: True}}
    # Internal representation of function calls:
    metadata = FunctionMetadata(
        literal=QueryLiteral("count(users.id)"),
        original_field=User.id,
        local_name="user_count"
    )
    # Used in query: SELECT count(users.id) AS user_count
    ```
    """

    literal: QueryLiteral
    """
    The SQL representation of the function call
    """

    original_field: DBFieldClassDefinition
    """
    The database field this function operates on
    """

    local_name: str | None = None
    """
    Optional alias for the function result in the query
    """

    def __init__(
        self,
        literal: QueryLiteral,
        original_field: DBFieldClassDefinition,
        local_name: str | None = None,
    ):
        self.literal = literal
        self.original_field = original_field
        self.local_name = local_name

    def to_query(self):
        """
        Converts the function metadata to its SQL representation.

        :return: A tuple of the SQL literal and an empty list of variables
        """
        return self.literal, []


class TSQueryFunctionMetadata(FunctionMetadata):
    """
    Represents metadata specifically for tsquery operations in PostgreSQL.
    This class provides methods that are only applicable to tsquery results.
    """

    def matches(self, vector: TSVectorFunctionMetadata) -> FieldComparison:
        """
        Creates a text search match operation (@@) between this tsquery and a tsvector.

        :param vector: The tsvector to match against
        :return: A field comparison object that resolves to a boolean

        ```python {{sticky: True}}
        # Match a tsvector against this tsquery
        matches = func.to_tsquery('english', 'python').matches(
            func.to_tsvector('english', Article.content)
        )
        ```
        """
        metadata = FunctionBuilder._column_to_metadata(vector)

        # Create a new FunctionMetadata for the @@ operation
        match_metadata = FunctionMetadata(
            literal=QueryLiteral(f"{metadata.literal} @@ {self.literal}"),
            original_field=self.original_field,
        )
        # Return a FieldComparison that will be accepted by where()
        return FieldComparison(
            left=match_metadata, comparison=ComparisonType.EQ, right=True
        )


class TSVectorFunctionMetadata(FunctionMetadata):
    """
    Represents metadata specifically for tsvector operations in PostgreSQL.
    This class provides methods that are only applicable to tsvector results.
    """

    def matches(self, query: TSQueryFunctionMetadata) -> FieldComparison:
        """
        Creates a text search match operation (@@) between this tsvector and a tsquery.

        :param query: The tsquery to match against
        :return: A field comparison object that resolves to a boolean

        ```python {{sticky: True}}
        # Match this tsvector against a tsquery
        matches = func.to_tsvector('english', Article.content).matches(
            func.to_tsquery('english', 'python')
        )
        ```
        """
        metadata = FunctionBuilder._column_to_metadata(query)

        # Create a new FunctionMetadata for the @@ operation
        match_metadata = FunctionMetadata(
            literal=QueryLiteral(f"{self.literal} @@ {metadata.literal}"),
            original_field=self.original_field,
        )
        # Return a FieldComparison that will be accepted by where()
        return FieldComparison(
            left=match_metadata, comparison=ComparisonType.EQ, right=True
        )

    def concat(self, other: TSVectorFunctionMetadata) -> TSVectorFunctionMetadata:
        """
        Concatenates two tsvectors.

        :param other: The tsvector to concatenate with
        :return: A TSVectorFunctionMetadata object preserving the input type

        ```python {{sticky: True}}
        # Concatenate two tsvectors
        combined = func.to_tsvector('english', Article.title).concat(
            func.to_tsvector('english', Article.content)
        )
        ```
        """
        metadata = FunctionBuilder._column_to_metadata(other)
        self.literal = QueryLiteral(f"{self.literal} || {metadata.literal}")
        return self


class BooleanExpression(FieldComparison):
    """
    A FieldComparison that represents a complete boolean expression.
    """

    def __init__(self, expression: FunctionMetadata):
        # Initialize with dummy values since we override to_query
        super().__init__(left=expression, comparison=ComparisonType.EQ, right=True)
        self.expression = expression

    def to_query(self, start: int = 1) -> tuple[QueryLiteral, list[Any]]:
        """
        Return the expression directly without additional comparison.
        """
        return self.expression.literal, []


class ArrayComparison(Generic[T], ComparisonBase[bool]):
    """
    Provides comparison methods for SQL array operations (ANY/ALL).
    This class enables ergonomic syntax for array comparisons.

    ```python {{sticky: True}}
    # Using ArrayComparison for ANY operations:
    func.any(Article.tags) == 'python'
    func.any(User.follower_ids) == current_user_id

    # Using ArrayComparison for ALL operations:
    func.all(Project.member_statuses) == 'active'
    func.all(Student.test_scores) >= 70
    ```
    """

    def __init__(
        self, array_metadata: FunctionMetadata, operation: Literal["ANY", "ALL"]
    ):
        """
        Initialize an ArrayComparison object.

        :param array_metadata: The metadata for the array field
        :param operation: The SQL operation (ANY or ALL)
        """
        self.array_metadata = array_metadata
        self.operation = operation

    def __eq__(self, other: T) -> BooleanExpression:  # type: ignore
        """
        Creates an equality comparison with the array elements.

        :param other: The value to compare with array elements
        :return: A boolean expression object that resolves to a boolean
        """
        return self._create_comparison(other, "=")

    def __ne__(self, other: T) -> BooleanExpression:  # type: ignore
        """
        Creates a not-equal comparison with the array elements.

        :param other: The value to compare with array elements
        :return: A boolean expression object that resolves to a boolean
        """
        return self._create_comparison(other, "!=")

    def __lt__(self, other: T) -> BooleanExpression:
        """
        Creates a less-than comparison with the array elements.

        :param other: The value to compare with array elements
        :return: A boolean expression object that resolves to a boolean
        """
        return self._create_comparison(other, "<")

    def __le__(self, other: T) -> BooleanExpression:
        """
        Creates a less-than-or-equal comparison with the array elements.

        :param other: The value to compare with array elements
        :return: A boolean expression object that resolves to a boolean
        """
        return self._create_comparison(other, "<=")

    def __gt__(self, other: T) -> BooleanExpression:
        """
        Creates a greater-than comparison with the array elements.

        :param other: The value to compare with array elements
        :return: A boolean expression object that resolves to a boolean
        """
        return self._create_comparison(other, ">")

    def __ge__(self, other: T) -> BooleanExpression:
        """
        Creates a greater-than-or-equal comparison with the array elements.

        :param other: The value to compare with array elements
        :return: A boolean expression object that resolves to a boolean
        """
        return self._create_comparison(other, ">=")

    def _create_comparison(self, value: T, operator: str) -> BooleanExpression:
        """
        Internal method to create the comparison SQL.

        :param value: The value to compare with array elements
        :param operator: The SQL comparison operator
        :return: A boolean expression object that resolves to a boolean
        """
        # Handle simple values
        if isinstance(value, (str, int, float, bool)):
            value_literal = f"'{value}'" if isinstance(value, str) else str(value)
        else:
            # For complex values, use the metadata
            value_metadata = FunctionBuilder._column_to_metadata(value)
            value_literal = str(value_metadata.literal)

        # Create the comparison as a FunctionMetadata
        result = FunctionMetadata(
            literal=QueryLiteral(
                f"{value_literal} {operator} {self.operation}({self.array_metadata.literal})"
            ),
            original_field=self.array_metadata.original_field,
        )
        # Return a BooleanExpression that generates the SQL directly
        return BooleanExpression(result)

    def to_query(self) -> tuple[QueryLiteral, list[Any]]:
        """
        Converts the array comparison to its SQL representation.

        :return: A tuple of the SQL query string and list of parameter values
        """
        return self.array_metadata.literal, []


class FunctionBuilder:
    """
    Builder class for SQL aggregate functions and other SQL operations.
    Provides a Pythonic interface for creating SQL function calls with proper type hints.

    This class is typically accessed through the global `func` instance:
    ```python {{sticky: True}}
    from iceaxe import func

    query = select((
        User.name,
        func.count(User.id),
        func.max(User.age)
    ))
    ```
    """

    def count(self, field: Any) -> int:
        """
        Creates a COUNT aggregate function call.

        :param field: The field to count. Can be a column or another function result
        :return: A function metadata object that resolves to an integer count

        ```python {{sticky: True}}
        # Count all users
        total = await conn.execute(select(func.count(User.id)))

        # Count distinct values
        unique = await conn.execute(
            select(func.count(func.distinct(User.status)))
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"count({metadata.literal})")
        return cast(int, metadata)

    def distinct(self, field: T) -> T:
        """
        Creates a DISTINCT function call that removes duplicate values.

        :param field: The field to get distinct values from
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get distinct status values
        statuses = await conn.execute(select(func.distinct(User.status)))

        # Count distinct values
        unique_count = await conn.execute(
            select(func.count(func.distinct(User.status)))
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"distinct {metadata.literal}")
        return cast(T, metadata)

    def sum(self, field: T) -> T:
        """
        Creates a SUM aggregate function call.

        :param field: The numeric field to sum
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get total of all salaries
        total = await conn.execute(select(func.sum(Employee.salary)))

        # Sum with grouping
        by_dept = await conn.execute(
            select((Department.name, func.sum(Employee.salary)))
            .group_by(Department.name)
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"sum({metadata.literal})")
        return cast(T, metadata)

    def avg(self, field: T) -> T:
        """
        Creates an AVG aggregate function call.

        :param field: The numeric field to average
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get average age of all users
        avg_age = await conn.execute(select(func.avg(User.age)))

        # Average with grouping
        by_dept = await conn.execute(
            select((Department.name, func.avg(Employee.salary)))
            .group_by(Department.name)
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"avg({metadata.literal})")
        return cast(T, metadata)

    def max(self, field: T) -> T:
        """
        Creates a MAX aggregate function call.

        :param field: The field to get the maximum value from
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get highest salary
        highest = await conn.execute(select(func.max(Employee.salary)))

        # Max with grouping
        by_dept = await conn.execute(
            select((Department.name, func.max(Employee.salary)))
            .group_by(Department.name)
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"max({metadata.literal})")
        return cast(T, metadata)

    def min(self, field: T) -> T:
        """
        Creates a MIN aggregate function call.

        :param field: The field to get the minimum value from
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get lowest salary
        lowest = await conn.execute(select(func.min(Employee.salary)))

        # Min with grouping
        by_dept = await conn.execute(
            select((Department.name, func.min(Employee.salary)))
            .group_by(Department.name)
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"min({metadata.literal})")
        return cast(T, metadata)

    def abs(self, field: T) -> T:
        """
        Creates an ABS function call to get the absolute value.

        :param field: The numeric field to get the absolute value of
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get absolute value of balance
        abs_balance = await conn.execute(select(func.abs(Account.balance)))
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"abs({metadata.literal})")
        return cast(T, metadata)

    def date_trunc(self, precision: DATE_PRECISION, field: T) -> T:
        """
        Truncates a timestamp or interval value to specified precision.

        :param precision: The precision to truncate to ('microseconds', 'milliseconds', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year', 'decade', 'century', 'millennium')
        :param field: The timestamp or interval field to truncate
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Truncate timestamp to month
        monthly = await conn.execute(select(func.date_trunc('month', User.created_at)))
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(
            f"date_trunc('{precision}', {metadata.literal})"
        )
        return cast(T, metadata)

    def date_part(self, field: DATE_PART_FIELD, source: Any) -> float:
        """
        Extracts a subfield from a date/time value.

        :param field: The subfield to extract ('century', 'day', 'decade', 'dow', 'doy', 'epoch', 'hour', 'isodow', 'isoyear', 'microseconds', 'millennium', 'milliseconds', 'minute', 'month', 'quarter', 'second', 'timezone', 'timezone_hour', 'timezone_minute', 'week', 'year')
        :param source: The date/time field to extract from
        :return: A function metadata object that resolves to an integer

        ```python {{sticky: True}}
        # Get month from timestamp
        month = await conn.execute(select(func.date_part('month', User.created_at)))
        ```
        """
        metadata = self._column_to_metadata(source)
        metadata.literal = QueryLiteral(f"date_part('{field}', {metadata.literal})")
        return cast(float, metadata)

    def extract(self, field: DATE_PART_FIELD, source: Any) -> int:
        """
        Extracts a subfield from a date/time value using SQL standard syntax.

        :param field: The subfield to extract ('century', 'day', 'decade', 'dow', 'doy', 'epoch', 'hour', 'isodow', 'isoyear', 'microseconds', 'millennium', 'milliseconds', 'minute', 'month', 'quarter', 'second', 'timezone', 'timezone_hour', 'timezone_minute', 'week', 'year')
        :param source: The date/time field to extract from
        :return: A function metadata object that resolves to an integer

        ```python {{sticky: True}}
        # Get year from timestamp
        year = await conn.execute(select(func.extract('year', User.created_at)))
        ```
        """
        metadata = self._column_to_metadata(source)
        metadata.literal = QueryLiteral(f"extract({field} from {metadata.literal})")
        return cast(int, metadata)

    def age(self, timestamp: T, reference: T | None = None) -> T:
        """
        Calculates the difference between two timestamps.
        If reference is not provided, current_date is used.

        :param timestamp: The timestamp to calculate age from
        :param reference: Optional reference timestamp (defaults to current_date)
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get age of a timestamp
        age = await conn.execute(select(func.age(User.birth_date)))

        # Get age between two timestamps
        age_diff = await conn.execute(select(func.age(Event.end_time, Event.start_time)))
        ```
        """
        metadata = self._column_to_metadata(timestamp)
        if reference is not None:
            ref_metadata = self._column_to_metadata(reference)
            metadata.literal = QueryLiteral(
                f"age({metadata.literal}, {ref_metadata.literal})"
            )
        else:
            metadata.literal = QueryLiteral(f"age({metadata.literal})")
        return cast(T, metadata)

    def date(self, field: T) -> T:
        """
        Converts a timestamp to a date by dropping the time component.

        :param field: The timestamp field to convert
        :return: A function metadata object that resolves to a date

        ```python {{sticky: True}}
        # Get just the date part
        event_date = await conn.execute(select(func.date(Event.timestamp)))
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"date({metadata.literal})")
        return cast(T, metadata)

    # String Functions
    def lower(self, field: T) -> T:
        """
        Converts string to lowercase.

        :param field: The string field to convert
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"lower({metadata.literal})")
        return cast(T, metadata)

    def upper(self, field: T) -> T:
        """
        Converts string to uppercase.

        :param field: The string field to convert
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"upper({metadata.literal})")
        return cast(T, metadata)

    def length(self, field: Any) -> int:
        """
        Returns length of string.

        :param field: The string field to measure
        :return: A function metadata object that resolves to an integer
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"length({metadata.literal})")
        return cast(int, metadata)

    def trim(self, field: T) -> T:
        """
        Removes whitespace from both ends of string.

        :param field: The string field to trim
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"trim({metadata.literal})")
        return cast(T, metadata)

    def substring(self, field: T, start: int, length: int) -> T:
        """
        Extracts substring.

        :param field: The string field to extract from
        :param start: Starting position (1-based)
        :param length: Number of characters to extract
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(
            f"substring({metadata.literal} from {start} for {length})"
        )
        return cast(T, metadata)

    # Mathematical Functions
    def round(self, field: T) -> T:
        """
        Rounds to nearest integer.

        :param field: The numeric field to round
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"round({metadata.literal})")
        return cast(T, metadata)

    def ceil(self, field: T) -> T:
        """
        Rounds up to nearest integer.

        :param field: The numeric field to round up
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"ceil({metadata.literal})")
        return cast(T, metadata)

    def floor(self, field: T) -> T:
        """
        Rounds down to nearest integer.

        :param field: The numeric field to round down
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"floor({metadata.literal})")
        return cast(T, metadata)

    def power(self, field: T, exponent: int | float) -> T:
        """
        Raises a number to the specified power.

        :param field: The numeric field to raise
        :param exponent: The power to raise to
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"power({metadata.literal}, {exponent})")
        return cast(T, metadata)

    def sqrt(self, field: T) -> T:
        """
        Calculates square root.

        :param field: The numeric field to calculate square root of
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"sqrt({metadata.literal})")
        return cast(T, metadata)

    # Aggregate Functions
    def array_agg(self, field: T) -> list[T]:
        """
        Collects values into an array.

        :param field: The field to aggregate
        :return: A function metadata object that resolves to a list
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"array_agg({metadata.literal})")
        return cast(list[T], metadata)

    def string_agg(self, field: Any, delimiter: str) -> str:
        """
        Concatenates values with delimiter.

        :param field: The field to aggregate
        :param delimiter: The delimiter to use between values
        :return: A function metadata object that resolves to a string
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(
            f"string_agg({metadata.literal}, '{delimiter}')"
        )
        return cast(str, metadata)

    def unnest(self, field: list[T]) -> T:
        """
        Expands an array into a set of rows.

        :param field: The array field to unnest
        :return: A function metadata object that resolves to the element type

        ```python {{sticky: True}}
        # Unnest an array column
        tags = await conn.execute(select(func.unnest(Article.tags)))

        # Use with joins
        result = await conn.execute(
            select((User.name, func.unnest(User.favorite_colors)))
            .where(User.id == user_id)
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"unnest({metadata.literal})")
        return cast(T, metadata)

    # Array Operators and Functions
    def any(self, array_field: list[T]) -> ArrayComparison[T]:
        """
        Creates an ANY array comparison that can be used with comparison operators.

        :param array_field: The array field to check against
        :return: An ArrayComparison object that supports comparison operators

        ```python {{sticky: True}}
        # Check if 'python' is in the tags array
        has_python = await conn.execute(
            select(Article)
            .where(func.any(Article.tags) == 'python')
        )

        # Check if user id is in the follower_ids array
        is_follower = await conn.execute(
            select(User)
            .where(func.any(User.follower_ids) == current_user_id)
        )

        # Check if any score is above threshold
        has_passing = await conn.execute(
            select(Student)
            .where(func.any(Student.test_scores) >= 70)
        )
        ```
        """
        array_metadata = self._column_to_metadata(array_field)
        return ArrayComparison(array_metadata, "ANY")

    def all(self, array_field: list[T]) -> ArrayComparison[T]:
        """
        Creates an ALL array comparison that can be used with comparison operators.

        :param array_field: The array field to check against
        :return: An ArrayComparison object that supports comparison operators

        ```python {{sticky: True}}
        # Check if all elements in status array are 'active'
        all_active = await conn.execute(
            select(Project)
            .where(func.all(Project.member_statuses) == 'active')
        )

        # Check if all scores are above threshold
        all_passing = await conn.execute(
            select(Student)
            .where(func.all(Student.test_scores) >= 70)
        )

        # Check if all values are non-null
        all_present = await conn.execute(
            select(Survey)
            .where(func.all(Survey.responses) != None)
        )
        ```
        """
        array_metadata = self._column_to_metadata(array_field)
        return ArrayComparison(array_metadata, "ALL")

    def array_contains(
        self, array_field: list[T], contained: list[T]
    ) -> FunctionMetadata:
        """
        Creates an array contains comparison using the @> operator.
        Checks if the array field contains all elements of the contained array.

        :param array_field: The array field to check
        :param contained: The array that should be contained
        :return: A function metadata object that resolves to a boolean

        ```python {{sticky: True}}
        # Check if tags contain both 'python' and 'django'
        has_both = await conn.execute(
            select(Article)
            .where(func.array_contains(Article.tags, ['python', 'django']) == True)
        )

        # Check if permissions contain required permissions
        has_perms = await conn.execute(
            select(User)
            .where(func.array_contains(User.permissions, ['read', 'write']) == True)
        )
        ```
        """
        array_metadata = self._column_to_metadata(array_field)

        # Convert the contained list to PostgreSQL array syntax
        if all(isinstance(x, str) for x in contained):
            contained_literal = "ARRAY[" + ",".join(f"'{x}'" for x in contained) + "]"
        else:
            contained_literal = "ARRAY[" + ",".join(str(x) for x in contained) + "]"

        # Create the @> comparison as a FunctionMetadata
        array_metadata.literal = QueryLiteral(
            f"{array_metadata.literal} @> {contained_literal}"
        )
        return array_metadata

    def array_contained_by(
        self, array_field: list[T], container: list[T]
    ) -> FunctionMetadata:
        """
        Creates an array contained by comparison using the <@ operator.
        Checks if all elements of the array field are contained in the container array.

        :param array_field: The array field to check
        :param container: The array that should contain all elements
        :return: A function metadata object that resolves to a boolean

        ```python {{sticky: True}}
        # Check if user's skills are all from allowed skills
        valid_skills = await conn.execute(
            select(User)
            .where(func.array_contained_by(User.skills, ['python', 'java', 'go', 'rust']) == True)
        )

        # Check if selected options are all from valid options
        valid_selection = await conn.execute(
            select(Survey)
            .where(func.array_contained_by(Survey.selected, [1, 2, 3, 4, 5]) == True)
        )
        ```
        """
        array_metadata = self._column_to_metadata(array_field)

        # Convert the container list to PostgreSQL array syntax
        if all(isinstance(x, str) for x in container):
            container_literal = "ARRAY[" + ",".join(f"'{x}'" for x in container) + "]"
        else:
            container_literal = "ARRAY[" + ",".join(str(x) for x in container) + "]"

        # Create the <@ comparison as a FunctionMetadata
        array_metadata.literal = QueryLiteral(
            f"{array_metadata.literal} <@ {container_literal}"
        )
        return array_metadata

    def array_overlaps(self, array_field: list[T], other: list[T]) -> FunctionMetadata:
        """
        Creates an array overlap comparison using the && operator.
        Checks if the arrays have any elements in common.

        :param array_field: The first array field
        :param other: The second array to check for overlap
        :return: A function metadata object that resolves to a boolean

        ```python {{sticky: True}}
        # Check if article tags overlap with user interests
        matching_interests = await conn.execute(
            select(Article)
            .where(func.array_overlaps(Article.tags, ['python', 'data-science', 'ml']) == True)
        )

        # Check if available times overlap
        has_common_time = await conn.execute(
            select(Meeting)
            .where(func.array_overlaps(Meeting.available_slots, [1, 2, 3]) == True)
        )
        ```
        """
        array_metadata = self._column_to_metadata(array_field)

        # Convert the other list to PostgreSQL array syntax
        if all(isinstance(x, str) for x in other):
            other_literal = "ARRAY[" + ",".join(f"'{x}'" for x in other) + "]"
        else:
            other_literal = "ARRAY[" + ",".join(str(x) for x in other) + "]"

        # Create the && comparison as a FunctionMetadata
        array_metadata.literal = QueryLiteral(
            f"{array_metadata.literal} && {other_literal}"
        )
        return array_metadata

    def array_append(self, array_field: list[T], element: T) -> list[T]:
        """
        Appends an element to the end of an array.

        :param array_field: The array field to append to
        :param element: The element to append
        :return: A function metadata object that resolves to an array

        ```python {{sticky: True}}
        # Append a tag to the array
        updated = await conn.execute(
            update(Article)
            .set(tags=func.array_append(Article.tags, 'new-tag'))
            .where(Article.id == article_id)
        )

        # Select with appended element
        with_extra = await conn.execute(
            select(func.array_append(User.skills, 'python'))
        )
        ```
        """
        array_metadata = self._column_to_metadata(array_field)

        # Handle element literal
        if isinstance(element, str):
            element_literal = f"'{element}'"
        elif isinstance(element, (int, float, bool)):
            element_literal = str(element)
        else:
            element_metadata = self._column_to_metadata(element)
            element_literal = str(element_metadata.literal)

        array_metadata.literal = QueryLiteral(
            f"array_append({array_metadata.literal}, {element_literal})"
        )
        return cast(list[T], array_metadata)

    def array_prepend(self, element: T, array_field: list[T]) -> list[T]:
        """
        Prepends an element to the beginning of an array.

        :param element: The element to prepend
        :param array_field: The array field to prepend to
        :return: A function metadata object that resolves to an array

        ```python {{sticky: True}}
        # Prepend a tag to the array
        updated = await conn.execute(
            update(Article)
            .set(tags=func.array_prepend('featured', Article.tags))
            .where(Article.id == article_id)
        )

        # Select with prepended element
        with_prefix = await conn.execute(
            select(func.array_prepend('beginner', User.skill_levels))
        )
        ```
        """
        array_metadata = self._column_to_metadata(array_field)

        # Handle element literal
        if isinstance(element, str):
            element_literal = f"'{element}'"
        elif isinstance(element, (int, float, bool)):
            element_literal = str(element)
        else:
            element_metadata = self._column_to_metadata(element)
            element_literal = str(element_metadata.literal)

        array_metadata.literal = QueryLiteral(
            f"array_prepend({element_literal}, {array_metadata.literal})"
        )
        return cast(list[T], array_metadata)

    def array_cat(self, array1: list[T], array2: list[T]) -> list[T]:
        """
        Concatenates two arrays.

        :param array1: The first array
        :param array2: The second array
        :return: A function metadata object that resolves to an array

        ```python {{sticky: True}}
        # Concatenate two arrays
        combined_tags = await conn.execute(
            select(func.array_cat(Article.tags, Article.categories))
        )

        # Merge user permissions
        all_perms = await conn.execute(
            update(User)
            .set(permissions=func.array_cat(User.permissions, ['admin', 'superuser']))
            .where(User.id == user_id)
        )
        ```
        """
        array1_metadata = self._column_to_metadata(array1)

        # Handle array2 - could be a field or a literal list
        if isinstance(array2, list):
            # Convert literal list to PostgreSQL array
            if all(isinstance(x, str) for x in array2):
                array2_literal = "ARRAY[" + ",".join(f"'{x}'" for x in array2) + "]"
            else:
                array2_literal = "ARRAY[" + ",".join(str(x) for x in array2) + "]"
        else:
            array2_metadata = self._column_to_metadata(array2)
            array2_literal = str(array2_metadata.literal)

        array1_metadata.literal = QueryLiteral(
            f"array_cat({array1_metadata.literal}, {array2_literal})"
        )
        return cast(list[T], array1_metadata)

    def array_position(self, array_field: list[T], element: T) -> int:
        """
        Returns the position of the first occurrence of an element in an array (1-based).
        Returns NULL if the element is not found.

        :param array_field: The array field to search in
        :param element: The element to find
        :return: A function metadata object that resolves to an integer

        ```python {{sticky: True}}
        # Find position of a tag
        position = await conn.execute(
            select(func.array_position(Article.tags, 'python'))
        )

        # Find position in a list of ids
        rank = await conn.execute(
            select(func.array_position(Contest.winner_ids, User.id))
            .where(Contest.id == contest_id)
        )
        ```
        """
        array_metadata = self._column_to_metadata(array_field)

        # Handle element literal
        if isinstance(element, str):
            element_literal = f"'{element}'"
        elif isinstance(element, (int, float, bool)):
            element_literal = str(element)
        else:
            element_metadata = self._column_to_metadata(element)
            element_literal = str(element_metadata.literal)

        array_metadata.literal = QueryLiteral(
            f"array_position({array_metadata.literal}, {element_literal})"
        )
        return cast(int, array_metadata)

    def array_remove(self, array_field: list[T], element: T) -> list[T]:
        """
        Removes all occurrences of an element from an array.

        :param array_field: The array field to remove from
        :param element: The element to remove
        :return: A function metadata object that resolves to an array

        ```python {{sticky: True}}
        # Remove a tag from the array
        updated = await conn.execute(
            update(Article)
            .set(tags=func.array_remove(Article.tags, 'deprecated'))
            .where(Article.id == article_id)
        )

        # Remove a skill from user's skill list
        cleaned = await conn.execute(
            update(User)
            .set(skills=func.array_remove(User.skills, 'obsolete-skill'))
            .where(User.id == user_id)
        )
        ```
        """
        array_metadata = self._column_to_metadata(array_field)

        # Handle element literal
        if isinstance(element, str):
            element_literal = f"'{element}'"
        elif isinstance(element, (int, float, bool)):
            element_literal = str(element)
        else:
            element_metadata = self._column_to_metadata(element)
            element_literal = str(element_metadata.literal)

        array_metadata.literal = QueryLiteral(
            f"array_remove({array_metadata.literal}, {element_literal})"
        )
        return cast(list[T], array_metadata)

    # Type Conversion Functions
    def cast(self, field: Any, type_name: Type[T]) -> T:
        """
        Converts value to specified type.

        :param field: The field to convert
        :param type_name: The target Python type to cast to
        :return: A function metadata object with the new type

        ```python {{sticky: True}}
        # Cast a string to integer
        int_value = await conn.execute(select(func.cast(User.string_id, int)))

        # Cast a float to string
        str_value = await conn.execute(select(func.cast(Account.balance, str)))

        # Cast a string to enum
        status = await conn.execute(select(func.cast(User.status_str, UserStatus)))
        ```
        """

        metadata = self._column_to_metadata(field)

        # Special handling for enums
        if issubclass(type_name, Enum):
            metadata.literal = QueryLiteral(
                f"cast({metadata.literal} as {type_name.__name__.lower()})"
            )
        else:
            sql_type = get_python_to_sql_mapping().get(type_name)  # type: ignore
            if not sql_type:
                raise ValueError(f"Unsupported type for casting: {type_name}")
            metadata.literal = QueryLiteral(f"cast({metadata.literal} as {sql_type})")

        return cast(T, metadata)

    def to_char(self, field: Any, format: str) -> str:
        """
        Converts value to string with format.

        :param field: The field to convert
        :param format: The format string
        :return: A function metadata object that resolves to a string
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"to_char({metadata.literal}, '{format}')")
        return cast(str, metadata)

    def to_number(self, field: Any, format: str) -> float:
        """
        Converts string to number with format.

        :param field: The string field to convert
        :param format: The format string
        :return: A function metadata object that resolves to a float
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"to_number({metadata.literal}, '{format}')")
        return cast(float, metadata)

    def to_timestamp(self, field: Any, format: str) -> datetime:
        """
        Converts string to timestamp with format.

        :param field: The string field to convert
        :param format: The format string
        :return: A function metadata object that resolves to a timestamp
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"to_timestamp({metadata.literal}, '{format}')")
        return cast(datetime, metadata)

    def to_tsvector(
        self, language: str, field: T | list[T]
    ) -> TSVectorFunctionMetadata:
        """
        Creates a tsvector from one or more text fields for full-text search.

        :param language: The language to use for text search (e.g., 'english')
        :param field: A single text field or list of text fields to convert to tsvector
        :return: A TSVectorFunctionMetadata object that resolves to a tsvector

        ```python {{sticky: True}}
        # Create a tsvector from a single text field
        vector = func.to_tsvector('english', Article.content)

        # Create a tsvector from multiple text fields
        vector = func.to_tsvector('english', [Article.title, Article.content, Article.summary])
        ```
        """
        if isinstance(field, list):
            if not field:
                raise ValueError("Cannot create tsvector from empty list of fields")

            # Start with the first field
            result = self._column_to_metadata(field[0])
            result.literal = QueryLiteral(
                f"to_tsvector('{language}', {result.literal})"
            )

            # Concatenate remaining fields
            for f in field[1:]:
                metadata = self._column_to_metadata(f)
                metadata.literal = QueryLiteral(
                    f"to_tsvector('{language}', {metadata.literal})"
                )
                result.literal = QueryLiteral(f"{result.literal} || {metadata.literal}")

            return TSVectorFunctionMetadata(
                literal=result.literal,
                original_field=result.original_field,
                local_name=result.local_name,
            )
        else:
            metadata = self._column_to_metadata(field)
            metadata.literal = QueryLiteral(
                f"to_tsvector('{language}', {metadata.literal})"
            )
            return TSVectorFunctionMetadata(
                literal=metadata.literal,
                original_field=metadata.original_field,
                local_name=metadata.local_name,
            )

    def to_tsquery(self, language: str, query: str) -> TSQueryFunctionMetadata:
        """
        Creates a tsquery for full-text search.

        :param language: The language to use for text search (e.g., 'english')
        :param query: The search query string
        :return: A TSQueryFunctionMetadata object that resolves to a tsquery

        ```python {{sticky: True}}
        # Create a tsquery from a search string
        query = func.to_tsquery('english', 'python & programming')
        ```
        """
        return TSQueryFunctionMetadata(
            literal=QueryLiteral(f"to_tsquery('{language}', '{query}')"),
            original_field=None,  # type: ignore
        )

    def setweight(self, field: Any, weight: str) -> TSVectorFunctionMetadata:
        """
        Sets the weight of a tsvector.

        :param field: The tsvector to set weight for
        :param weight: The weight to set (A, B, C, or D)
        :return: A TSVectorFunctionMetadata object for the weighted tsvector

        ```python {{sticky: True}}
        # Set weight for a tsvector
        weighted = func.setweight(func.to_tsvector('english', Article.title), 'A')
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"setweight({metadata.literal}, '{weight}')")
        return TSVectorFunctionMetadata(
            literal=metadata.literal,
            original_field=metadata.original_field,
            local_name=metadata.local_name,
        )

    def ts_rank(self, vector: Any, query: Any) -> int:
        """
        Ranks search results.

        :param vector: The tsvector to rank
        :param query: The tsquery to rank against
        :return: A function metadata object that resolves to a float

        ```python {{sticky: True}}
        # Rank search results
        rank = func.ts_rank(
            func.to_tsvector('english', Article.content),
            func.to_tsquery('english', 'python')
        )
        ```
        """
        vector_metadata = self._column_to_metadata(vector)
        query_metadata = self._column_to_metadata(query)
        metadata = FunctionMetadata(
            literal=QueryLiteral(
                f"ts_rank({vector_metadata.literal}, {query_metadata.literal})"
            ),
            original_field=vector_metadata.original_field,
        )
        return cast(int, metadata)

    def ts_headline(
        self, language: str, field: T, query: T, options: str | None = None
    ) -> str:
        """
        Generates search result highlights.

        :param language: The language to use for text search
        :param field: The text field to generate highlights for
        :param query: The tsquery to highlight
        :param options: Optional configuration string
        :return: A function metadata object that resolves to a string

        ```python {{sticky: True}}
        # Generate search result highlights
        headline = func.ts_headline(
            'english',
            Article.content,
            func.to_tsquery('english', 'python'),
            'StartSel=<mark>, StopSel=</mark>'
        )
        ```
        """
        field_metadata = self._column_to_metadata(field)
        query_metadata = self._column_to_metadata(query)
        if options:
            metadata = FunctionMetadata(
                literal=QueryLiteral(
                    f"ts_headline('{language}', {field_metadata.literal}, {query_metadata.literal}, '{options}')"
                ),
                original_field=field_metadata.original_field,
            )
        else:
            metadata = FunctionMetadata(
                literal=QueryLiteral(
                    f"ts_headline('{language}', {field_metadata.literal}, {query_metadata.literal})"
                ),
                original_field=field_metadata.original_field,
            )
        return cast(str, metadata)

    @staticmethod
    def _column_to_metadata(field: Any) -> FunctionMetadata:
        """
        Internal helper method to convert a field to FunctionMetadata.
        Handles both raw columns and nested function calls.

        :param field: The field to convert
        :return: A FunctionMetadata instance
        :raises ValueError: If the field cannot be converted to a column
        """
        if is_function_metadata(field):
            return field
        elif is_column(field):
            return FunctionMetadata(literal=field.to_query()[0], original_field=field)
        else:
            raise ValueError(
                f"Unable to cast this type to a column: {field} ({type(field)})"
            )


func = FunctionBuilder()
"""
A global instance of FunctionBuilder that provides SQL function operations for use in queries.
This instance offers a comprehensive set of SQL functions including aggregates, string operations,
mathematical functions, date/time manipulations, and type conversions.

Available function categories:
- Aggregate Functions: count, sum, avg, min, max, array_agg, string_agg
- String Functions: lower, upper, length, trim, substring
- Mathematical Functions: abs, round, ceil, floor, power, sqrt
- Date/Time Functions: date_trunc, date_part, extract, age, date
- Type Conversion: cast, to_char, to_number, to_timestamp

```python {{sticky: True}}
from iceaxe import func, select

# Aggregate functions
total_users = await conn.execute(select(func.count(User.id)))
avg_salary = await conn.execute(select(func.avg(Employee.salary)))
unique_statuses = await conn.execute(select(func.distinct(User.status)))

# String operations
users = await conn.execute(select((
    User.id,
    func.lower(User.name),
    func.upper(User.email),
    func.length(User.bio)
)))

# Date/time operations
monthly_stats = await conn.execute(select((
    func.date_trunc('month', Event.created_at),
    func.count(Event.id)
)).group_by(func.date_trunc('month', Event.created_at)))

# Mathematical operations
account_stats = await conn.execute(select((
    Account.id,
    func.abs(Account.balance),
    func.ceil(Account.interest_rate)
)))

# Type conversions
converted = await conn.execute(select((
    func.cast(User.string_id, int),
    func.to_char(User.created_at, 'YYYY-MM-DD'),
    func.cast(User.status_str, UserStatus)
)))

# Complex aggregations
department_stats = await conn.execute(
    select((
        Department.name,
        func.array_agg(Employee.name),
        func.string_agg(Employee.email, ','),
        func.sum(Employee.salary)
    )).group_by(Department.name)
)
```
"""
