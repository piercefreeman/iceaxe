from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Generic, Self, Sequence, TypeVar

from iceaxe.queries_str import QueryElementBase, QueryLiteral
from iceaxe.typing import is_column, is_comparison, is_comparison_group

T = TypeVar("T", bound="ComparisonBase")
J = TypeVar("J")


class ComparisonType(StrEnum):
    """
    Enumeration of SQL comparison operators used in query conditions.
    These operators are used to build WHERE clauses and other conditional expressions.

    ```python {{sticky: True}}
    # Using comparison operators in queries:
    query = select(User).where(
        User.name == "John",  # Uses ComparisonType.EQ
        User.age >= 21,       # Uses ComparisonType.GE
        User.status.in_(["active", "pending"])  # Uses ComparisonType.IN
    )
    ```
    """

    EQ = "="
    """
    Equal to comparison
    """

    NE = "!="
    """
    Not equal to comparison
    """

    LT = "<"
    """
    Less than comparison
    """

    LE = "<="
    """
    Less than or equal to comparison
    """

    GT = ">"
    """
    Greater than comparison
    """

    GE = ">="
    """
    Greater than or equal to comparison
    """

    IN = "IN"
    """
    Check if value is in a list of values
    """

    NOT_IN = "NOT IN"
    """
    Check if value is not in a list of values
    """

    LIKE = "LIKE"
    """
    Pattern matching with wildcards. Supports cases like:
    - "John%" (matches "John Doe", "Johnny", etc.)
    """

    NOT_LIKE = "NOT LIKE"
    """
    Negated pattern matching with wildcards. Supports cases like:
    - "John%" (matches "Amy", "Bob", etc.)
    """

    ILIKE = "ILIKE"
    """
    Case-insensitive pattern matching. Supports cases like:
    - "john%" (matches "John Doe", "johnny", etc.)
    """

    NOT_ILIKE = "NOT ILIKE"
    """
    Negated case-insensitive pattern matching. Supports cases like:
    - "john%" (matches "Amy", "Bob", etc.)
    """

    IS = "IS"
    """
    NULL comparison
    """

    IS_NOT = "IS NOT"
    """
    NOT NULL comparison
    """

    IS_DISTINCT_FROM = "IS DISTINCT FROM"
    """
    IS DISTINCT FROM comparison
    """


class ComparisonGroupType(StrEnum):
    """
    Enumeration of logical operators used to combine multiple comparisons in SQL queries.
    These operators allow building complex conditions by combining multiple WHERE clauses.

    ```python {{sticky: True}}
    # Combining multiple conditions:
    query = select(User).where(
        and_(
            User.age >= 21,
            User.status == "active"
        )
    )

    # Using OR conditions:
    query = select(User).where(
        or_(
            User.role == "admin",
            User.permissions.contains("manage_users")
        )
    )
    ```
    """

    AND = "AND"
    """
    Logical AND operator, all conditions must be true
    """

    OR = "OR"
    """
    Logical OR operator, at least one condition must be true
    """


@dataclass
class FieldComparison(Generic[T]):
    """
    Represents a single SQL comparison operation between a field and a value or another field.
    This class is typically created through the comparison operators (==, !=, >, <, etc.) on database fields.

    ```python {{sticky: True}}
    # These expressions create FieldComparison objects:
    User.age >= 21
    User.status.in_(["active", "pending"])
    User.name.like("%John%")

    # Direct instantiation (rarely needed):
    comparison = FieldComparison(
        left=User.age,
        comparison=ComparisonType.GE,
        right=21
    )
    ```
    """

    left: T
    """
    The left side of the comparison (typically a database field)
    """

    comparison: ComparisonType
    """
    The type of comparison to perform
    """

    right: T | Any
    """
    The right side of the comparison (can be a value or another field)
    """

    def to_query(self, start: int = 1) -> tuple[QueryLiteral, list[Any]]:
        """
        Converts the comparison to its SQL representation.

        :param start: The starting index for query parameters, defaults to 1
        :return: A tuple of the SQL query string and list of parameter values
        """
        variables = []

        field, left_vars = self.left.to_query()
        variables += left_vars

        value: QueryElementBase
        comparison = self.comparison
        if is_column(self.right):
            # Support comparison to other fields (both identifiers)
            value, right_vars = self.right.to_query()
            variables += right_vars
        else:
            variable_offset = str(len(variables) + start)

            if self.right is None:
                # "None" values are not supported as query variables
                value = QueryLiteral("NULL")
            elif self.comparison in (ComparisonType.IN, ComparisonType.NOT_IN):
                variables.append(self.right)
                comparison_map = {
                    ComparisonType.IN: (ComparisonType.EQ, "ANY"),
                    ComparisonType.NOT_IN: (ComparisonType.NE, "ALL"),
                }
                comparison, operator = comparison_map[self.comparison]
                value = QueryLiteral(f"{operator}(${variable_offset})")
            else:
                # Support comparison to static values
                variables.append(self.right)
                value = QueryLiteral(f"${variable_offset}")

        return QueryLiteral(f"{field} {comparison.value} {value}"), variables


@dataclass
class FieldComparisonGroup:
    """
    Represents a group of field comparisons combined with a logical operator (AND/OR).
    This class is typically created through the and_() and or_() functions.

    ```python {{sticky: True}}
    # Using and_() to create an AND group:
    query = select(User).where(
        and_(
            User.age >= 21,
            User.status == "active",
            or_(
                User.role == "admin",
                User.permissions.contains("manage_users")
            )
        )
    )

    # Direct instantiation (rarely needed):
    group = FieldComparisonGroup(
        type=ComparisonGroupType.AND,
        elements=[
            User.age >= 21,
            User.status == "active"
        ]
    )
    ```
    """

    type: ComparisonGroupType
    """
    The type of logical operator to use (AND/OR)

    """

    elements: list["FieldComparison | FieldComparisonGroup"]
    """
    List of comparisons or nested comparison groups to combine
    """

    def to_query(self, start: int = 1) -> tuple[QueryLiteral, list[Any]]:
        """
        Converts the comparison group to its SQL representation.

        :param start: The starting index for query parameters, defaults to 1
        :return: A tuple of the SQL query string and list of parameter values
        """
        queries = ""
        all_variables = []

        for i, element in enumerate(self.elements):
            if i > 0:
                queries += f" {self.type.value} "

            if is_comparison(element):
                query, variables = element.to_query(start=start + len(all_variables))
                queries += f"{query}"
                all_variables += variables
            elif is_comparison_group(element):
                query, variables = element.to_query(start=start + len(all_variables))
                queries += f"({query})"
                all_variables += variables
            else:
                raise ValueError(f"Unexpected element type: {type(element)}")

        return QueryLiteral(queries), all_variables


class ComparisonBase(ABC, Generic[J]):
    """
    Abstract base class for database fields that can be used in comparisons.
    Provides standard comparison operators and methods for SQL query generation.

    This class implements Python's comparison magic methods (__eq__, __ne__, etc.)
    to enable natural syntax for building SQL queries. It also provides additional
    methods for SQL-specific operations like IN, LIKE, and NULL comparisons.

    ```python {{sticky: True}}
    # ComparisonBase enables these operations on database fields:
    User.age >= 21
    User.status == "active"
    User.name.like("%John%")
    User.role.in_(["admin", "moderator"])
    User.deleted_at.is_(None)
    ```
    """

    def __eq__(self, other):  # type: ignore
        """
        Implements equality comparison (==).
        Maps to SQL '=' or 'IS' for NULL comparisons.

        :param other: Value to compare against
        :return: A field comparison object
        """
        if other is None:
            return self._compare(ComparisonType.IS, None)
        return self._compare(ComparisonType.EQ, other)

    def __ne__(self, other):  # type: ignore
        """
        Implements inequality comparison (!=).
        Maps to SQL '!=' or 'IS NOT' for NULL comparisons.

        :param other: Value to compare against
        :return: A field comparison object
        """
        if other is None:
            return self._compare(ComparisonType.IS_NOT, None)
        return self._compare(ComparisonType.NE, other)

    def __lt__(self, other):
        """
        Implements less than comparison (<).
        Maps to SQL '<'.

        :param other: Value to compare against
        :return: A field comparison object
        """
        return self._compare(ComparisonType.LT, other)

    def __le__(self, other):
        """
        Implements less than or equal comparison (<=).
        Maps to SQL '<='.

        :param other: Value to compare against
        :return: A field comparison object
        """
        return self._compare(ComparisonType.LE, other)

    def __gt__(self, other):
        """
        Implements greater than comparison (>).
        Maps to SQL '>'.

        :param other: Value to compare against
        :return: A field comparison object
        """
        return self._compare(ComparisonType.GT, other)

    def __ge__(self, other):
        """
        Implements greater than or equal comparison (>=).
        Maps to SQL '>='.

        :param other: Value to compare against
        :return: A field comparison object
        """
        return self._compare(ComparisonType.GE, other)

    def in_(self, other: Sequence[J]) -> bool:
        """
        Implements SQL IN operator.
        Checks if the field's value is in a sequence of values.

        :param other: Sequence of values to check against
        :return: A field comparison object
        """
        return self._compare(ComparisonType.IN, other)  # type: ignore

    def not_in(self, other: Sequence[J]) -> bool:
        """
        Implements SQL NOT IN operator.
        Checks if the field's value is not in a sequence of values.

        :param other: Sequence of values to check against
        :return: A field comparison object
        """
        return self._compare(ComparisonType.NOT_IN, other)  # type: ignore

    def is_distinct_from(self, other: Any) -> bool:
        """
        Implements SQL IS DISTINCT FROM operator.
        Checks if the field's value is distinct from another value.
        """
        return self._compare(ComparisonType.IS_DISTINCT_FROM, other)  # type: ignore

    def like(
        self: "ComparisonBase[str] | ComparisonBase[str | None]", other: str
    ) -> bool:
        """
        Implements SQL LIKE operator for pattern matching.
        Case-sensitive string pattern matching.

        :param other: Pattern to match against
        :return: A field comparison object
        """
        return self._compare(ComparisonType.LIKE, other)  # type: ignore

    def not_like(
        self: "ComparisonBase[str] | ComparisonBase[str | None]", other: str
    ) -> bool:
        """
        Implements SQL NOT LIKE operator.
        Case-sensitive string pattern non-matching.

        :param other: Pattern to match against
        :return: A field comparison object
        """
        return self._compare(ComparisonType.NOT_LIKE, other)  # type: ignore

    def ilike(
        self: "ComparisonBase[str] | ComparisonBase[str | None]", other: str
    ) -> bool:
        """
        Implements PostgreSQL ILIKE operator.
        Case-insensitive string pattern matching.

        :param other: Pattern to match against
        :return: A field comparison object
        """
        return self._compare(ComparisonType.ILIKE, other)  # type: ignore

    def not_ilike(
        self: "ComparisonBase[str] | ComparisonBase[str | None]", other: str
    ) -> bool:
        """
        Implements PostgreSQL NOT ILIKE operator.
        Case-insensitive string pattern non-matching.

        :param other: Pattern to match against
        :return: A field comparison object
        """
        return self._compare(ComparisonType.NOT_ILIKE, other)  # type: ignore

    def _compare(self, comparison: ComparisonType, other: Any) -> FieldComparison[Self]:
        """
        Internal method to create a field comparison.

        :param comparison: Type of comparison to create
        :param other: Value to compare against
        :return: A field comparison object
        """
        return FieldComparison(left=self, comparison=comparison, right=other)

    @abstractmethod
    def to_query(self) -> tuple["QueryLiteral", list[Any]]:
        """
        Abstract method to convert the field to its SQL representation.
        Must be implemented by subclasses.

        :return: A tuple of the SQL query string and list of parameter values
        """
        pass
