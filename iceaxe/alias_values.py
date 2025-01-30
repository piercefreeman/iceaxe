from dataclasses import dataclass
from typing import Generic, TypeVar, cast

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Alias(Generic[T]):
    name: str
    value: T

    def __str__(self):
        return self.name


def alias(name: str, type: T) -> T:
    """
    Creates an alias for a field in raw SQL queries, allowing for type-safe mapping of raw SQL results.
    This is particularly useful in two main scenarios:

    1. When using raw SQL queries with aliased columns:
    ```python
    # Map a COUNT(*) result to an integer
    query = select(alias("user_count", int)).text(
        "SELECT COUNT(*) AS user_count FROM users"
    )

    # Map multiple aliased columns with different types
    query = select((
        alias("full_name", str),
        alias("order_count", int),
        alias("total_spent", float)
    )).text(
        '''
        SELECT
            concat(first_name, ' ', last_name) AS full_name,
            COUNT(orders.id) AS order_count,
            SUM(orders.amount) AS total_spent
        FROM users
        LEFT JOIN orders ON users.id = orders.user_id
        GROUP BY users.id
        '''
    )
    ```

    2. When combining ORM models with function results:
    ```python
    # Select a model alongside a function result
    query = select((
        User,
        alias("name_length", func.length(User.name)),
        alias("upper_name", func.upper(User.name))
    ))

    # Use with aggregation functions
    query = select((
        User,
        alias("total_orders", func.count(Order.id))
    )).join(Order, User.id == Order.user_id).group_by(User.id)
    ```

    :param name: The name of the alias as it appears in the SQL query's AS clause
    :param type: Either a Python type to cast the result to (e.g., int, str, float) or
                a function metadata object (e.g., from func.length())
    :return: A type-safe alias that can be used in select() statements
    """
    return cast(T, Alias(name, type))
