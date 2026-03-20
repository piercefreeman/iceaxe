from functools import lru_cache

import asyncpg


class IceaxeQueryError(asyncpg.PostgresError):
    """
    Query error that subclasses the original asyncpg exception type,
    adding iceaxe context (SQL text and variables) to the message.
    """

    def __init__(
        self,
        original: asyncpg.PostgresError,
        sql_text: str,
        variables: tuple,
    ):
        self.original = original
        self.sql_text = sql_text
        self.variables = variables

        context = f"\nQuery: {sql_text}\nVariables: {variables}"
        message = f"{original}{context}"
        super().__init__(message)

        # Preserve asyncpg's sqlstate if present
        self.sqlstate = getattr(original, "sqlstate", None)


@lru_cache(maxsize=128)
def _get_query_error_class(
    base: type[asyncpg.PostgresError],
) -> type[IceaxeQueryError]:
    """Dynamically create an error class that inherits from both
    IceaxeQueryError and the specific asyncpg exception type."""
    if base is asyncpg.PostgresError:
        return IceaxeQueryError
    return type(
        f"IceaxeQueryError_{base.__name__}",
        (IceaxeQueryError, base),
        {},
    )


def wrap_query_error(
    original: asyncpg.PostgresError,
    sql_text: str,
    variables: tuple,
) -> IceaxeQueryError:
    """Wrap an asyncpg error with query context, preserving the original type hierarchy."""
    cls = _get_query_error_class(type(original))
    return cls(original, sql_text, variables)
