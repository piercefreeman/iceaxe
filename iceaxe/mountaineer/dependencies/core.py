"""
Optional compatibility layer for `mountaineer` dependency access.

"""

from typing import AsyncGenerator

import asyncpg
from mountaineer import CoreDependencies, Depends

from iceaxe.mountaineer.config import DatabaseConfig
from iceaxe.session import DBConnection


async def get_db_connection(
    config: DatabaseConfig = Depends(
        CoreDependencies.get_config_with_type(DatabaseConfig)
    ),
) -> AsyncGenerator[DBConnection, None]:
    """
    A dependency that provides a database connection for use in FastAPI endpoints or other
    dependency-injected contexts. The connection is automatically closed when the endpoint
    finishes processing.

    This dependency:
    - Creates a new PostgreSQL connection using the provided configuration
    - Wraps it in a DBConnection for ORM functionality
    - Initializes the connection's type cache to support enums without per-connection
      type introspection
    - Automatically closes the connection when done
    - Integrates with Mountaineer's dependency injection system

    :param config: DatabaseConfig instance containing connection parameters.
                  Automatically injected by Mountaineer if not provided.
    :return: An async generator yielding a DBConnection instance

    ```python
    from fastapi import FastAPI, Depends
    from iceaxe.mountaineer.dependencies import get_db_connection
    from iceaxe.session import DBConnection

    app = FastAPI()

    # Basic usage in a FastAPI endpoint
    @app.get("/users")
    async def get_users(db: DBConnection = Depends(get_db_connection)):
        users = await db.exec(select(User))
        return users
    ```
    """
    conn = await asyncpg.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        database=config.POSTGRES_DB,
    )

    connection = DBConnection(
        conn, uncommitted_verbosity=config.ICEAXE_UNCOMMITTED_VERBOSITY
    )
    await connection.initialize_types()

    try:
        yield connection
    finally:
        await connection.close()
