from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from iceaxe.mountaineer.config import DatabaseConfig
from iceaxe.mountaineer.dependencies.core import get_db_connection
from iceaxe.session import DBConnection


@pytest.fixture(autouse=True)
def mock_db_connect(mock_connection: AsyncMock):
    with patch("asyncpg.connect", new_callable=AsyncMock) as mock:
        mock.return_value = mock_connection

        yield mock


@pytest.fixture
def mock_config():
    return DatabaseConfig(
        POSTGRES_HOST="test-host",
        POSTGRES_PORT=5432,
        POSTGRES_USER="test-user",
        POSTGRES_PASSWORD="test-pass",
        POSTGRES_DB="test-db",
    )


@pytest.fixture
def mock_connection():
    conn = AsyncMock(spec=asyncpg.Connection)
    conn.close = AsyncMock()

    # We need to populate the internal dsn parameters like the real query
    conn._addr = ("test-host", 5432)
    conn._params.user = "test-user"
    conn._params.password = "test-pass"
    conn._params.database = "test-db"

    conn._introspect_types.return_value = (MagicMock(), MagicMock())

    return conn


@pytest.mark.asyncio
async def test_get_db_connection_closes_after_yield(
    mock_config: DatabaseConfig,
    mock_connection: AsyncMock,
    mock_db_connect: AsyncMock,
):
    mock_db_connect.return_value = mock_connection

    # Get the generator
    db_gen = get_db_connection(mock_config)

    # Get the connection
    connection = await anext(db_gen)  # noqa: F821

    assert isinstance(connection, DBConnection)
    assert connection.conn == mock_connection
    mock_db_connect.assert_called_once_with(
        host=mock_config.POSTGRES_HOST,
        port=mock_config.POSTGRES_PORT,
        user=mock_config.POSTGRES_USER,
        password=mock_config.POSTGRES_PASSWORD,
        database=mock_config.POSTGRES_DB,
    )

    # Simulate the end of the generator's scope
    try:
        await db_gen.aclose()
    except StopAsyncIteration:
        pass

    mock_connection.close.assert_called_once()
