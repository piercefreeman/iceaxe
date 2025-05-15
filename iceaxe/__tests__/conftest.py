import logging
import socket
import time
import uuid

import asyncpg
import docker
import pytest
import pytest_asyncio
from docker.errors import APIError

from iceaxe.base import DBModelMetaclass
from iceaxe.session import DBConnection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_free_port():
    """Find a free port on the host machine."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def docker_postgres():
    """
    Fixture that creates a PostgreSQL container using the Python Docker API.
    This allows running individual tests without needing Docker Compose.
    """
    # Initialize Docker client
    client = docker.from_env()

    # Generate a unique container name to avoid conflicts
    container_name = f"iceaxe-postgres-test-{uuid.uuid4().hex[:8]}"

    port = get_free_port()
    logger.info(f"Using port {port}")

    # PostgreSQL connection details
    pg_user = "iceaxe"
    pg_password = "mysecretpassword"
    pg_db = "iceaxe_test_db"

    # Start PostgreSQL container
    try:
        container = client.containers.run(
            "postgres:16",
            name=container_name,
            detach=True,
            environment={
                "POSTGRES_USER": pg_user,
                "POSTGRES_PASSWORD": pg_password,
                "POSTGRES_DB": pg_db,
            },
            ports={"5432/tcp": port},
            remove=True,  # Auto-remove container when stopped
        )
    except APIError as e:
        # If there's still an issue, try with a random port as a last resort
        if "port is already allocated" in str(e):
            logger.warning(
                f"Port {port} is still in use. Trying with a completely random port."
            )
            port = get_free_port()
            try:
                container = client.containers.run(
                    "postgres:16",
                    name=container_name,
                    detach=True,
                    environment={
                        "POSTGRES_USER": pg_user,
                        "POSTGRES_PASSWORD": pg_password,
                        "POSTGRES_DB": pg_db,
                    },
                    ports={"5432/tcp": port},
                    remove=True,
                )
            except Exception as inner_e:
                raise RuntimeError(
                    f"Failed to start PostgreSQL container with random port: {inner_e}"
                ) from e
        else:
            raise RuntimeError(f"Failed to start PostgreSQL container: {e}")

    # Wait for PostgreSQL to be ready
    max_retries = 30
    retry_interval = 1
    for i in range(max_retries):
        container.reload()  # Refresh container status
        if container.status != "running":
            raise RuntimeError(f"Container failed to start: {container.status}")

        # Try to connect to PostgreSQL
        try:
            conn = socket.create_connection(("localhost", port), timeout=1)
            conn.close()
            break
        except (socket.error, ConnectionRefusedError):
            if i == max_retries - 1:
                container.stop()
                raise RuntimeError("Failed to connect to PostgreSQL container")
            time.sleep(retry_interval)

    # Wait a bit more to ensure PostgreSQL is fully initialized
    time.sleep(2)

    # Yield connection details
    connection_info = {
        "host": "localhost",
        "port": port,
        "user": pg_user,
        "password": pg_password,
        "database": pg_db,
    }

    yield connection_info

    # Cleanup: stop the container
    try:
        container.stop()
    except Exception as e:
        logger.warning(f"Failed to stop container: {e}")


@pytest_asyncio.fixture
async def db_connection(docker_postgres):
    """
    Create a database connection using the PostgreSQL container.
    """
    conn = DBConnection(
        await asyncpg.connect(
            host=docker_postgres["host"],
            port=docker_postgres["port"],
            user=docker_postgres["user"],
            password=docker_postgres["password"],
            database=docker_postgres["database"],
        )
    )

    # Drop all tables first to ensure clean state
    await conn.conn.execute("DROP TABLE IF EXISTS artifactdemo CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS userdemo CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS complexdemo CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS article CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS employee CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS department CASCADE", timeout=30.0)
    await conn.conn.execute(
        "DROP TABLE IF EXISTS projectassignment CASCADE", timeout=30.0
    )
    await conn.conn.execute(
        "DROP TABLE IF EXISTS employeemetadata CASCADE", timeout=30.0
    )
    await conn.conn.execute(
        "DROP TABLE IF EXISTS functiondemomodel CASCADE", timeout=30.0
    )
    await conn.conn.execute("DROP TABLE IF EXISTS demomodela CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS demomodelb CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS jsondemo CASCADE", timeout=30.0)
    await conn.conn.execute(
        "DROP TABLE IF EXISTS complextypedemo CASCADE", timeout=30.0
    )
    await conn.conn.execute("DROP TYPE IF EXISTS statusenum CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TYPE IF EXISTS employeestatus CASCADE", timeout=30.0)

    # Create tables
    await conn.conn.execute(
        """
        CREATE TABLE IF NOT EXISTS userdemo (
            id SERIAL PRIMARY KEY,
            name TEXT,
            email TEXT
        )
    """,
        timeout=30.0,
    )

    await conn.conn.execute(
        """
        CREATE TABLE IF NOT EXISTS artifactdemo (
            id SERIAL PRIMARY KEY,
            title TEXT,
            user_id INT REFERENCES userdemo(id)
        )
    """,
        timeout=30.0,
    )

    await conn.conn.execute(
        """
        CREATE TABLE IF NOT EXISTS complexdemo (
            id SERIAL PRIMARY KEY,
            string_list TEXT[],
            json_data JSON
        )
    """,
        timeout=30.0,
    )

    await conn.conn.execute(
        """
        CREATE TABLE IF NOT EXISTS article (
            id SERIAL PRIMARY KEY,
            title TEXT,
            content TEXT,
            summary TEXT
        )
    """,
        timeout=30.0,
    )

    # Create each index separately to handle errors better
    yield conn

    # Drop all tables after tests
    await conn.conn.execute("DROP TABLE IF EXISTS artifactdemo CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS userdemo CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS complexdemo CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS article CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS employee CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS department CASCADE", timeout=30.0)
    await conn.conn.execute(
        "DROP TABLE IF EXISTS projectassignment CASCADE", timeout=30.0
    )
    await conn.conn.execute(
        "DROP TABLE IF EXISTS employeemetadata CASCADE", timeout=30.0
    )
    await conn.conn.execute(
        "DROP TABLE IF EXISTS functiondemomodel CASCADE", timeout=30.0
    )
    await conn.conn.execute("DROP TABLE IF EXISTS demomodela CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS demomodelb CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TABLE IF EXISTS jsondemo CASCADE", timeout=30.0)
    await conn.conn.execute(
        "DROP TABLE IF EXISTS complextypedemo CASCADE", timeout=30.0
    )
    await conn.conn.execute("DROP TYPE IF EXISTS statusenum CASCADE", timeout=30.0)
    await conn.conn.execute("DROP TYPE IF EXISTS employeestatus CASCADE", timeout=30.0)
    await conn.conn.close()


@pytest_asyncio.fixture()
async def indexed_db_connection(db_connection: DBConnection):
    await db_connection.conn.execute(
        "CREATE INDEX IF NOT EXISTS article_title_tsv_idx ON article USING GIN (to_tsvector('english', title))",
        timeout=30.0,
    )
    await db_connection.conn.execute(
        "CREATE INDEX IF NOT EXISTS article_content_tsv_idx ON article USING GIN (to_tsvector('english', content))",
        timeout=30.0,
    )
    await db_connection.conn.execute(
        "CREATE INDEX IF NOT EXISTS article_summary_tsv_idx ON article USING GIN (to_tsvector('english', summary))",
        timeout=30.0,
    )

    yield db_connection


@pytest_asyncio.fixture(autouse=True)
async def clear_table(db_connection):
    # Clear all tables and reset sequences
    await db_connection.conn.execute(
        "TRUNCATE TABLE userdemo, article RESTART IDENTITY CASCADE", timeout=30.0
    )


@pytest_asyncio.fixture
async def clear_all_database_objects(db_connection: DBConnection):
    """
    Clear all database objects.
    """
    # Step 1: Drop all tables in the public schema
    await db_connection.conn.execute(
        """
        DO $$ DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
            END LOOP;
        END $$;
    """,
        timeout=30.0,
    )

    # Step 2: Drop all custom types in the public schema
    await db_connection.conn.execute(
        """
        DO $$ DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT typname FROM pg_type WHERE typtype = 'e' AND typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')) LOOP
                EXECUTE 'DROP TYPE IF EXISTS ' || quote_ident(r.typname) || ' CASCADE';
            END LOOP;
        END $$;
    """,
        timeout=30.0,
    )


@pytest.fixture
def clear_registry():
    current_registry = DBModelMetaclass._registry
    DBModelMetaclass._registry = []

    try:
        yield
    finally:
        DBModelMetaclass._registry = current_registry
