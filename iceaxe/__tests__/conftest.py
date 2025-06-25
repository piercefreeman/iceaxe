import logging

import asyncpg
import pytest
import pytest_asyncio

from iceaxe.__tests__ import docker_helpers
from iceaxe.base import DBModelMetaclass
from iceaxe.session import DBConnection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def docker_postgres():
    """
    Fixture that creates a PostgreSQL container using the Python Docker API.
    This allows running individual tests without needing Docker Compose.
    """
    # Create and start a PostgreSQL container
    postgres_container = docker_helpers.PostgresContainer()

    # Start the container and yield connection details
    connection_info = postgres_container.start()
    yield connection_info

    # Cleanup: stop the container
    postgres_container.stop()


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
    known_tables = [
        "artifactdemo",
        "userdemo",
        "complexdemo",
        "article",
        "employee",
        "department",
        "projectassignment",
        "employeemetadata",
        "functiondemomodel",
        "demomodela",
        "demomodelb",
        "jsondemo",
        "complextypedemo",
    ]
    known_types = ["statusenum", "employeestatus"]

    for table in known_tables:
        await conn.conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE", timeout=30.0)

    for known_type in known_types:
        await conn.conn.execute(
            f"DROP TYPE IF EXISTS {known_type} CASCADE", timeout=30.0
        )

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
    for table in known_tables:
        await conn.conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE", timeout=30.0)

    # Drop all types after tests
    for known_type in known_types:
        await conn.conn.execute(
            f"DROP TYPE IF EXISTS {known_type} CASCADE", timeout=30.0
        )

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
