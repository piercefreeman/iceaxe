import asyncpg
import pytest
import pytest_asyncio

from iceaxe.base import DBModelMetaclass
from iceaxe.session import DBConnection


@pytest_asyncio.fixture
async def db_connection():
    conn = DBConnection(
        await asyncpg.connect(
            host="localhost",
            port=5438,
            user="iceaxe",
            password="mysecretpassword",
            database="iceaxe_test_db",
        )
    )

    # Drop all tables first to ensure clean state
    await conn.conn.execute("DROP TABLE IF EXISTS artifactdemo CASCADE")
    await conn.conn.execute("DROP TABLE IF EXISTS userdemo CASCADE")
    await conn.conn.execute("DROP TABLE IF EXISTS complexdemo CASCADE")
    await conn.conn.execute("DROP TABLE IF EXISTS article CASCADE")

    # Create tables
    await conn.conn.execute("""
        CREATE TABLE IF NOT EXISTS userdemo (
            id SERIAL PRIMARY KEY,
            name TEXT,
            email TEXT
        )
    """)

    await conn.conn.execute("""
        CREATE TABLE IF NOT EXISTS artifactdemo (
            id SERIAL PRIMARY KEY,
            title TEXT,
            user_id INT REFERENCES userdemo(id)
        )
    """)

    await conn.conn.execute("""
        CREATE TABLE IF NOT EXISTS complexdemo (
            id SERIAL PRIMARY KEY,
            string_list TEXT[],
            json_data JSON
        )
    """)

    await conn.conn.execute("""
        CREATE TABLE IF NOT EXISTS article (
            id SERIAL PRIMARY KEY,
            title TEXT,
            content TEXT,
            summary TEXT
        )
    """)

    # Create each index separately to handle errors better
    yield conn

    # Drop all tables after tests
    await conn.conn.execute("DROP TABLE IF EXISTS artifactdemo CASCADE")
    await conn.conn.execute("DROP TABLE IF EXISTS userdemo CASCADE")
    await conn.conn.execute("DROP TABLE IF EXISTS complexdemo CASCADE")
    await conn.conn.execute("DROP TABLE IF EXISTS article CASCADE")
    await conn.conn.close()


@pytest_asyncio.fixture()
async def indexed_db_connection(db_connection: DBConnection):
    await db_connection.conn.execute(
        "CREATE INDEX IF NOT EXISTS article_title_tsv_idx ON article USING GIN (to_tsvector('english', title))"
    )
    await db_connection.conn.execute(
        "CREATE INDEX IF NOT EXISTS article_content_tsv_idx ON article USING GIN (to_tsvector('english', content))"
    )
    await db_connection.conn.execute(
        "CREATE INDEX IF NOT EXISTS article_summary_tsv_idx ON article USING GIN (to_tsvector('english', summary))"
    )

    yield db_connection


@pytest_asyncio.fixture(autouse=True)
async def clear_table(db_connection):
    # Clear all tables and reset sequences
    await db_connection.conn.execute(
        "TRUNCATE TABLE userdemo, article RESTART IDENTITY CASCADE"
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
    """
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
    """
    )


@pytest.fixture
def clear_registry():
    current_registry = DBModelMetaclass._registry
    DBModelMetaclass._registry = []

    try:
        yield
    finally:
        DBModelMetaclass._registry = current_registry
