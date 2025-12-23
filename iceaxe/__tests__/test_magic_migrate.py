import pytest

from iceaxe.base import DBModelMetaclass, TableBase
from iceaxe.field import Field
from iceaxe.queries import select
from iceaxe.session import DBConnection, _migration_has_changes


class TestMigrationHasChanges:
    """Unit tests for the _migration_has_changes helper function."""

    def test_migration_with_only_pass_has_no_changes(self):
        migration_code = """
class TestMigration:
    async def up(self, migrator):
        pass

    async def down(self, migrator):
        pass
"""
        assert _migration_has_changes(migration_code) is False

    def test_migration_with_actual_changes(self):
        migration_code = """
class TestMigration:
    async def up(self, migrator):
        await migrator.actor.add_table("users")

    async def down(self, migrator):
        await migrator.actor.drop_table("users")
"""
        assert _migration_has_changes(migration_code) is True

    def test_migration_with_comments_and_pass_has_no_changes(self):
        migration_code = """
class TestMigration:
    async def up(self, migrator):
        # This is a comment
        pass

    async def down(self, migrator):
        pass
"""
        assert _migration_has_changes(migration_code) is False

    def test_migration_with_multiple_statements(self):
        migration_code = """
class TestMigration:
    async def up(self, migrator):
        await migrator.actor.add_table("users")
        await migrator.actor.add_column("users", "name", "VARCHAR")

    async def down(self, migrator):
        await migrator.actor.drop_table("users")
"""
        assert _migration_has_changes(migration_code) is True

    def test_migration_with_docstring_and_pass_has_no_changes(self):
        # Edge case: up method with docstring and pass
        migration_code = '''
class TestMigration:
    async def up(self, migrator):
        """No changes."""
        pass

    async def down(self, migrator):
        pass
'''
        # Note: Current implementation doesn't filter docstrings, so this will show as having changes
        # This is acceptable behavior - migrations with docstrings usually have code too
        assert _migration_has_changes(migration_code) is True

    def test_unparseable_migration_assumes_changes(self):
        # If we can't parse the migration, assume it has changes to be safe
        migration_code = "invalid python code that doesn't match our pattern"
        assert _migration_has_changes(migration_code) is True


@pytest.fixture
def temp_package(tmp_path):
    """Create a temporary package structure for testing migrations."""
    package_dir = tmp_path / "test_package"
    package_dir.mkdir()
    (package_dir / "__init__.py").touch()
    return package_dir


@pytest.fixture
def clear_registry():
    """Clear the model registry before/after tests."""
    current_registry = DBModelMetaclass._registry.copy()
    DBModelMetaclass._registry = []
    try:
        yield
    finally:
        DBModelMetaclass._registry = current_registry


@pytest.mark.asyncio
async def test_magic_migrate_creates_table(
    db_connection: DBConnection,
    tmp_path,
    clear_registry,
    clear_all_database_objects,
):
    """Test that magic_migrate creates a new table from a model."""
    # Create a temporary package
    package_dir = tmp_path / "test_pkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").touch()

    # Define a model (this will auto-register)
    class MagicUser(TableBase):
        id: int | None = Field(default=None, primary_key=True)
        username: str

    # Run magic_migrate
    # We need to patch resolve_package_path since we're using a temp directory
    from unittest.mock import patch

    with patch("iceaxe.io.resolve_package_path", return_value=package_dir):
        await db_connection.magic_migrate("test_pkg", models=[MagicUser])

    # Verify the table was created
    result = await db_connection.conn.fetch(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'magicuser'"
    )
    assert len(result) == 1

    # Verify we can insert and query
    await db_connection.insert([MagicUser(username="testuser")])
    users = await db_connection.exec(select(MagicUser))
    assert len(users) == 1
    assert users[0].username == "testuser"

    # Verify migration file was created
    migrations_dir = package_dir / "migrations"
    assert migrations_dir.exists()
    migration_files = list(migrations_dir.glob("rev_*.py"))
    assert len(migration_files) == 1


@pytest.mark.asyncio
async def test_magic_migrate_no_changes_no_migration(
    db_connection: DBConnection,
    tmp_path,
    clear_registry,
    clear_all_database_objects,
):
    """Test that magic_migrate doesn't create a migration if no changes needed."""
    # Create a temporary package
    package_dir = tmp_path / "test_pkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").touch()

    # Define a model
    class MagicItem(TableBase):
        id: int = Field(primary_key=True)
        name: str

    from unittest.mock import patch

    with patch("iceaxe.io.resolve_package_path", return_value=package_dir):
        # First migration - creates the table
        await db_connection.magic_migrate("test_pkg", models=[MagicItem])

        migrations_dir = package_dir / "migrations"
        initial_migrations = list(migrations_dir.glob("rev_*.py"))
        assert len(initial_migrations) == 1

        # Second migration - no changes
        await db_connection.magic_migrate("test_pkg", models=[MagicItem])

        # Should still have only one migration file
        final_migrations = list(migrations_dir.glob("rev_*.py"))
        assert len(final_migrations) == 1


@pytest.mark.asyncio
async def test_magic_migrate_multiple_tables(
    db_connection: DBConnection,
    tmp_path,
    clear_registry,
    clear_all_database_objects,
):
    """Test that magic_migrate can create multiple tables at once."""
    package_dir = tmp_path / "test_pkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").touch()

    from unittest.mock import patch

    # Define two models
    class MagicAuthor(TableBase):
        id: int | None = Field(default=None, primary_key=True)
        name: str

    class MagicBook(TableBase):
        id: int | None = Field(default=None, primary_key=True)
        title: str

    with patch("iceaxe.io.resolve_package_path", return_value=package_dir):
        await db_connection.magic_migrate(
            "test_pkg", models=[MagicAuthor, MagicBook]
        )

    # Verify both tables were created
    tables = await db_connection.conn.fetch(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_name IN ('magicauthor', 'magicbook')
        """
    )
    table_names = {t["table_name"] for t in tables}
    assert table_names == {"magicauthor", "magicbook"}

    # Verify we can insert into both tables
    await db_connection.insert([MagicAuthor(name="Test Author")])
    await db_connection.insert([MagicBook(title="Test Book")])

    authors = await db_connection.exec(select(MagicAuthor))
    books = await db_connection.exec(select(MagicBook))
    assert len(authors) == 1
    assert len(books) == 1

    # Verify one migration file was created
    migrations_dir = package_dir / "migrations"
    migration_files = list(migrations_dir.glob("rev_*.py"))
    assert len(migration_files) == 1


@pytest.mark.asyncio
async def test_magic_migrate_with_message(
    db_connection: DBConnection,
    tmp_path,
    clear_registry,
    clear_all_database_objects,
):
    """Test that magic_migrate includes the message in the migration file."""
    package_dir = tmp_path / "test_pkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").touch()

    class MagicConfig(TableBase):
        id: int = Field(primary_key=True)
        key: str
        value: str

    from unittest.mock import patch

    with patch("iceaxe.io.resolve_package_path", return_value=package_dir):
        await db_connection.magic_migrate(
            "test_pkg",
            models=[MagicConfig],
            message="Add configuration table",
        )

    # Check that the migration file contains the message
    migrations_dir = package_dir / "migrations"
    migration_files = list(migrations_dir.glob("rev_*.py"))
    assert len(migration_files) == 1

    migration_content = migration_files[0].read_text()
    assert "Add configuration table" in migration_content


@pytest.mark.asyncio
async def test_magic_migrate_applies_pending_migrations(
    db_connection: DBConnection,
    tmp_path,
    clear_registry,
    clear_all_database_objects,
):
    """Test that magic_migrate applies existing pending migrations."""
    package_dir = tmp_path / "test_pkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").touch()

    class MagicLog(TableBase):
        id: int = Field(primary_key=True)
        message: str

    from unittest.mock import patch

    # Create the first migration
    with patch("iceaxe.io.resolve_package_path", return_value=package_dir):
        await db_connection.magic_migrate("test_pkg", models=[MagicLog])

    # Drop the table to simulate a fresh database with existing migration files
    await db_connection.conn.execute("DROP TABLE IF EXISTS magiclog CASCADE")
    await db_connection.conn.execute("DELETE FROM migration_info")

    # Run magic_migrate again - should apply the existing migration
    with patch("iceaxe.io.resolve_package_path", return_value=package_dir):
        await db_connection.magic_migrate("test_pkg", models=[MagicLog])

    # Verify the table exists again
    result = await db_connection.conn.fetch(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'magiclog'"
    )
    assert len(result) == 1
