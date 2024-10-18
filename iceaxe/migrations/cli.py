from inspect import isclass
from time import monotonic_ns

from iceaxe.base import DBModelMetaclass, TableBase
from iceaxe.io import resolve_package_path
from iceaxe.logging import CONSOLE
from iceaxe.migrations.client_io import fetch_migrations, sort_migrations
from iceaxe.migrations.generator import MigrationGenerator
from iceaxe.migrations.migrator import Migrator
from iceaxe.schemas.db_serializer import DatabaseSerializer
from iceaxe.session import DBConnection


async def handle_generate(
    package: str, db_connection: DBConnection, message: str | None = None
):
    """
    Creates a new migration definition file, comparing the previous version
    (if it exists) with the current schema.

    :param package: The current python package name. This should match the name of the
        project that's specified in pyproject.toml or setup.py.

    :param message: An optional message to include in the migration file. Helps
        with describing changes and searching for past migration logic over time.

    ```python {{sticky: True}}
    from iceaxe.migrations.cli import handle_generate
    from click import command, option

    @command()
    @option("--message", help="A message to include in the migration file.")
    def generate_migration(message: str):
        db_connection = DBConnection(...)
        handle_generate("my_project", db_connection, message=message)
    ```
    """

    CONSOLE.print("[bold blue]Generating migration to current schema")

    CONSOLE.print(
        "[grey58]Note that Iceaxe's migration support is well tested but still in beta."
    )
    CONSOLE.print(
        "[grey58]File an issue @ https://github.com/piercefreeman/iceaxe/issues if you encounter any problems."
    )

    # Locate the migrations directory that belongs to this project
    package_path = resolve_package_path(package)
    migrations_path = package_path / "migrations"

    # Create the path if it doesn't exist
    migrations_path.mkdir(exist_ok=True)
    if not (migrations_path / "__init__.py").exists():
        (migrations_path / "__init__.py").touch()

    # Get all of the instances that have been registered
    # in memory scope by the user.
    models = [
        cls
        for cls in DBModelMetaclass.get_registry()
        if isclass(cls) and issubclass(cls, TableBase)
    ]

    db_serializer = DatabaseSerializer()
    db_objects = []
    async for values in db_serializer.get_objects(db_connection):
        db_objects.append(values)

    migration_generator = MigrationGenerator()
    up_objects = list(migration_generator.serializer.delegate(models))

    # Get the current revision from the database, this should represent the "down" revision
    # for the new migration
    migrator = Migrator(db_connection)
    await migrator.init_db()
    current_revision = await migrator.get_active_revision()

    # Make sure there's not a duplicate revision that already have this down revision. If so that means
    # that we will have two conflicting migration chains
    migration_revisions = fetch_migrations(migrations_path)
    conflict_migrations = [
        migration
        for migration in migration_revisions
        if migration.down_revision == current_revision
    ]
    if conflict_migrations:
        up_revisions = {migration.up_revision for migration in conflict_migrations}
        raise ValueError(
            f"Found conflicting migrations with down revision {current_revision} (conflicts: {up_revisions}).\n"
            "If you're trying to generate a new migration, make sure to apply the previous migration first - or delete the old one and recreate."
        )

    migration_code, revision = await migration_generator.new_migration(
        db_objects,
        up_objects,
        down_revision=current_revision,
        user_message=message,
    )

    # Create the migration file. The change of a conflict with this timestamp is very low, but we make sure
    # not to override any existing files anyway.
    migration_file_path = migrations_path / f"rev_{revision}.py"
    if migration_file_path.exists():
        raise ValueError(
            f"Migration file {migration_file_path} already exists. Wait a second and try again."
        )

    migration_file_path.write_text(migration_code)

    CONSOLE.print(f"[bold green]New migration added: {migration_file_path.name}")


async def handle_apply(
    package: str,
    db_connection: DBConnection,
):
    """
    Applies all migrations that have not been applied to the database.

    :param package: The current python package name. This should match the name of the
        project that's specified in pyproject.toml or setup.py.

    """

    migrations_path = resolve_package_path(package) / "migrations"
    if not migrations_path.exists():
        raise ValueError(f"Migrations path {migrations_path} does not exist.")

    # Load all the migration files into memory and locate the subclasses of MigrationRevisionBase
    migration_revisions = fetch_migrations(migrations_path)
    migration_revisions = sort_migrations(migration_revisions)

    # Get the current revision from the database
    migrator = Migrator(db_connection)
    await migrator.init_db()
    current_revision = await migrator.get_active_revision()

    CONSOLE.print(f"Current revision: {current_revision}")

    # Find the item in the sequence that has down_revision equal to the current_revision
    # This indicates the next migration to apply
    next_migration_index = None
    for i, revision in enumerate(migration_revisions):
        if revision.down_revision == current_revision:
            next_migration_index = i
            break

    if next_migration_index is None:
        raise ValueError(
            f"Could not find a migration to apply after revision {current_revision}."
        )

    # Get the chain after this index, this should indicate the next migration to apply
    migration_chain = migration_revisions[next_migration_index:]
    CONSOLE.print(f"Applying {len(migration_chain)} migrations...")

    for migration in migration_chain:
        with CONSOLE.status(
            f"[bold blue]Applying {migration.up_revision}...", spinner="dots"
        ):
            start = monotonic_ns()
            await migration._handle_up(db_connection)

        CONSOLE.print(
            f"[bold green]🚀 Applied {migration.up_revision} in {(monotonic_ns() - start) / 1e9:.2f}s"
        )


async def handle_rollback(
    package: str,
    db_connection: DBConnection,
):
    """
    Rolls back the last migration that was applied to the database.

    :param package: The current python package name. This should match the name of the
        project that's specified in pyproject.toml or setup.py.

    """

    migrations_path = resolve_package_path(package) / "migrations"
    if not migrations_path.exists():
        raise ValueError(f"Migrations path {migrations_path} does not exist.")

    # Load all the migration files into memory and locate the subclasses of MigrationRevisionBase
    migration_revisions = fetch_migrations(migrations_path)
    migration_revisions = sort_migrations(migration_revisions)

    # Get the current revision from the database
    migrator = Migrator(db_connection)
    await migrator.init_db()
    current_revision = await migrator.get_active_revision()

    CONSOLE.print(f"Current revision: {current_revision}")

    # Find the item in the sequence that has down_revision equal to the current_revision
    # This indicates the next migration to apply
    this_migration_index = None
    for i, revision in enumerate(migration_revisions):
        if revision.up_revision == current_revision:
            this_migration_index = i
            break

    if this_migration_index is None:
        raise ValueError(
            f"Could not find a migration matching {current_revision} for rollback."
        )

    # Get the chain after this index, this should indicate the next migration to apply
    this_migration = migration_revisions[this_migration_index]

    with CONSOLE.status(
        f"[bold blue]Rolling back revision {this_migration.up_revision} to {this_migration.down_revision}...",
        spinner="dots",
    ):
        start = monotonic_ns()
        await this_migration._handle_down(db_connection)

    CONSOLE.print(
        f"[bold green]🪃 Rolled back migration to {this_migration.down_revision} in {(monotonic_ns() - start) / 1e9:.2f}s"
    )
