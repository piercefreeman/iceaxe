import sys
from pathlib import Path
from textwrap import dedent

import pytest

from iceaxe.migrations.client_io import fetch_migrations, sort_migrations


def write_revision(path: Path, up: str, down: str | None, header: str = ""):
    path.write_text(
        header
        + dedent(
            f"""
            from iceaxe.migrations.migration import MigrationRevisionBase
            from iceaxe.migrations.migrator import Migrator


            class MigrationRevision(MigrationRevisionBase):
                up_revision = "{up}"
                down_revision = {down!r}

                async def up(self, migrator: Migrator):
                    pass

                async def down(self, migrator: Migrator):
                    pass
            """
        )
    )


@pytest.fixture
def migrations_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """An importable `app.migrations` package, cleaned out of sys.modules afterwards."""
    package = tmp_path / "app" / "migrations"
    package.mkdir(parents=True)
    (tmp_path / "app" / "__init__.py").write_text("")
    (package / "__init__.py").write_text("")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(sys, "modules", dict(sys.modules))
    return package


def test_fetch_migrations_skips_imported_revisions(migrations_package: Path):
    write_revision(migrations_package / "rev_1.py", up="1", down=None)
    # A later migration that reuses an earlier one's up().
    write_revision(
        migrations_package / "rev_2.py",
        up="2",
        down="1",
        header="from app.migrations.rev_1 import MigrationRevision as Earlier\n",
    )

    migrations = fetch_migrations(migrations_package)

    assert sorted(migration.up_revision for migration in migrations) == ["1", "2"]
    assert [migration.up_revision for migration in sort_migrations(migrations)] == [
        "1",
        "2",
    ]
