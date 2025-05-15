from contextlib import asynccontextmanager
from enum import StrEnum
from typing import Any, Type
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
from asyncpg.connection import Connection

from iceaxe.__tests__.conf_models import (
    ArtifactDemo,
    ComplexDemo,
    DemoModelA,
    DemoModelB,
    JsonDemo,
    UserDemo,
)
from iceaxe.base import INTERNAL_TABLE_FIELDS, TableBase
from iceaxe.field import Field
from iceaxe.functions import func
from iceaxe.queries import QueryBuilder
from iceaxe.schemas.cli import create_all
from iceaxe.session import (
    PG_MAX_PARAMETERS,
    TYPE_CACHE,
    DBConnection,
)
from iceaxe.typing import column

#
# Insert / Update / Delete with ORM objects
#


@pytest.mark.asyncio
async def test_db_connection_insert(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    result = await db_connection.conn.fetch(
        "SELECT * FROM userdemo WHERE name = $1", "John Doe"
    )
    assert len(result) == 1
    assert result[0]["id"] == user.id
    assert result[0]["name"] == "John Doe"
    assert result[0]["email"] == "john@example.com"
    assert user.get_modified_attributes() == {}


@pytest.mark.asyncio
async def test_db_connection_update(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    user.name = "Jane Doe"
    await db_connection.update([user])

    result = await db_connection.conn.fetch(
        "SELECT * FROM userdemo WHERE id = $1", user.id
    )
    assert len(result) == 1
    assert result[0]["name"] == "Jane Doe"
    assert user.get_modified_attributes() == {}


@pytest.mark.asyncio
async def test_db_obj_mixin_track_modifications():
    user = UserDemo(name="John Doe", email="john@example.com")
    assert user.get_modified_attributes() == {}

    user.name = "Jane Doe"
    assert user.get_modified_attributes() == {"name": "Jane Doe"}

    user.email = "jane@example.com"
    assert user.get_modified_attributes() == {
        "name": "Jane Doe",
        "email": "jane@example.com",
    }

    user.clear_modified_attributes()
    assert user.get_modified_attributes() == {}


@pytest.mark.asyncio
async def test_db_connection_delete_query(db_connection: DBConnection):
    userdemo = [
        UserDemo(name="John Doe", email="john@example.com"),
        UserDemo(name="Jane Doe", email="jane@example.com"),
    ]

    await db_connection.insert(userdemo)

    query = QueryBuilder().delete(UserDemo).where(UserDemo.name == "John Doe")
    await db_connection.exec(query)

    result = await db_connection.conn.fetch("SELECT * FROM userdemo")
    assert len(result) == 1


@pytest.mark.asyncio
async def test_db_connection_insert_multiple(db_connection: DBConnection):
    userdemo = [
        UserDemo(name="John Doe", email="john@example.com"),
        UserDemo(name="Jane Doe", email="jane@example.com"),
    ]

    await db_connection.insert(userdemo)

    result = await db_connection.conn.fetch("SELECT * FROM userdemo ORDER BY id")
    assert len(result) == 2
    assert result[0]["name"] == "John Doe"
    assert result[1]["name"] == "Jane Doe"
    assert userdemo[0].id == result[0]["id"]
    assert userdemo[1].id == result[1]["id"]
    assert all(user.get_modified_attributes() == {} for user in userdemo)


@pytest.mark.asyncio
async def test_db_connection_update_multiple(db_connection: DBConnection):
    userdemo = [
        UserDemo(name="John Doe", email="john@example.com"),
        UserDemo(name="Jane Doe", email="jane@example.com"),
    ]
    await db_connection.insert(userdemo)

    userdemo[0].name = "Johnny Doe"
    userdemo[1].email = "janey@example.com"

    await db_connection.update(userdemo)

    result = await db_connection.conn.fetch("SELECT * FROM userdemo ORDER BY id")
    assert len(result) == 2
    assert result[0]["name"] == "Johnny Doe"
    assert result[1]["email"] == "janey@example.com"
    assert all(user.get_modified_attributes() == {} for user in userdemo)


@pytest.mark.asyncio
async def test_db_connection_insert_empty_list(db_connection: DBConnection):
    await db_connection.insert([])
    result = await db_connection.conn.fetch("SELECT * FROM userdemo")
    assert len(result) == 0


@pytest.mark.asyncio
async def test_db_connection_update_empty_list(db_connection: DBConnection):
    await db_connection.update([])
    # This test doesn't really assert anything, as an empty update shouldn't change the database


@pytest.mark.asyncio
async def test_db_connection_update_no_modifications(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    await db_connection.update([user])

    result = await db_connection.conn.fetch(
        "SELECT * FROM userdemo WHERE id = $1", user.id
    )
    assert len(result) == 1
    assert result[0]["name"] == "John Doe"
    assert result[0]["email"] == "john@example.com"


@pytest.mark.asyncio
async def test_delete_object(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    result = await db_connection.conn.fetch(
        "SELECT * FROM userdemo WHERE id = $1", user.id
    )
    assert len(result) == 1

    await db_connection.delete([user])

    result = await db_connection.conn.fetch(
        "SELECT * FROM userdemo WHERE id = $1", user.id
    )
    assert len(result) == 0


#
# Select into ORM objects
#


@pytest.mark.asyncio
async def test_select(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    # Table selection
    result_1 = await db_connection.exec(QueryBuilder().select(UserDemo))
    assert result_1 == [UserDemo(id=user.id, name="John Doe", email="john@example.com")]

    # Single column selection
    result_2 = await db_connection.exec(QueryBuilder().select(UserDemo.email))
    assert result_2 == ["john@example.com"]

    # Multiple column selection
    result_3 = await db_connection.exec(
        QueryBuilder().select((UserDemo.name, UserDemo.email))
    )
    assert result_3 == [("John Doe", "john@example.com")]

    # Table and column selection
    result_4 = await db_connection.exec(
        QueryBuilder().select((UserDemo, UserDemo.email))
    )
    assert result_4 == [
        (
            UserDemo(id=user.id, name="John Doe", email="john@example.com"),
            "john@example.com",
        )
    ]


@pytest.mark.asyncio
async def test_is_null(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    # Table selection
    result_1 = await db_connection.exec(
        QueryBuilder()
        .select(UserDemo)
        .where(
            UserDemo.id == None,  # noqa: E711
        )
    )
    assert result_1 == []

    # Single column selection
    result_2 = await db_connection.exec(
        QueryBuilder()
        .select(UserDemo)
        .where(
            UserDemo.id != None,  # noqa: E711
        )
    )
    assert result_2 == [UserDemo(id=user.id, name="John Doe", email="john@example.com")]


@pytest.mark.asyncio
async def test_select_complex(db_connection: DBConnection):
    """
    Ensure that we can serialize the complex types.

    """
    complex_obj = ComplexDemo(id=1, string_list=["a", "b", "c"], json_data={"a": "a"})
    await db_connection.insert([complex_obj])

    # Table selection
    result = await db_connection.exec(QueryBuilder().select(ComplexDemo))
    assert result == [
        ComplexDemo(id=1, string_list=["a", "b", "c"], json_data={"a": "a"})
    ]


@pytest.mark.asyncio
async def test_select_where(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    new_query = QueryBuilder().select(UserDemo).where(UserDemo.name == "John Doe")
    result = await db_connection.exec(new_query)
    assert result == [
        UserDemo(id=user.id, name="John Doe", email="john@example.com"),
    ]


@pytest.mark.asyncio
async def test_select_join(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])
    assert user.id is not None

    artifact = ArtifactDemo(title="Artifact 1", user_id=user.id)
    await db_connection.insert([artifact])

    new_query = (
        QueryBuilder()
        .select((ArtifactDemo, UserDemo.email))
        .join(UserDemo, UserDemo.id == ArtifactDemo.user_id)
        .where(UserDemo.name == "John Doe")
    )
    result = await db_connection.exec(new_query)
    assert result == [
        (
            ArtifactDemo(id=artifact.id, title="Artifact 1", user_id=user.id),
            "john@example.com",
        )
    ]


@pytest.mark.asyncio
async def test_select_join_multiple_tables(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])
    assert user.id is not None

    artifact = ArtifactDemo(title="Artifact 1", user_id=user.id)
    await db_connection.insert([artifact])

    new_query = (
        QueryBuilder()
        .select((ArtifactDemo, UserDemo))
        .join(UserDemo, UserDemo.id == ArtifactDemo.user_id)
        .where(UserDemo.name == "John Doe")
    )
    result = await db_connection.exec(new_query)
    assert result == [
        (
            ArtifactDemo(id=artifact.id, title="Artifact 1", user_id=user.id),
            UserDemo(id=user.id, name="John Doe", email="john@example.com"),
        )
    ]


@pytest.mark.asyncio
async def test_select_with_limit_and_offset(db_connection: DBConnection):
    users = [
        UserDemo(name="User 1", email="user1@example.com"),
        UserDemo(name="User 2", email="user2@example.com"),
        UserDemo(name="User 3", email="user3@example.com"),
        UserDemo(name="User 4", email="user4@example.com"),
        UserDemo(name="User 5", email="user5@example.com"),
    ]
    await db_connection.insert(users)

    query = (
        QueryBuilder().select(UserDemo).order_by(UserDemo.id, "ASC").limit(2).offset(1)
    )
    result = await db_connection.exec(query)
    assert len(result) == 2
    assert result[0].name == "User 2"
    assert result[1].name == "User 3"


@pytest.mark.asyncio
async def test_select_with_multiple_where_conditions(db_connection: DBConnection):
    users = [
        UserDemo(name="John Doe", email="john@example.com"),
        UserDemo(name="Jane Doe", email="jane@example.com"),
        UserDemo(name="Bob Smith", email="bob@example.com"),
    ]
    await db_connection.insert(users)

    query = (
        QueryBuilder()
        .select(UserDemo)
        .where(
            column(UserDemo.name).like("%Doe%"), UserDemo.email != "john@example.com"
        )
    )
    result = await db_connection.exec(query)
    assert len(result) == 1
    assert result[0].name == "Jane Doe"


@pytest.mark.asyncio
async def test_select_with_list_filter(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    result = await db_connection.exec(
        QueryBuilder()
        .select(UserDemo)
        .where(
            column(UserDemo.name).in_(["John Doe"]),
        )
    )
    assert result == [UserDemo(id=user.id, name="John Doe", email="john@example.com")]

    result = await db_connection.exec(
        QueryBuilder()
        .select(UserDemo)
        .where(
            column(UserDemo.name).not_in(["John A"]),
        )
    )
    assert result == [UserDemo(id=user.id, name="John Doe", email="john@example.com")]


@pytest.mark.asyncio
async def test_select_with_order_by_multiple_columns(db_connection: DBConnection):
    users = [
        UserDemo(name="Alice", email="alice@example.com"),
        UserDemo(name="Bob", email="bob@example.com"),
        UserDemo(name="Charlie", email="charlie@example.com"),
        UserDemo(name="Alice", email="alice2@example.com"),
    ]
    await db_connection.insert(users)

    query = (
        QueryBuilder()
        .select(UserDemo)
        .order_by(UserDemo.name, "ASC")
        .order_by(UserDemo.email, "ASC")
    )
    result = await db_connection.exec(query)
    assert len(result) == 4
    assert result[0].name == "Alice" and result[0].email == "alice2@example.com"
    assert result[1].name == "Alice" and result[1].email == "alice@example.com"
    assert result[2].name == "Bob"
    assert result[3].name == "Charlie"


@pytest.mark.asyncio
async def test_select_with_group_by_and_having(db_connection: DBConnection):
    users = [
        UserDemo(name="John", email="john@example.com"),
        UserDemo(name="Jane", email="jane@example.com"),
        UserDemo(name="John", email="john2@example.com"),
        UserDemo(name="Bob", email="bob@example.com"),
    ]
    await db_connection.insert(users)

    query = (
        QueryBuilder()
        .select((UserDemo.name, func.count(UserDemo.id)))
        .group_by(UserDemo.name)
        .having(func.count(UserDemo.id) > 1)
    )
    result = await db_connection.exec(query)
    assert len(result) == 1
    assert result[0] == ("John", 2)


@pytest.mark.asyncio
async def test_select_with_left_join(db_connection: DBConnection):
    users = [
        UserDemo(name="John", email="john@example.com"),
        UserDemo(name="Jane", email="jane@example.com"),
    ]
    await db_connection.insert(users)

    posts = [
        ArtifactDemo(title="John's Post", user_id=users[0].id),
        ArtifactDemo(title="Another Post", user_id=users[0].id),
    ]
    await db_connection.insert(posts)

    query = (
        QueryBuilder()
        .select((UserDemo.name, func.count(ArtifactDemo.id)))
        .join(ArtifactDemo, UserDemo.id == ArtifactDemo.user_id, "LEFT")
        .group_by(UserDemo.name)
        .order_by(UserDemo.name, "ASC")
    )
    result = await db_connection.exec(query)
    assert len(result) == 2
    assert result[0] == ("Jane", 0)
    assert result[1] == ("John", 2)


@pytest.mark.asyncio
async def test_select_with_left_join_object(db_connection: DBConnection):
    users = [
        UserDemo(name="John", email="john@example.com"),
        UserDemo(name="Jane", email="jane@example.com"),
    ]
    await db_connection.insert(users)

    posts = [
        ArtifactDemo(title="John's Post", user_id=users[0].id),
        ArtifactDemo(title="Another Post", user_id=users[0].id),
    ]
    await db_connection.insert(posts)

    query = (
        QueryBuilder()
        .select((UserDemo, ArtifactDemo))
        .join(ArtifactDemo, UserDemo.id == ArtifactDemo.user_id, "LEFT")
    )
    result = await db_connection.exec(query)
    assert len(result) == 3
    assert result[0] == (users[0], posts[0])
    assert result[1] == (users[0], posts[1])
    assert result[2] == (users[1], None)


# @pytest.mark.asyncio
# async def test_select_with_subquery(db_connection: DBConnection):
#     users = [
#         UserDemo(name="John", email="john@example.com"),
#         UserDemo(name="Jane", email="jane@example.com"),
#         UserDemo(name="Bob", email="bob@example.com"),
#     ]
#     await db_connection.insert(users)

#     posts = [
#         ArtifactDemo(title="John's Post", content="Hello", user_id=users[0].id),
#         ArtifactDemo(title="Jane's Post", content="World", user_id=users[1].id),
#         ArtifactDemo(title="John's Second Post", content="!", user_id=users[0].id),
#     ]
#     await db_connection.insert(posts)

#     subquery = QueryBuilder().select(ArtifactDemo.user_id).where(func.count(ArtifactDemo.id) > 1).group_by(PostDemo.user_id)
#     query = QueryBuilder().select(UserDemo).where(is_column(UserDemo.id).in_(subquery))
#     result = await db_connection.exec(query)
#     assert len(result) == 1
#     assert result[0].name == "John"


@pytest.mark.asyncio
async def test_select_with_distinct(db_connection: DBConnection):
    users = [
        UserDemo(name="John", email="john@example.com"),
        UserDemo(name="Jane", email="jane@example.com"),
        UserDemo(name="John", email="john2@example.com"),
    ]
    await db_connection.insert(users)

    query = (
        QueryBuilder()
        .select(func.distinct(UserDemo.name))
        .order_by(UserDemo.name, "ASC")
    )
    result = await db_connection.exec(query)
    assert result == ["Jane", "John"]


@pytest.mark.asyncio
async def test_refresh(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    # Update the user with a manual SQL query to simulate another process
    # doing an update
    await db_connection.conn.execute(
        "UPDATE userdemo SET name = 'Jane Doe' WHERE id = $1", user.id
    )

    # The user object in memory should still have the old name
    assert user.name == "John Doe"

    # Refreshing the user object from the database should pull the
    # new attributes
    await db_connection.refresh([user])
    assert user.name == "Jane Doe"


@pytest.mark.asyncio
async def test_get(db_connection: DBConnection):
    """
    Test retrieving a single record by primary key using the get method.
    """
    # Create a test user
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])
    assert user.id is not None

    # Test successful get
    retrieved_user = await db_connection.get(UserDemo, user.id)
    assert retrieved_user is not None
    assert retrieved_user.id == user.id
    assert retrieved_user.name == "John Doe"
    assert retrieved_user.email == "john@example.com"

    # Test get with non-existent ID
    non_existent = await db_connection.get(UserDemo, 9999)
    assert non_existent is None


@pytest.mark.asyncio
async def test_db_connection_insert_update_enum(db_connection: DBConnection):
    """
    Test that casting enum types with is working for both insert and updates.

    """

    class EnumValue(StrEnum):
        A = "a"
        B = "b"

    class EnumDemo(TableBase):
        id: int | None = Field(default=None, primary_key=True)
        value: EnumValue

    # Clear out previous tables
    await db_connection.conn.execute("DROP TABLE IF EXISTS enumdemo")
    await db_connection.conn.execute("DROP TYPE IF EXISTS enumvalue")
    await create_all(db_connection, [EnumDemo])

    userdemo = EnumDemo(value=EnumValue.A)
    await db_connection.insert([userdemo])

    result = await db_connection.conn.fetch("SELECT * FROM enumdemo")
    assert len(result) == 1
    assert result[0]["value"] == "a"

    userdemo.value = EnumValue.B
    await db_connection.update([userdemo])

    result = await db_connection.conn.fetch("SELECT * FROM enumdemo")
    assert len(result) == 1
    assert result[0]["value"] == "b"


#
# Upsert
#


@pytest.mark.asyncio
async def test_upsert_basic_insert(db_connection: DBConnection):
    """
    Test basic insert when no conflict exists

    """
    await db_connection.conn.execute(
        """
        ALTER TABLE userdemo
        ADD CONSTRAINT email_unique UNIQUE (email)
        """
    )

    user = UserDemo(name="John Doe", email="john@example.com")
    result = await db_connection.upsert(
        [user],
        conflict_fields=(UserDemo.email,),
        returning_fields=(UserDemo.id, UserDemo.name, UserDemo.email),
    )

    assert result is not None
    assert len(result) == 1
    assert result[0][1] == "John Doe"
    assert result[0][2] == "john@example.com"

    # Verify in database
    db_result = await db_connection.conn.fetch("SELECT * FROM userdemo")
    assert len(db_result) == 1
    assert db_result[0][1] == "John Doe"


@pytest.mark.asyncio
async def test_upsert_update_on_conflict(db_connection: DBConnection):
    """
    Test update when conflict exists

    """
    await db_connection.conn.execute(
        """
        ALTER TABLE userdemo
        ADD CONSTRAINT email_unique UNIQUE (email)
        """
    )

    # First insert
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    # Attempt upsert with same email but different name
    new_user = UserDemo(name="Johnny Doe", email="john@example.com")
    result = await db_connection.upsert(
        [new_user],
        conflict_fields=(UserDemo.email,),
        update_fields=(UserDemo.name,),
        returning_fields=(UserDemo.id, UserDemo.name, UserDemo.email),
    )

    assert result is not None
    assert len(result) == 1
    assert result[0][1] == "Johnny Doe"

    # Verify only one record exists
    db_result = await db_connection.conn.fetch("SELECT * FROM userdemo")
    assert len(db_result) == 1
    assert db_result[0]["name"] == "Johnny Doe"


@pytest.mark.asyncio
async def test_upsert_do_nothing_on_conflict(db_connection: DBConnection):
    """
    Test DO NOTHING behavior when no update_fields specified

    """
    await db_connection.conn.execute(
        """
        ALTER TABLE userdemo
        ADD CONSTRAINT email_unique UNIQUE (email)
        """
    )

    # First insert
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    # Attempt upsert with same email but different name
    new_user = UserDemo(name="Johnny Doe", email="john@example.com")
    result = await db_connection.upsert(
        [new_user],
        conflict_fields=(UserDemo.email,),
        returning_fields=(UserDemo.id, UserDemo.name, UserDemo.email),
    )

    # Should return empty list as no update was performed
    assert result == []

    # Verify original record unchanged
    db_result = await db_connection.conn.fetch("SELECT * FROM userdemo")
    assert len(db_result) == 1
    assert db_result[0][1] == "John Doe"


@pytest.mark.asyncio
async def test_upsert_multiple_objects(db_connection: DBConnection):
    """
    Test upserting multiple objects at once

    """
    await db_connection.conn.execute(
        """
        ALTER TABLE userdemo
        ADD CONSTRAINT email_unique UNIQUE (email)
        """
    )

    users = [
        UserDemo(name="John Doe", email="john@example.com"),
        UserDemo(name="Jane Doe", email="jane@example.com"),
    ]
    result = await db_connection.upsert(
        users,
        conflict_fields=(UserDemo.email,),
        returning_fields=(UserDemo.name, UserDemo.email),
    )

    assert result is not None
    assert len(result) == 2
    assert {r[1] for r in result} == {"john@example.com", "jane@example.com"}


@pytest.mark.asyncio
async def test_upsert_empty_list(db_connection: DBConnection):
    await db_connection.conn.execute(
        """
        ALTER TABLE userdemo
        ADD CONSTRAINT email_unique UNIQUE (email)
        """
    )

    """Test upserting an empty list"""
    result = await db_connection.upsert(
        [], conflict_fields=(UserDemo.email,), returning_fields=(UserDemo.id,)
    )
    assert result is None


@pytest.mark.asyncio
async def test_upsert_multiple_conflict_fields(db_connection: DBConnection):
    """
    Test upserting with multiple conflict fields

    """
    await db_connection.conn.execute(
        """
        ALTER TABLE userdemo
        ADD CONSTRAINT email_unique UNIQUE (name, email)
        """
    )

    users = [
        UserDemo(name="John Doe", email="john@example.com"),
        UserDemo(name="John Doe", email="john@example.com"),
        UserDemo(name="Jane Doe", email="jane@example.com"),
    ]
    result = await db_connection.upsert(
        users,
        conflict_fields=(UserDemo.name, UserDemo.email),
        returning_fields=(UserDemo.name, UserDemo.email),
    )

    assert result is not None
    assert len(result) == 2
    assert {r[1] for r in result} == {"john@example.com", "jane@example.com"}


@pytest.mark.asyncio
async def test_for_update_prevents_concurrent_modification(
    db_connection: DBConnection, docker_postgres
):
    """
    Test that FOR UPDATE actually locks the row for concurrent modifications.
    """
    # Create initial user
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    async with db_connection.transaction():
        # Lock the row with FOR UPDATE
        [locked_user] = await db_connection.exec(
            QueryBuilder().select(UserDemo).where(UserDemo.id == user.id).for_update()
        )
        assert locked_user.name == "John Doe"

        # Try to update from another connection - this should block
        # until our transaction is done
        other_conn = DBConnection(
            await asyncpg.connect(
                host=docker_postgres["host"],
                port=docker_postgres["port"],
                user=docker_postgres["user"],
                password=docker_postgres["password"],
                database=docker_postgres["database"],
            )
        )
        try:
            with pytest.raises(asyncpg.exceptions.LockNotAvailableError):
                # This should raise an error since we're using NOWAIT
                await other_conn.exec(
                    QueryBuilder()
                    .select(UserDemo)
                    .where(UserDemo.id == user.id)
                    .for_update(nowait=True)
                )
        finally:
            await other_conn.conn.close()


@pytest.mark.asyncio
async def test_for_update_skip_locked(db_connection: DBConnection, docker_postgres):
    """
    Test that SKIP LOCKED works as expected.
    """
    # Create test users
    users = [
        UserDemo(name="User 1", email="user1@example.com"),
        UserDemo(name="User 2", email="user2@example.com"),
    ]
    await db_connection.insert(users)

    async with db_connection.transaction():
        # Lock the first user
        [locked_user] = await db_connection.exec(
            QueryBuilder()
            .select(UserDemo)
            .where(UserDemo.id == users[0].id)
            .for_update()
        )
        assert locked_user.name == "User 1"

        # From another connection, try to select both users with SKIP LOCKED
        other_conn = DBConnection(
            await asyncpg.connect(
                host=docker_postgres["host"],
                port=docker_postgres["port"],
                user=docker_postgres["user"],
                password=docker_postgres["password"],
                database=docker_postgres["database"],
            )
        )
        try:
            # This should only return User 2 since User 1 is locked
            result = await other_conn.exec(
                QueryBuilder()
                .select(UserDemo)
                .order_by(UserDemo.id, "ASC")
                .for_update(skip_locked=True)
            )
            assert len(result) == 1
            assert result[0].name == "User 2"
        finally:
            await other_conn.conn.close()


@pytest.mark.asyncio
async def test_for_update_of_with_join(db_connection: DBConnection, docker_postgres):
    """
    Test FOR UPDATE OF with JOINed tables.
    """
    # Create test data
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    artifact = ArtifactDemo(title="Test Artifact", user_id=user.id)
    await db_connection.insert([artifact])

    async with db_connection.transaction():
        # Lock only the artifacts table in a join query
        [(selected_artifact, selected_user)] = await db_connection.exec(
            QueryBuilder()
            .select((ArtifactDemo, UserDemo))
            .join(UserDemo, UserDemo.id == ArtifactDemo.user_id)
            .for_update(of=(ArtifactDemo,))
        )
        assert selected_artifact.title == "Test Artifact"
        assert selected_user.name == "John Doe"

        # In another connection, we should be able to lock the user
        # but not the artifact
        other_conn = DBConnection(
            await asyncpg.connect(
                host=docker_postgres["host"],
                port=docker_postgres["port"],
                user=docker_postgres["user"],
                password=docker_postgres["password"],
                database=docker_postgres["database"],
            )
        )
        try:
            # Should succeed since user table isn't locked
            [other_user] = await other_conn.exec(
                QueryBuilder()
                .select(UserDemo)
                .where(UserDemo.id == user.id)
                .for_update(nowait=True)
            )
            assert other_user.name == "John Doe"

            # Should fail since artifact table is locked
            with pytest.raises(asyncpg.exceptions.LockNotAvailableError):
                await other_conn.exec(
                    QueryBuilder()
                    .select(ArtifactDemo)
                    .where(ArtifactDemo.id == artifact.id)
                    .for_update(nowait=True)
                )
                pytest.fail("Should have raised an error")
        finally:
            await other_conn.conn.close()


@pytest.mark.asyncio
async def test_select_same_column_name_from_different_tables(
    db_connection: DBConnection,
):
    """
    Test that we can correctly select and distinguish between columns with the same name
    from different tables. Both tables have a 'name' column to verify proper disambiguation.
    """
    # Create tables first
    await db_connection.conn.execute("DROP TABLE IF EXISTS demomodela")
    await db_connection.conn.execute("DROP TABLE IF EXISTS demomodelb")
    await create_all(db_connection, [DemoModelA, DemoModelB])

    # Create test data
    model_a = DemoModelA(name="Name from A", description="Description A", code="ABC123")
    model_b = DemoModelB(
        name="Name from B",
        category="Category B",
        code="ABC123",  # Same code to join on
    )
    await db_connection.insert([model_a, model_b])

    # Select both name columns and verify they are correctly distinguished
    query = (
        QueryBuilder()
        .select((DemoModelA.name, DemoModelB.name))
        .join(DemoModelB, DemoModelA.code == DemoModelB.code)
    )
    result = await db_connection.exec(query)

    # The first column should be DemoModelA's name, and the second should be DemoModelB's name
    assert len(result) == 1
    assert result[0] == ("Name from A", "Name from B")

    # Verify the order is maintained when selecting in reverse
    query_reversed = (
        QueryBuilder()
        .select((DemoModelB.name, DemoModelA.name))
        .join(DemoModelA, DemoModelA.code == DemoModelB.code)
    )
    result_reversed = await db_connection.exec(query_reversed)

    assert len(result_reversed) == 1
    assert result_reversed[0] == ("Name from B", "Name from A")


@pytest.mark.asyncio
async def test_select_with_order_by_func_count(db_connection: DBConnection):
    # Create users with different numbers of artifacts
    users = [
        UserDemo(name="John", email="john@example.com"),
        UserDemo(name="Jane", email="jane@example.com"),
        UserDemo(name="Bob", email="bob@example.com"),
    ]
    await db_connection.insert(users)

    # Create artifacts with different counts per user
    artifacts = [
        ArtifactDemo(title="John's Post 1", user_id=users[0].id),
        ArtifactDemo(title="John's Post 2", user_id=users[0].id),
        ArtifactDemo(title="Jane's Post", user_id=users[1].id),
        # Bob has no posts
    ]
    await db_connection.insert(artifacts)

    query = (
        QueryBuilder()
        .select((UserDemo.name, func.count(ArtifactDemo.id)))
        .join(ArtifactDemo, UserDemo.id == ArtifactDemo.user_id, "LEFT")
        .group_by(UserDemo.name)
        .order_by(func.count(ArtifactDemo.id), "DESC")
    )
    result = await db_connection.exec(query)

    assert len(result) == 3
    # John has 2 posts
    assert result[0] == ("John", 2)
    # Jane has 1 post
    assert result[1] == ("Jane", 1)
    # Bob has 0 posts
    assert result[2] == ("Bob", 0)


@pytest.mark.asyncio
async def test_json_update(db_connection: DBConnection):
    """
    Test that JSON fields are correctly serialized during updates.
    """
    # Create the table first
    await db_connection.conn.execute("DROP TABLE IF EXISTS jsondemo")
    await create_all(db_connection, [JsonDemo])

    # Create initial object with JSON data
    demo = JsonDemo(
        settings={"theme": "dark", "notifications": True},
        metadata={"version": 1},
        unique_val="1",
    )
    await db_connection.insert([demo])

    # Update JSON fields
    demo.settings = {"theme": "light", "notifications": False}
    demo.metadata = {"version": 2, "last_updated": "2024-01-01"}
    await db_connection.update([demo])

    # Verify the update through a fresh select
    result = await db_connection.exec(
        QueryBuilder().select(JsonDemo).where(JsonDemo.id == demo.id)
    )
    assert len(result) == 1
    assert result[0].settings == {"theme": "light", "notifications": False}
    assert result[0].metadata == {"version": 2, "last_updated": "2024-01-01"}


@pytest.mark.asyncio
async def test_json_upsert(db_connection: DBConnection):
    """
    Test that JSON fields are correctly serialized during upsert operations.
    """
    # Create the table first
    await db_connection.conn.execute("DROP TABLE IF EXISTS jsondemo")
    await create_all(db_connection, [JsonDemo])

    # Initial insert via upsert
    demo = JsonDemo(
        settings={"theme": "dark", "notifications": True},
        metadata={"version": 1},
        unique_val="1",
    )
    result = await db_connection.upsert(
        [demo],
        conflict_fields=(JsonDemo.unique_val,),
        update_fields=(JsonDemo.metadata,),
        returning_fields=(JsonDemo.unique_val, JsonDemo.metadata),
    )

    assert result is not None
    assert len(result) == 1
    assert result[0][0] == "1"
    assert result[0][1] == {"version": 1}

    # Update via upsert
    demo2 = JsonDemo(
        settings={"theme": "dark", "notifications": True},
        metadata={"version": 2, "last_updated": "2024-01-01"},  # New metadata
        unique_val="1",  # Same value to trigger update
    )
    result = await db_connection.upsert(
        [demo2],
        conflict_fields=(JsonDemo.unique_val,),
        update_fields=(JsonDemo.metadata,),
        returning_fields=(JsonDemo.unique_val, JsonDemo.metadata),
    )

    assert result is not None
    assert len(result) == 1
    assert result[0][0] == "1"
    assert result[0][1] == {"version": 2, "last_updated": "2024-01-01"}

    # Verify through a fresh select
    result = await db_connection.exec(QueryBuilder().select(JsonDemo))
    assert len(result) == 1
    assert result[0].settings == {"theme": "dark", "notifications": True}
    assert result[0].metadata == {"version": 2, "last_updated": "2024-01-01"}


@pytest.mark.asyncio
async def test_db_connection_update_batched(db_connection: DBConnection):
    """Test that updates are properly batched when dealing with many objects and different field combinations."""
    # Create test data with different update patterns
    users_group1 = [
        UserDemo(name=f"User{i}", email=f"user{i}@example.com") for i in range(10)
    ]
    users_group2 = [
        UserDemo(name=f"User{i}", email=f"user{i}@example.com") for i in range(10, 20)
    ]
    users_group3 = [
        UserDemo(name=f"User{i}", email=f"user{i}@example.com") for i in range(20, 30)
    ]
    all_users = users_group1 + users_group2 + users_group3
    await db_connection.insert(all_users)

    # Modify different fields for different groups to test batching by modified fields
    for user in users_group1:
        user.name = f"Updated{user.name}"  # Only name modified

    for user in users_group2:
        user.email = f"updated_{user.email}"  # Only email modified

    for user in users_group3:
        user.name = f"Updated{user.name}"  # Both fields modified
        user.email = f"updated_{user.email}"

    await db_connection.update(all_users)

    # Verify all updates were applied correctly
    result = await db_connection.conn.fetch("SELECT * FROM userdemo ORDER BY id")
    assert len(result) == 30

    # Check group 1 (only names updated)
    for i, row in enumerate(result[:10]):
        assert row["name"] == f"UpdatedUser{i}"
        assert row["email"] == f"user{i}@example.com"

    # Check group 2 (only emails updated)
    for i, row in enumerate(result[10:20]):
        assert row["name"] == f"User{i + 10}"
        assert row["email"] == f"updated_user{i + 10}@example.com"

    # Check group 3 (both fields updated)
    for i, row in enumerate(result[20:30]):
        assert row["name"] == f"UpdatedUser{i + 20}"
        assert row["email"] == f"updated_user{i + 20}@example.com"

    # Verify all modifications were cleared
    assert all(user.get_modified_attributes() == {} for user in all_users)


#
# Batch query construction
#


def assert_expected_user_fields(user: Type[UserDemo]):
    # Verify UserDemo structure hasn't changed - if this fails, update the parameter calculations below
    assert {
        key for key in UserDemo.model_fields.keys() if key not in INTERNAL_TABLE_FIELDS
    } == {"id", "name", "email"}
    assert UserDemo.model_fields["id"].primary_key
    assert UserDemo.model_fields["id"].default is None
    return True


@asynccontextmanager
async def mock_transaction():
    yield


@pytest.mark.asyncio
async def test_batch_insert_exceeds_parameters():
    """
    Test that insert() correctly batches operations when we exceed Postgres parameter limits.
    We'll create enough objects with enough fields that a single query would exceed PG_MAX_PARAMETERS.
    """
    assert assert_expected_user_fields(UserDemo)

    # Mock the connection
    mock_conn = AsyncMock()
    mock_conn.fetchmany = AsyncMock(return_value=[{"id": i} for i in range(1000)])
    mock_conn.executemany = AsyncMock()
    mock_conn.transaction = mock_transaction

    db = DBConnection(mock_conn)

    # Calculate how many objects we need to exceed the parameter limit
    # Each object has 2 fields (name, email) in UserDemo
    # So each object uses 2 parameters
    objects_needed = (PG_MAX_PARAMETERS // 2) + 1
    users = [
        UserDemo(name=f"User {i}", email=f"user{i}@example.com")
        for i in range(objects_needed)
    ]

    # Insert the objects
    await db.insert(users)

    # We should have made at least 2 calls to fetchmany since we exceeded the parameter limit
    assert len(mock_conn.fetchmany.mock_calls) >= 2

    # Verify the structure of the first call
    first_call = mock_conn.fetchmany.mock_calls[0]
    assert "INSERT INTO" in first_call.args[0]
    assert '"name"' in first_call.args[0]
    assert '"email"' in first_call.args[0]
    assert "RETURNING" in first_call.args[0]


@pytest.mark.asyncio
async def test_batch_update_exceeds_parameters():
    """
    Test that update() correctly batches operations when we exceed Postgres parameter limits.
    We'll create enough objects with enough modified fields that a single query would exceed PG_MAX_PARAMETERS.
    """
    assert assert_expected_user_fields(UserDemo)

    # Mock the connection
    mock_conn = AsyncMock()
    mock_conn.executemany = AsyncMock()
    mock_conn.transaction = mock_transaction

    db = DBConnection(mock_conn)

    # Calculate how many objects we need to exceed the parameter limit
    # Each UPDATE row needs:
    # - 1 parameter for WHERE clause (id)
    # - 2 parameters for SET clause (name, email)
    # So each object uses 3 parameters
    objects_needed = (PG_MAX_PARAMETERS // 3) + 1
    users: list[UserDemo] = []

    # Create objects and mark all fields as modified
    for i in range(objects_needed):
        user = UserDemo(id=i, name=f"User {i}", email=f"user{i}@example.com")
        user.clear_modified_attributes()

        # Simulate modifications to both fields
        user.name = f"New User {i}"
        user.email = f"newuser{i}@example.com"

        users.append(user)

    # Update the objects
    await db.update(users)

    # We should have made at least 2 calls to executemany since we exceeded the parameter limit
    assert len(mock_conn.executemany.mock_calls) >= 2

    # Verify the structure of the first call
    first_call = mock_conn.executemany.mock_calls[0]
    assert "UPDATE" in first_call.args[0]
    assert "SET" in first_call.args[0]
    assert "WHERE" in first_call.args[0]
    assert '"id"' in first_call.args[0]


@pytest.mark.asyncio
async def test_batch_upsert_exceeds_parameters():
    """
    Test that upsert() correctly batches operations when we exceed Postgres parameter limits.
    We'll create enough objects with enough fields that a single query would exceed PG_MAX_PARAMETERS.
    """
    assert assert_expected_user_fields(UserDemo)

    # Calculate how many objects we need to exceed the parameter limit
    # Each object has 2 fields (name, email) in UserDemo
    # So each object uses 2 parameters
    objects_needed = (PG_MAX_PARAMETERS // 2) + 1
    users = [
        UserDemo(name=f"User {i}", email=f"user{i}@example.com")
        for i in range(objects_needed)
    ]

    # Mock the connection with dynamic results based on input
    mock_conn = AsyncMock()
    mock_conn.fetchmany = AsyncMock(
        side_effect=lambda query, values_list: [
            {"id": i, "name": f"User {i}", "email": f"user{i}@example.com"}
            for i in range(len(values_list))
        ]
    )
    mock_conn.executemany = AsyncMock()
    mock_conn.transaction = mock_transaction

    db = DBConnection(mock_conn)

    # Upsert the objects with all possible kwargs
    result = await db.upsert(
        users,
        conflict_fields=(UserDemo.email,),
        update_fields=(UserDemo.name,),
        returning_fields=(UserDemo.id, UserDemo.name, UserDemo.email),
    )

    # We should have made at least 2 calls to fetchmany since we exceeded the parameter limit
    assert len(mock_conn.fetchmany.mock_calls) >= 2

    # Verify the structure of the first call
    first_call = mock_conn.fetchmany.mock_calls[0]
    assert "INSERT INTO" in first_call.args[0]
    assert "ON CONFLICT" in first_call.args[0]
    assert "DO UPDATE SET" in first_call.args[0]
    assert "RETURNING" in first_call.args[0]

    # Verify we got back the expected number of results
    assert result is not None
    assert len(result) == objects_needed
    assert all(len(r) == 3 for r in result)  # Each result should have id, name, email


@pytest.mark.asyncio
async def test_batch_upsert_multiple_with_real_db(db_connection: DBConnection):
    """
    Integration test for upserting multiple objects at once with a real database connection.
    Tests both insert and update scenarios in the same batch.
    """
    await db_connection.conn.execute(
        """
        ALTER TABLE userdemo
        ADD CONSTRAINT email_unique UNIQUE (email)
        """
    )

    # Create initial set of users
    initial_users = [
        UserDemo(name="User 1", email="user1@example.com"),
        UserDemo(name="User 2", email="user2@example.com"),
    ]
    await db_connection.insert(initial_users)

    # Create a mix of new and existing users for upsert
    users_to_upsert = [
        # These should update
        UserDemo(name="Updated User 1", email="user1@example.com"),
        UserDemo(name="Updated User 2", email="user2@example.com"),
        # These should insert
        UserDemo(name="User 3", email="user3@example.com"),
        UserDemo(name="User 4", email="user4@example.com"),
    ]

    result = await db_connection.upsert(
        users_to_upsert,
        conflict_fields=(UserDemo.email,),
        update_fields=(UserDemo.name,),
        returning_fields=(UserDemo.name, UserDemo.email),
    )

    # Verify we got all results back
    assert result is not None
    assert len(result) == 4

    # Verify the database state
    db_result = await db_connection.conn.fetch("SELECT * FROM userdemo ORDER BY email")
    assert len(db_result) == 4

    # Check that updates worked
    assert db_result[0]["name"] == "Updated User 1"
    assert db_result[1]["name"] == "Updated User 2"

    # Check that inserts worked
    assert db_result[2]["name"] == "User 3"
    assert db_result[3]["name"] == "User 4"


@pytest.mark.asyncio
async def test_initialize_types_caching(docker_postgres):
    # Clear the global cache for isolation.
    TYPE_CACHE.clear()

    # Define a sample enum and model that require type introspection.
    class StatusEnum(StrEnum):
        ACTIVE = "active"
        INACTIVE = "inactive"
        PENDING = "pending"

    class ComplexTypeDemo(TableBase):
        id: int = Field(primary_key=True)
        status: StatusEnum
        tags: list[str]
        metadata: dict[Any, Any] = Field(is_json=True)

    # Establish the first connection.
    conn1 = await asyncpg.connect(
        host=docker_postgres["host"],
        port=docker_postgres["port"],
        user=docker_postgres["user"],
        password=docker_postgres["password"],
        database=docker_postgres["database"],
    )
    db1 = DBConnection(conn1)

    # Prepare the database schema.
    await db1.conn.execute("DROP TYPE IF EXISTS statusenum CASCADE")
    await db1.conn.execute("DROP TABLE IF EXISTS complextypedemo")
    await create_all(db1, [ComplexTypeDemo])

    # Save the original method.
    original_introspect = Connection._introspect_types

    # Default value
    introspect_wrapper_call_count = 0

    # Define a wrapper that counts calls and then calls through.
    async def introspect_wrapper(self, types_with_missing_codecs, timeout):
        nonlocal introspect_wrapper_call_count
        introspect_wrapper_call_count += 1
        return await original_introspect(self, types_with_missing_codecs, timeout)

    # Patch the _introspect_types method on the Connection class.
    with patch.object(Connection, "_introspect_types", new=introspect_wrapper):
        # For the first connection, initialize types.
        await db1.initialize_types()
        # Verify that introspection was called.
        assert introspect_wrapper_call_count == 1

        # Insert test data via the first connection.
        demo1 = ComplexTypeDemo(
            id=1,
            status=StatusEnum.ACTIVE,
            tags=["test", "demo"],
            metadata={"version": 1},
        )
        await db1.insert([demo1])

        # Create a second connection to the same database.
        conn2 = await asyncpg.connect(
            host=docker_postgres["host"],
            port=docker_postgres["port"],
            user=docker_postgres["user"],
            password=docker_postgres["password"],
            database=docker_postgres["database"],
        )
        db2 = DBConnection(conn2)

        # For the second connection, initializing types should use the cache.
        await db2.initialize_types()

        # The call count should remain unchanged.
        assert introspect_wrapper_call_count == 1

        # Verify that we can query the inserted record via the second connection.
        results = await db2.exec(
            QueryBuilder().select(ComplexTypeDemo).order_by(ComplexTypeDemo.id, "ASC")
        )
        assert len(results) == 1
        assert results[0].status == StatusEnum.ACTIVE

        # Insert additional data via the second connection.
        demo2 = ComplexTypeDemo(
            id=2,
            status=StatusEnum.PENDING,
            tags=["test2", "demo2"],
            metadata={"version": 2},
        )
        await db2.insert([demo2])

        # Retrieve and verify data from both connections.
        result1 = await db1.exec(
            QueryBuilder().select(ComplexTypeDemo).order_by(ComplexTypeDemo.id, "ASC")
        )
        result2 = await db2.exec(
            QueryBuilder().select(ComplexTypeDemo).order_by(ComplexTypeDemo.id, "ASC")
        )

        assert len(result1) == 2
        assert len(result2) == 2
        assert result1[0].status == StatusEnum.ACTIVE
        assert result1[1].status == StatusEnum.PENDING
        assert result2[0].tags == ["test", "demo"]
        assert result2[1].tags == ["test2", "demo2"]

    await conn2.close()
    await conn1.close()


@pytest.mark.asyncio
async def test_get_dsn(db_connection: DBConnection):
    """
    Test that get_dsn correctly formats the connection parameters into a DSN string.
    """
    dsn = db_connection.get_dsn()
    assert dsn.startswith("postgresql://")
    assert "iceaxe" in dsn
    assert "localhost" in dsn
    assert ":" in dsn  # Just verify there is a port
    assert "iceaxe_test_db" in dsn


@pytest.mark.asyncio
async def test_nested_transactions(db_connection):
    """
    Test that nested transactions raise an error by default, but work with ensure=True.
    """
    # Start an outer transaction
    async with db_connection.transaction():
        # This should work fine
        assert db_connection.in_transaction is True

        # Nested transaction with ensure=True should work
        async with db_connection.transaction(ensure=True):
            assert db_connection.in_transaction is True

        # Nested transaction without ensure should fail
        with pytest.raises(
            RuntimeError,
            match="Cannot start a new transaction while already in a transaction",
        ):
            async with db_connection.transaction():
                pass  # Should not reach here

    # After outer transaction ends, we should be out of transaction
    assert db_connection.in_transaction is False

    # Now a new transaction should start without error
    async with db_connection.transaction():
        assert db_connection.in_transaction is True
