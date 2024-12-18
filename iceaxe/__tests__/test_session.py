from enum import StrEnum

import pytest
import asyncpg

from iceaxe.__tests__.conf_models import ArtifactDemo, ComplexDemo, UserDemo
from iceaxe.base import TableBase
from iceaxe.field import Field
from iceaxe.functions import func
from iceaxe.queries import QueryBuilder
from iceaxe.schemas.cli import create_all
from iceaxe.session import (
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
async def test_for_update_prevents_concurrent_modification(db_connection: DBConnection):
    """
    Test that FOR UPDATE actually locks the row for concurrent modifications.
    """
    # Create initial user
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    async with db_connection.transaction():
        # Lock the row with FOR UPDATE
        [locked_user] = await db_connection.exec(
            QueryBuilder()
            .select(UserDemo)
            .where(UserDemo.id == user.id)
            .for_update()
        )
        assert locked_user.name == "John Doe"

        # Try to update from another connection - this should block
        # until our transaction is done
        other_conn = DBConnection(
            await asyncpg.connect(
                host="localhost",
                port=5438,
                user="iceaxe",
                password="mysecretpassword",
                database="iceaxe_test_db",
            )
        )
        try:
            # This should raise an error since we're using NOWAIT
            await other_conn.exec(
                QueryBuilder()
                .select(UserDemo)
                .where(UserDemo.id == user.id)
                .for_update(nowait=True)
            )
            pytest.fail("Should have raised an error")
        except asyncpg.exceptions.LockNotAvailableError:
            # This is expected
            pass
        finally:
            await other_conn.conn.close()


@pytest.mark.asyncio
async def test_for_update_skip_locked(db_connection: DBConnection):
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
                host="localhost",
                port=5438,
                user="iceaxe",
                password="mysecretpassword",
                database="iceaxe_test_db",
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
async def test_for_update_of_with_join(db_connection: DBConnection):
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
                host="localhost",
                port=5438,
                user="iceaxe",
                password="mysecretpassword",
                database="iceaxe_test_db",
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
            try:
                await other_conn.exec(
                    QueryBuilder()
                    .select(ArtifactDemo)
                    .where(ArtifactDemo.id == artifact.id)
                    .for_update(nowait=True)
                )
                pytest.fail("Should have raised an error")
            except asyncpg.exceptions.LockNotAvailableError:
                # This is expected
                pass
        finally:
            await other_conn.conn.close()
