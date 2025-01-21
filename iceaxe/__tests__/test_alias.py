import pytest

from iceaxe import alias, func, select, sql
from iceaxe.__tests__.conf_models import UserDemo
from iceaxe.queries import QueryBuilder
from iceaxe.session import (
    DBConnection,
)


@pytest.mark.asyncio
async def test_alias_with_function(db_connection: DBConnection):
    """Test using alias with a function value."""
    demo = UserDemo(id=1, name="Test Title", email="john@example.com")
    await db_connection.insert([demo])

    # Test using string length function with alias
    results = await db_connection.exec(
        select((UserDemo, alias("name_length", func.length(UserDemo.name))))
    )

    assert len(results) == 1
    assert results[0][0].id == 1
    assert isinstance(results[0][1], int)  # length result


@pytest.mark.asyncio
async def test_alias_with_raw_sql(db_connection: DBConnection):
    """
    Test that we can use a text query alongside an alias to map raw SQL results
    to typed values.
    """
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    # Create a query that uses text() alongside an alias
    query = (
        QueryBuilder()
        .select((UserDemo, alias("rollup_value", int)))
        .text(
            f"""
            SELECT {sql.select(UserDemo)}, COUNT(*) AS rollup_value
            FROM userdemo
            GROUP BY id
            """
        )
    )
    result = await db_connection.exec(query)
    assert len(result) == 1
    assert isinstance(result[0], tuple)
    assert isinstance(result[0][0], UserDemo)
    assert result[0][0].name == "John Doe"
    assert result[0][0].email == "john@example.com"
    assert result[0][1] == 1  # The count should be 1


@pytest.mark.asyncio
async def test_multiple_aliases(db_connection: DBConnection):
    """Test using multiple aliases in a single query."""
    demo1 = UserDemo(id=1, name="First Item", email="john@example.com")
    demo2 = UserDemo(id=2, name="Second Item", email="jane@example.com")
    await db_connection.insert([demo1, demo2])

    # Test multiple aliases with different SQL functions
    results = await db_connection.exec(
        select(
            (
                UserDemo,
                alias("upper_name", func.upper(UserDemo.name)),
                alias("item_count", func.count(UserDemo.id)),
            )
        )
        .group_by(UserDemo.id, UserDemo.name)
        .order_by(UserDemo.id)
    )

    assert len(results) == 2
    assert results[0][0].id == 1
    assert results[0][1] == "FIRST ITEM"  # uppercase name
    assert results[0][2] == 1  # count result
    assert results[1][0].id == 2
    assert results[1][1] == "SECOND ITEM"  # uppercase name
    assert results[1][2] == 1  # count result
