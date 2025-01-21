from typing import List, Literal, Optional, TypeVar, cast

import pytest

from iceaxe import Field, TableBase, func, select
from iceaxe.field import DBFieldInfo
from iceaxe.postgres import LexemePriority, PostgresFullText
from iceaxe.queries import QueryBuilder
from iceaxe.session import DBConnection

T = TypeVar("T")


class Article(TableBase):
    """Test model for full-text search."""

    id: int = Field(primary_key=True)
    title: str = Field(postgres_config=PostgresFullText(language="english", weight="A"))
    content: str = Field(
        postgres_config=PostgresFullText(language="english", weight="B")
    )
    summary: Optional[str] = Field(
        default=None, postgres_config=PostgresFullText(language="english", weight="C")
    )

    async def save(self, db_connection: DBConnection) -> None:
        """Save the article to the database."""
        await db_connection.insert([self])


async def execute(
    query: QueryBuilder[T, Literal["SELECT"]], db_connection: DBConnection
) -> List[T]:
    """Execute a query and return the results."""
    return cast(List[T], await db_connection.exec(query))


@pytest.mark.asyncio
async def test_basic_text_search(indexed_db_connection: DBConnection):
    """Test basic text search functionality using query builder."""
    # Create test data
    articles = [
        Article(
            id=1, title="Python Programming", content="Learn Python programming basics"
        ),
        Article(
            id=2, title="Database Design", content="Python and database design patterns"
        ),
        Article(id=3, title="Web Development", content="Building web apps with Python"),
    ]
    for article in articles:
        await article.save(indexed_db_connection)

    # Search in title only
    title_vector = func.to_tsvector("english", Article.title)
    query = func.to_tsquery("english", "python")

    results = await execute(
        select(Article).where(title_vector.matches(query)), indexed_db_connection
    )
    assert len(results) == 1
    assert results[0].id == 1

    # Search in content only
    content_vector = func.to_tsvector("english", Article.content)
    results = await execute(
        select(Article).where(content_vector.matches(query)), indexed_db_connection
    )
    assert len(results) == 3  # All articles mention Python in content


@pytest.mark.asyncio
async def test_complex_text_search(indexed_db_connection: DBConnection):
    """Test complex text search queries with boolean operators."""
    articles = [
        Article(id=1, title="Python Programming", content="Learn programming basics"),
        Article(id=2, title="Python Advanced", content="Advanced programming concepts"),
        Article(
            id=3, title="JavaScript Basics", content="Learn programming with JavaScript"
        ),
    ]
    for article in articles:
        await article.save(indexed_db_connection)

    # Test AND operator
    vector = func.to_tsvector("english", Article.title)
    query = func.to_tsquery("english", "python & programming")
    results = await execute(
        select(Article).where(vector.matches(query)), indexed_db_connection
    )
    assert len(results) == 1
    assert results[0].id == 1

    # Test OR operator
    query = func.to_tsquery("english", "python | javascript")
    results = await execute(
        select(Article).where(vector.matches(query)), indexed_db_connection
    )
    assert len(results) == 3
    assert {r.id for r in results} == {1, 2, 3}

    # Test NOT operator
    query = func.to_tsquery("english", "programming & !python")
    results = await execute(
        select(Article).where(vector.matches(query)), indexed_db_connection
    )
    assert len(results) == 0  # No articles have "programming" without "python" in title


@pytest.mark.asyncio
async def test_combined_field_search(indexed_db_connection: DBConnection):
    """Test searching across multiple fields."""
    articles = [
        Article(
            id=1,
            title="Python Guide",
            content="Learn programming basics",
            summary="A beginner's guide to Python",
        ),
        Article(
            id=2,
            title="Programming Tips",
            content="Python best practices",
            summary="Advanced Python concepts",
        ),
    ]
    for article in articles:
        await article.save(indexed_db_connection)

    # Search across all fields
    vector = (
        func.to_tsvector("english", Article.title)
        .concat(func.to_tsvector("english", Article.content))
        .concat(func.to_tsvector("english", Article.summary))
    )
    query = func.to_tsquery("english", "python & guide")

    results = await execute(
        select(Article).where(vector.matches(query)), indexed_db_connection
    )
    assert len(results) == 1
    assert results[0].id == 1  # Only first article has both "python" and "guide"


@pytest.mark.asyncio
async def test_weighted_text_search(indexed_db_connection: DBConnection):
    """Test text search with weighted columns."""
    articles = [
        Article(
            id=1,
            title="Python Guide",  # Weight A
            content="Basic Python",  # Weight B
            summary="Python tutorial",  # Weight C
        ),
        Article(
            id=2,
            title="Programming",
            content="Python Guide",
            summary="Guide to programming",
        ),
    ]
    for article in articles:
        await article.save(indexed_db_connection)

    # Search with weights
    vector = (
        func.setweight(func.to_tsvector("english", Article.title), "A")
        .concat(func.setweight(func.to_tsvector("english", Article.content), "B"))
        .concat(func.setweight(func.to_tsvector("english", Article.summary), "C"))
    )
    query = func.to_tsquery("english", "python & guide")

    results = await execute(
        select((Article, func.ts_rank(vector, query).as_("rank")))
        .where(vector.matches(query))
        .order_by("rank", direction="DESC"),
        indexed_db_connection,
    )
    assert len(results) == 2
    # First article should rank higher because "Python Guide" is in title (weight A)
    assert results[0][0].id == 1
    assert results[1][0].id == 2
    assert results[0][1] > results[1][1]  # Check that rank is higher


@pytest.mark.asyncio
async def test_weight_priority_variants(indexed_db_connection: DBConnection):
    """Test text search using both string literals and LexemePriority enum for weights."""
    articles = [
        Article(
            id=1,
            title="Python Guide",  # Weight A (string literal)
            content="Basic Python",  # Weight B (enum)
            summary="Python tutorial",  # Weight C (enum)
        ),
        Article(
            id=2,
            title="Programming",
            content="Python Guide",
            summary="Guide to programming",
        ),
    ]

    # Create a variant of Article using the enum
    class ArticleWithEnum(TableBase):
        id: int = Field(primary_key=True)
        title: str = Field(
            postgres_config=PostgresFullText(
                language="english", weight=LexemePriority.HIGHEST
            )
        )
        content: str = Field(
            postgres_config=PostgresFullText(
                language="english", weight=LexemePriority.HIGH
            )
        )
        summary: Optional[str] = Field(
            default=None,
            postgres_config=PostgresFullText(
                language="english", weight=LexemePriority.LOW
            ),
        )

    # Verify both models can be created and weights are equivalent
    assert (
        cast(
            PostgresFullText,
            cast(DBFieldInfo, Article.model_fields["title"]).postgres_config,
        ).weight
        == cast(
            PostgresFullText,
            cast(DBFieldInfo, ArticleWithEnum.model_fields["title"]).postgres_config,
        ).weight
        == "A"
    )
    assert (
        cast(
            PostgresFullText,
            cast(DBFieldInfo, Article.model_fields["content"]).postgres_config,
        ).weight
        == cast(
            PostgresFullText,
            cast(DBFieldInfo, ArticleWithEnum.model_fields["content"]).postgres_config,
        ).weight
        == "B"
    )
    assert (
        cast(
            PostgresFullText,
            cast(DBFieldInfo, Article.model_fields["summary"]).postgres_config,
        ).weight
        == cast(
            PostgresFullText,
            cast(DBFieldInfo, ArticleWithEnum.model_fields["summary"]).postgres_config,
        ).weight
        == "C"
    )

    # Save and query using the original Article model to verify functionality
    for article in articles:
        await article.save(indexed_db_connection)

    vector = (
        func.setweight(
            func.to_tsvector("english", Article.title), LexemePriority.HIGHEST
        )
        .concat(
            func.setweight(
                func.to_tsvector("english", Article.content), LexemePriority.HIGH
            )
        )
        .concat(
            func.setweight(
                func.to_tsvector("english", Article.summary), LexemePriority.LOW
            )
        )
    )
    query = func.to_tsquery("english", "python & guide")

    results = await execute(
        select((Article, func.ts_rank(vector, query).as_("rank")))
        .where(vector.matches(query))
        .order_by("rank", direction="DESC"),
        indexed_db_connection,
    )

    assert len(results) == 2
    # First article should rank higher because "Python Guide" is in title (HIGHEST weight)
    assert results[0][0].id == 1
    assert results[1][0].id == 2
    assert results[0][1] > results[1][1]
