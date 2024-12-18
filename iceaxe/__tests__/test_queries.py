from typing import TYPE_CHECKING, Literal

import pytest

from iceaxe.__tests__.conf_models import ArtifactDemo, UserDemo
from iceaxe.functions import func
from iceaxe.queries import QueryBuilder, and_, or_, select


def test_select():
    new_query = QueryBuilder().select(UserDemo)
    assert new_query.build() == (
        'SELECT "userdemo"."id" as "userdemo_id", "userdemo"."name" as '
        '"userdemo_name", "userdemo"."email" as "userdemo_email" FROM "userdemo"',
        [],
    )


def test_select_single_field():
    new_query = QueryBuilder().select(UserDemo.email)
    assert new_query.build() == ('SELECT "userdemo"."email" FROM "userdemo"', [])


def test_select_multiple_fields():
    new_query = QueryBuilder().select((UserDemo.id, UserDemo.name, UserDemo.email))
    assert new_query.build() == (
        'SELECT "userdemo"."id", "userdemo"."name", "userdemo"."email" FROM "userdemo"',
        [],
    )


def test_where():
    new_query = QueryBuilder().select(UserDemo.id).where(UserDemo.id > 0)
    assert new_query.build() == (
        'SELECT "userdemo"."id" FROM "userdemo" WHERE "userdemo"."id" > $1',
        [0],
    )


def test_where_columns():
    new_query = (
        QueryBuilder().select(UserDemo.id).where(UserDemo.name == UserDemo.email)
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" FROM "userdemo" WHERE "userdemo"."name" = "userdemo"."email"',
        [],
    )


def test_multiple_where_conditions():
    new_query = (
        QueryBuilder()
        .select(UserDemo.id)
        .where(UserDemo.id > 0, UserDemo.name == "John")
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" FROM "userdemo" WHERE "userdemo"."id" > $1 AND "userdemo"."name" = $2',
        [0, "John"],
    )


def test_order_by():
    new_query = QueryBuilder().select(UserDemo.id).order_by(UserDemo.id, "DESC")
    assert new_query.build() == (
        'SELECT "userdemo"."id" FROM "userdemo" ORDER BY "userdemo"."id" DESC',
        [],
    )


def test_multiple_order_by():
    new_query = (
        QueryBuilder()
        .select(UserDemo.id)
        .order_by(UserDemo.id, "DESC")
        .order_by(UserDemo.name, "ASC")
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" FROM "userdemo" ORDER BY "userdemo"."id" DESC, "userdemo"."name" ASC',
        [],
    )


def test_join():
    new_query = (
        QueryBuilder()
        .select((UserDemo.id, ArtifactDemo.title))
        .join(ArtifactDemo, UserDemo.id == ArtifactDemo.user_id)
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id", "artifactdemo"."title" FROM "userdemo" INNER JOIN artifactdemo ON "userdemo"."id" = "artifactdemo"."user_id"',
        [],
    )


def test_left_join():
    new_query = (
        QueryBuilder()
        .select((UserDemo.id, ArtifactDemo.title))
        .join(ArtifactDemo, UserDemo.id == ArtifactDemo.user_id, "LEFT")
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id", "artifactdemo"."title" FROM "userdemo" LEFT JOIN artifactdemo ON "userdemo"."id" = "artifactdemo"."user_id"',
        [],
    )


def test_limit():
    new_query = QueryBuilder().select(UserDemo.id).limit(10)
    assert new_query.build() == ('SELECT "userdemo"."id" FROM "userdemo" LIMIT 10', [])


def test_offset():
    new_query = QueryBuilder().select(UserDemo.id).offset(5)
    assert new_query.build() == ('SELECT "userdemo"."id" FROM "userdemo" OFFSET 5', [])


def test_limit_and_offset():
    new_query = QueryBuilder().select(UserDemo.id).limit(10).offset(5)
    assert new_query.build() == (
        'SELECT "userdemo"."id" FROM "userdemo" LIMIT 10 OFFSET 5',
        [],
    )


def test_group_by():
    new_query = (
        QueryBuilder()
        .select((UserDemo.name, func.count(UserDemo.id)))
        .group_by(UserDemo.name)
    )
    assert new_query.build() == (
        'SELECT "userdemo"."name", count("userdemo"."id") AS aggregate_0 FROM "userdemo" GROUP BY "userdemo"."name"',
        [],
    )


def test_update():
    new_query = (
        QueryBuilder()
        .update(UserDemo)
        .set(UserDemo.name, "John")
        .where(UserDemo.id == 1)
    )
    assert new_query.build() == (
        'UPDATE "userdemo" SET "userdemo"."name" = $1 WHERE "userdemo"."id" = $2',
        ["John", 1],
    )


def test_delete():
    new_query = QueryBuilder().delete(UserDemo).where(UserDemo.id == 1)
    assert new_query.build() == (
        'DELETE FROM "userdemo" WHERE "userdemo"."id" = $1',
        [1],
    )


def test_text():
    new_query = QueryBuilder().text("SELECT * FROM users WHERE id = $1", 1)
    assert new_query.build() == ("SELECT * FROM users WHERE id = $1", [1])


def test_function_count():
    new_query = QueryBuilder().select(func.count(UserDemo.id))
    assert new_query.build() == (
        'SELECT count("userdemo"."id") AS aggregate_0 FROM "userdemo"',
        [],
    )


def test_function_distinct():
    new_query = QueryBuilder().select(func.distinct(UserDemo.name))
    assert new_query.build() == (
        'SELECT distinct "userdemo"."name" AS aggregate_0 FROM "userdemo"',
        [],
    )


def test_invalid_where_condition():
    with pytest.raises(ValueError):
        QueryBuilder().select(UserDemo.id).where("invalid condition")  # type: ignore


def test_invalid_join_condition():
    with pytest.raises(ValueError):
        QueryBuilder().select(UserDemo.id).join(ArtifactDemo, "invalid condition")  # type: ignore


def test_invalid_group_by():
    with pytest.raises(ValueError):
        QueryBuilder().select(UserDemo.id).group_by("invalid field")


#
# Comparison groups
#


def test_and_group():
    new_query = (
        QueryBuilder()
        .select(UserDemo.id)
        .where(
            and_(
                UserDemo.name == UserDemo.email,
                UserDemo.id > 0,
            )
        )
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" FROM "userdemo" WHERE ("userdemo"."name" = "userdemo"."email" AND "userdemo"."id" > $1)',
        [0],
    )


def test_or_group():
    new_query = (
        QueryBuilder()
        .select(UserDemo.id)
        .where(
            or_(
                UserDemo.name == UserDemo.email,
                UserDemo.id > 0,
            )
        )
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" FROM "userdemo" WHERE ("userdemo"."name" = "userdemo"."email" OR "userdemo"."id" > $1)',
        [0],
    )


def test_nested_and_or_group():
    new_query = (
        QueryBuilder()
        .select(UserDemo.id)
        .where(
            and_(
                or_(
                    UserDemo.name == UserDemo.email,
                    UserDemo.id > 0,
                ),
                UserDemo.id < 10,
            )
        )
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" FROM "userdemo" WHERE (("userdemo"."name" = "userdemo"."email" OR "userdemo"."id" > $1) AND "userdemo"."id" < $2)',
        [0, 10],
    )


#
# Typehinting
# These checks are run as part of the static typechecking we do
# for our codebase, not as part of the pytest runtime.
#


def test_select_single_typehint():
    query = select(UserDemo)
    if TYPE_CHECKING:
        _: QueryBuilder[UserDemo, Literal["SELECT"]] = query


def test_select_multiple_typehints():
    query = select((UserDemo, UserDemo.id, UserDemo.name))
    if TYPE_CHECKING:
        _: QueryBuilder[tuple[UserDemo, int, str], Literal["SELECT"]] = query


def test_allow_branching():
    base_query = select(UserDemo)

    query_1 = base_query.limit(1)
    query_2 = base_query.limit(2)

    assert query_1.limit_value == 1
    assert query_2.limit_value == 2


def test_distinct_on():
    new_query = (
        QueryBuilder()
        .select((UserDemo.name, UserDemo.email))
        .distinct_on(UserDemo.name)
    )
    assert new_query.build() == (
        'SELECT DISTINCT ON ("userdemo"."name") "userdemo"."name", "userdemo"."email" FROM "userdemo"',
        [],
    )


def test_distinct_on_multiple_fields():
    new_query = (
        QueryBuilder()
        .select((UserDemo.name, UserDemo.email))
        .distinct_on(UserDemo.name, UserDemo.email)
    )
    assert new_query.build() == (
        'SELECT DISTINCT ON ("userdemo"."name", "userdemo"."email") "userdemo"."name", "userdemo"."email" FROM "userdemo"',
        [],
    )


def test_for_update_basic():
    new_query = QueryBuilder().select(UserDemo).for_update()
    assert new_query.build() == (
        'SELECT "userdemo"."id" as "userdemo_id", "userdemo"."name" as '
        '"userdemo_name", "userdemo"."email" as "userdemo_email" FROM "userdemo" FOR UPDATE',
        [],
    )


def test_for_update_nowait():
    new_query = QueryBuilder().select(UserDemo).for_update(nowait=True)
    assert new_query.build() == (
        'SELECT "userdemo"."id" as "userdemo_id", "userdemo"."name" as '
        '"userdemo_name", "userdemo"."email" as "userdemo_email" FROM "userdemo" FOR UPDATE NOWAIT',
        [],
    )


def test_for_update_skip_locked():
    new_query = QueryBuilder().select(UserDemo).for_update(skip_locked=True)
    assert new_query.build() == (
        'SELECT "userdemo"."id" as "userdemo_id", "userdemo"."name" as '
        '"userdemo_name", "userdemo"."email" as "userdemo_email" FROM "userdemo" FOR UPDATE SKIP LOCKED',
        [],
    )


def test_for_update_of():
    new_query = QueryBuilder().select(UserDemo).for_update(of=(UserDemo,))
    assert new_query.build() == (
        'SELECT "userdemo"."id" as "userdemo_id", "userdemo"."name" as '
        '"userdemo_name", "userdemo"."email" as "userdemo_email" FROM "userdemo" FOR UPDATE OF userdemo',
        [],
    )


def test_for_update_multiple_calls():
    new_query = (
        QueryBuilder()
        .select(UserDemo)
        .for_update(of=(UserDemo,))
        .for_update(nowait=True)
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" as "userdemo_id", "userdemo"."name" as '
        '"userdemo_name", "userdemo"."email" as "userdemo_email" FROM "userdemo" FOR UPDATE OF userdemo NOWAIT',
        [],
    )


def test_for_update_multiple_of():
    new_query = (
        QueryBuilder()
        .select((UserDemo, ArtifactDemo))
        .join(ArtifactDemo, UserDemo.id == ArtifactDemo.user_id)
        .for_update(of=(UserDemo,))
        .for_update(of=(ArtifactDemo,))
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" as "userdemo_id", "userdemo"."name" as '
        '"userdemo_name", "userdemo"."email" as "userdemo_email", '
        '"artifactdemo"."id" as "artifactdemo_id", "artifactdemo"."title" as '
        '"artifactdemo_title", "artifactdemo"."user_id" as "artifactdemo_user_id" '
        'FROM "userdemo" INNER JOIN artifactdemo ON "userdemo"."id" = "artifactdemo"."user_id" '
        "FOR UPDATE OF artifactdemo, userdemo",
        [],
    )
