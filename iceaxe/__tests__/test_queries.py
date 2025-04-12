from enum import IntEnum, StrEnum
from typing import TYPE_CHECKING, Literal

import pytest

from iceaxe.__tests__.conf_models import (
    ArtifactDemo,
    Employee,
    FunctionDemoModel,
    UserDemo,
)
from iceaxe.functions import func
from iceaxe.queries import QueryBuilder, and_, or_, select


class UserStatus(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"


def test_select():
    new_query = QueryBuilder().select(UserDemo)
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo"',
        [],
    )


def test_select_single_field():
    new_query = QueryBuilder().select(UserDemo.email)
    assert new_query.build() == (
        'SELECT "userdemo"."email" AS "userdemo_email" FROM "userdemo"',
        [],
    )


def test_select_multiple_fields():
    new_query = QueryBuilder().select((UserDemo.id, UserDemo.name, UserDemo.email))
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS "userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo"',
        [],
    )


def test_where():
    new_query = QueryBuilder().select(UserDemo.id).where(UserDemo.id > 0)
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id" FROM "userdemo" WHERE "userdemo"."id" > $1',
        [0],
    )


def test_where_columns():
    new_query = (
        QueryBuilder().select(UserDemo.id).where(UserDemo.name == UserDemo.email)
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id" FROM "userdemo" WHERE "userdemo"."name" IS NOT DISTINCT FROM "userdemo"."email"',
        [],
    )


def test_multiple_where_conditions():
    new_query = (
        QueryBuilder()
        .select(UserDemo.id)
        .where(UserDemo.id > 0, UserDemo.name == "John")
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id" FROM "userdemo" WHERE "userdemo"."id" > $1 AND "userdemo"."name" = $2',
        [0, "John"],
    )


def test_order_by():
    new_query = QueryBuilder().select(UserDemo.id).order_by(UserDemo.id, "DESC")
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id" FROM "userdemo" ORDER BY "userdemo"."id" DESC',
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
        'SELECT "userdemo"."id" AS "userdemo_id" FROM "userdemo" ORDER BY "userdemo"."id" DESC, "userdemo"."name" ASC',
        [],
    )


def test_join():
    new_query = (
        QueryBuilder()
        .select((UserDemo.id, ArtifactDemo.title))
        .join(ArtifactDemo, UserDemo.id == ArtifactDemo.user_id)
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "artifactdemo"."title" AS "artifactdemo_title" FROM "userdemo" INNER JOIN "artifactdemo" ON "userdemo"."id" = "artifactdemo"."user_id"',
        [],
    )


def test_left_join():
    new_query = (
        QueryBuilder()
        .select((UserDemo.id, ArtifactDemo.title))
        .join(ArtifactDemo, UserDemo.id == ArtifactDemo.user_id, "LEFT")
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "artifactdemo"."title" AS "artifactdemo_title" FROM "userdemo" LEFT JOIN "artifactdemo" ON "userdemo"."id" = "artifactdemo"."user_id"',
        [],
    )


def test_limit():
    new_query = QueryBuilder().select(UserDemo.id).limit(10)
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id" FROM "userdemo" LIMIT 10',
        [],
    )


def test_offset():
    new_query = QueryBuilder().select(UserDemo.id).offset(5)
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id" FROM "userdemo" OFFSET 5',
        [],
    )


def test_limit_and_offset():
    new_query = QueryBuilder().select(UserDemo.id).limit(10).offset(5)
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id" FROM "userdemo" LIMIT 10 OFFSET 5',
        [],
    )


def test_group_by():
    new_query = (
        QueryBuilder()
        .select((UserDemo.name, func.count(UserDemo.id)))
        .group_by(UserDemo.name)
    )
    assert new_query.build() == (
        'SELECT "userdemo"."name" AS "userdemo_name", count("userdemo"."id") AS aggregate_0 FROM "userdemo" GROUP BY "userdemo"."name"',
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
        'UPDATE "userdemo" SET name = $1 WHERE "userdemo"."id" = $2',
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


def test_function_abs():
    new_query = QueryBuilder().select(func.abs(FunctionDemoModel.balance))
    assert new_query.build() == (
        'SELECT abs("functiondemomodel"."balance") AS aggregate_0 FROM "functiondemomodel"',
        [],
    )


def test_function_date_trunc():
    new_query = QueryBuilder().select(
        func.date_trunc("month", FunctionDemoModel.created_at)
    )
    assert new_query.build() == (
        'SELECT date_trunc(\'month\', "functiondemomodel"."created_at") AS aggregate_0 FROM "functiondemomodel"',
        [],
    )


def test_function_date_part():
    new_query = QueryBuilder().select(
        func.date_part("year", FunctionDemoModel.created_at)
    )
    assert new_query.build() == (
        'SELECT date_part(\'year\', "functiondemomodel"."created_at") AS aggregate_0 FROM "functiondemomodel"',
        [],
    )


def test_function_extract():
    new_query = QueryBuilder().select(
        func.extract("month", FunctionDemoModel.created_at)
    )
    assert new_query.build() == (
        'SELECT extract(month from "functiondemomodel"."created_at") AS aggregate_0 FROM "functiondemomodel"',
        [],
    )


def test_function_age():
    # Test age with single argument
    new_query = QueryBuilder().select(func.age(FunctionDemoModel.birth_date))
    assert new_query.build() == (
        'SELECT age("functiondemomodel"."birth_date") AS aggregate_0 FROM "functiondemomodel"',
        [],
    )

    # Test age with two arguments
    new_query = QueryBuilder().select(
        func.age(FunctionDemoModel.end_date, FunctionDemoModel.start_date)
    )
    assert new_query.build() == (
        'SELECT age("functiondemomodel"."end_date", "functiondemomodel"."start_date") AS aggregate_0 FROM "functiondemomodel"',
        [],
    )


def test_function_date():
    new_query = QueryBuilder().select(func.date(FunctionDemoModel.created_at))
    assert new_query.build() == (
        'SELECT date("functiondemomodel"."created_at") AS aggregate_0 FROM "functiondemomodel"',
        [],
    )


def test_function_transformations():
    # Test string functions
    new_query = QueryBuilder().select(
        (
            func.lower(FunctionDemoModel.name),
            func.upper(FunctionDemoModel.name),
            func.length(FunctionDemoModel.name),
            func.trim(FunctionDemoModel.name),
            func.substring(FunctionDemoModel.name, 1, 3),
        )
    )
    assert new_query.build() == (
        'SELECT lower("functiondemomodel"."name") AS aggregate_0, '
        'upper("functiondemomodel"."name") AS aggregate_1, '
        'length("functiondemomodel"."name") AS aggregate_2, '
        'trim("functiondemomodel"."name") AS aggregate_3, '
        'substring("functiondemomodel"."name" from 1 for 3) AS aggregate_4 '
        'FROM "functiondemomodel"',
        [],
    )

    # Test mathematical functions
    new_query = QueryBuilder().select(
        (
            func.round(FunctionDemoModel.balance),
            func.ceil(FunctionDemoModel.balance),
            func.floor(FunctionDemoModel.balance),
            func.power(FunctionDemoModel.balance, 2),
            func.sqrt(FunctionDemoModel.balance),
        )
    )
    assert new_query.build() == (
        'SELECT round("functiondemomodel"."balance") AS aggregate_0, '
        'ceil("functiondemomodel"."balance") AS aggregate_1, '
        'floor("functiondemomodel"."balance") AS aggregate_2, '
        'power("functiondemomodel"."balance", 2) AS aggregate_3, '
        'sqrt("functiondemomodel"."balance") AS aggregate_4 '
        'FROM "functiondemomodel"',
        [],
    )

    # Test aggregate functions
    new_query = QueryBuilder().select(
        (
            func.array_agg(FunctionDemoModel.name),
            func.string_agg(FunctionDemoModel.name, ","),
        )
    )
    assert new_query.build() == (
        'SELECT array_agg("functiondemomodel"."name") AS aggregate_0, '
        'string_agg("functiondemomodel"."name", \',\') AS aggregate_1 '
        'FROM "functiondemomodel"',
        [],
    )

    # Test type conversion functions
    new_query = QueryBuilder().select(
        (
            func.cast(FunctionDemoModel.balance, int),
            func.cast(FunctionDemoModel.name, UserStatus),
            func.to_char(FunctionDemoModel.created_at, "YYYY-MM-DD"),
            func.to_number(FunctionDemoModel.balance_str, "999999.99"),
            func.to_timestamp(FunctionDemoModel.timestamp_str, "YYYY-MM-DD HH24:MI:SS"),
        )
    )
    assert new_query.build() == (
        'SELECT cast("functiondemomodel"."balance" as integer) AS aggregate_0, '
        'cast("functiondemomodel"."name" as userstatus) AS aggregate_1, '
        'to_char("functiondemomodel"."created_at", \'YYYY-MM-DD\') AS aggregate_2, '
        'to_number("functiondemomodel"."balance_str", \'999999.99\') AS aggregate_3, '
        'to_timestamp("functiondemomodel"."timestamp_str", \'YYYY-MM-DD HH24:MI:SS\') AS aggregate_4 '
        'FROM "functiondemomodel"',
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
        'SELECT "userdemo"."id" AS "userdemo_id" FROM "userdemo" WHERE ("userdemo"."name" IS NOT DISTINCT FROM "userdemo"."email" AND "userdemo"."id" > $1)',
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
        'SELECT "userdemo"."id" AS "userdemo_id" FROM "userdemo" WHERE ("userdemo"."name" IS NOT DISTINCT FROM "userdemo"."email" OR "userdemo"."id" > $1)',
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
        'SELECT "userdemo"."id" AS "userdemo_id" FROM "userdemo" WHERE (("userdemo"."name" IS NOT DISTINCT FROM "userdemo"."email" OR "userdemo"."id" > $1) AND "userdemo"."id" < $2)',
        [0, 10],
    )


#
# Typehinting
# These checks are run AS part of the static typechecking we do
# for our codebase, not AS part of the pytest runtime.
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

    assert query_1._limit_value == 1
    assert query_2._limit_value == 2


def test_distinct_on():
    new_query = (
        QueryBuilder()
        .select((UserDemo.name, UserDemo.email))
        .distinct_on(UserDemo.name)
    )
    assert new_query.build() == (
        'SELECT DISTINCT ON ("userdemo"."name") "userdemo"."name" AS "userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo"',
        [],
    )


def test_distinct_on_multiple_fields():
    new_query = (
        QueryBuilder()
        .select((UserDemo.name, UserDemo.email))
        .distinct_on(UserDemo.name, UserDemo.email)
    )
    assert new_query.build() == (
        'SELECT DISTINCT ON ("userdemo"."name", "userdemo"."email") "userdemo"."name" AS "userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo"',
        [],
    )


def test_for_update_basic():
    new_query = QueryBuilder().select(UserDemo).for_update()
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" FOR UPDATE',
        [],
    )


def test_for_update_nowait():
    new_query = QueryBuilder().select(UserDemo).for_update(nowait=True)
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" FOR UPDATE NOWAIT',
        [],
    )


def test_for_update_skip_locked():
    new_query = QueryBuilder().select(UserDemo).for_update(skip_locked=True)
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" FOR UPDATE SKIP LOCKED',
        [],
    )


def test_for_update_of():
    new_query = QueryBuilder().select(UserDemo).for_update(of=(UserDemo,))
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" FOR UPDATE OF "userdemo"',
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
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" FOR UPDATE OF "userdemo" NOWAIT',
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
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS "userdemo_name", "userdemo"."email" AS "userdemo_email", '
        '"artifactdemo"."id" AS "artifactdemo_id", "artifactdemo"."title" AS "artifactdemo_title", "artifactdemo"."user_id" AS "artifactdemo_user_id" '
        'FROM "userdemo" INNER JOIN "artifactdemo" ON "userdemo"."id" = "artifactdemo"."user_id" '
        'FOR UPDATE OF "artifactdemo", "userdemo"',
        [],
    )


def test_function_cast_enum():
    """
    Test casting to enum types.
    """

    class UserStatus(StrEnum):
        ACTIVE = "active"
        INACTIVE = "inactive"
        PENDING = "pending"

    class UserLevel(IntEnum):
        BASIC = 1
        PREMIUM = 2
        VIP = 3

    # Test casting to StrEnum
    new_query = QueryBuilder().select(func.cast(FunctionDemoModel.name, UserStatus))
    assert new_query.build() == (
        'SELECT cast("functiondemomodel"."name" as userstatus) AS aggregate_0 '
        'FROM "functiondemomodel"',
        [],
    )

    # Test casting to IntEnum
    new_query = QueryBuilder().select(func.cast(FunctionDemoModel.balance, UserLevel))
    assert new_query.build() == (
        'SELECT cast("functiondemomodel"."balance" as userlevel) AS aggregate_0 '
        'FROM "functiondemomodel"',
        [],
    )


def test_function_in():
    """
    Test the in_ function with a list of values.
    """
    # Test with direct in_ method
    new_query = (
        QueryBuilder()
        .select(UserDemo)
        .where(func.in_(UserDemo.name, ["John", "Jane", "Bob"]))
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" '
        'WHERE "userdemo"."name" IN ($1, $2, $3)',
        ["John", "Jane", "Bob"],
    )

    # Test with a single value
    new_query = QueryBuilder().select(UserDemo).where(func.in_(UserDemo.name, "John"))
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" '
        'WHERE "userdemo"."name" IN ($1)',
        ["John"],
    )

    # Test with empty list
    new_query = QueryBuilder().select(UserDemo).where(func.in_(UserDemo.name, []))
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" '
        'WHERE "userdemo"."name" IN ()',
        [],
    )


def test_function_in_subquery():
    """
    Test the in_ function with a subquery.
    """
    # Create a subquery
    subquery = (
        QueryBuilder().select(ArtifactDemo.user_id).where(ArtifactDemo.title == "Test")
    )

    # Use the subquery with in_
    new_query = QueryBuilder().select(UserDemo).where(func.in_(UserDemo.id, subquery))

    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" '
        'WHERE "userdemo"."id" IN (SELECT "artifactdemo"."user_id" AS '
        '"artifactdemo_user_id" FROM "artifactdemo" WHERE "artifactdemo"."title" = $1)',
        ["Test"],
    )


def test_function_in_with_other_conditions():
    """
    Test the in_ function combined with other conditions.
    """
    # Test with AND conditions
    new_query = (
        QueryBuilder()
        .select(UserDemo)
        .where(func.in_(UserDemo.name, ["John", "Jane"]), UserDemo.id > 10)
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" '
        'WHERE "userdemo"."name" IN ($1, $2) AND "userdemo"."id" > $3',
        ["John", "Jane", 10],
    )

    # Test with OR conditions using properly typed conditions
    new_query = (
        QueryBuilder()
        .select(UserDemo)
        .where(
            or_(
                UserDemo.name == "John",  # Use proper boolean comparison
                UserDemo.id > 10,
            )
        )
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" '
        'WHERE ("userdemo"."name" = $1 OR "userdemo"."id" > $2)',
        ["John", 10],
    )


def test_function_in_complex_queries():
    """
    Test the in_ function in more complex query scenarios.
    """
    # Test with multiple IN conditions
    new_query = (
        QueryBuilder()
        .select(UserDemo)
        .where(
            func.in_(UserDemo.name, ["John", "Jane"]),
            func.in_(UserDemo.email, ["john@example.com", "jane@example.com"]),
        )
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "userdemo"."name" AS '
        '"userdemo_name", "userdemo"."email" AS "userdemo_email" FROM "userdemo" '
        'WHERE "userdemo"."name" IN ($1, $2) AND "userdemo"."email" IN ($3, $4)',
        ["John", "Jane", "john@example.com", "jane@example.com"],
    )

    # Test with joins and IN condition
    new_query = (
        QueryBuilder()
        .select((UserDemo.id, ArtifactDemo.title))
        .join(ArtifactDemo, UserDemo.id == ArtifactDemo.user_id)
        .where(func.in_(ArtifactDemo.title, ["Report", "Article", "Blog"]))
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" AS "userdemo_id", "artifactdemo"."title" AS "artifactdemo_title" '
        'FROM "userdemo" INNER JOIN "artifactdemo" ON "userdemo"."id" = "artifactdemo"."user_id" '
        'WHERE "artifactdemo"."title" IN ($1, $2, $3)',
        ["Report", "Article", "Blog"],
    )


def test_multiple_group_by():
    new_query = (
        QueryBuilder()
        .select(
            (
                Employee.department,
                Employee.last_name,
                func.count(Employee.id),
                func.avg(Employee.salary),
            )
        )
        .group_by(Employee.department)
        .group_by(Employee.last_name)
    )
    assert new_query.build() == (
        'SELECT "employee"."department" AS "employee_department", '
        '"employee"."last_name" AS "employee_last_name", '
        'count("employee"."id") AS aggregate_0, '
        'avg("employee"."salary") AS aggregate_1 '
        'FROM "employee" '
        'GROUP BY "employee"."department", "employee"."last_name"',
        [],
    )


def test_group_by_with_function():
    new_query = (
        QueryBuilder()
        .select(
            (
                func.date_trunc("month", FunctionDemoModel.created_at),
                func.count(FunctionDemoModel.id),
            )
        )
        .group_by(func.date_trunc("month", FunctionDemoModel.created_at))
    )
    assert new_query.build() == (
        'SELECT date_trunc(\'month\', "functiondemomodel"."created_at") AS aggregate_0, count("functiondemomodel"."id") AS aggregate_1 FROM "functiondemomodel" GROUP BY date_trunc(\'month\', "functiondemomodel"."created_at")',
        [],
    )
