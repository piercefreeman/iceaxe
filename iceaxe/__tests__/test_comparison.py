from re import compile as re_compile
from typing import Any

import pytest
from typing_extensions import assert_type

from iceaxe.__tests__.conf_models import UserDemo
from iceaxe.__tests__.helpers import pyright_raises
from iceaxe.base import TableBase
from iceaxe.comparison import ComparisonType, FieldComparison
from iceaxe.field import DBFieldClassDefinition, DBFieldInfo
from iceaxe.queries_str import QueryLiteral
from iceaxe.sql_types import ColumnType
from iceaxe.typing import column


def test_comparison_type_enum():
    assert ComparisonType.EQ == "="
    assert ComparisonType.NE == "!="
    assert ComparisonType.LT == "<"
    assert ComparisonType.LE == "<="
    assert ComparisonType.GT == ">"
    assert ComparisonType.GE == ">="
    assert ComparisonType.IN == "IN"
    assert ComparisonType.NOT_IN == "NOT IN"
    assert ComparisonType.LIKE == "LIKE"
    assert ComparisonType.NOT_LIKE == "NOT LIKE"
    assert ComparisonType.ILIKE == "ILIKE"
    assert ComparisonType.NOT_ILIKE == "NOT ILIKE"
    assert ComparisonType.IS == "IS"
    assert ComparisonType.IS_NOT == "IS NOT"
    assert ComparisonType.IS_DISTINCT_FROM == "IS DISTINCT FROM"
    assert ComparisonType.IS_NOT_DISTINCT_FROM == "IS NOT DISTINCT FROM"


@pytest.fixture
def db_field():
    return DBFieldClassDefinition(
        root_model=TableBase, key="test_key", field_definition=DBFieldInfo()
    )


def test_eq(db_field: DBFieldClassDefinition):
    result = db_field == 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.EQ
    assert result.right == 5


def test_eq_none(db_field: DBFieldClassDefinition):
    result = db_field == None  # noqa: E711
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.IS
    assert result.right is None


def test_ne(db_field: DBFieldClassDefinition):
    result = db_field != 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.NE
    assert result.right == 5


def test_ne_none(db_field: DBFieldClassDefinition):
    result = db_field != None  # noqa: E711
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.IS_NOT
    assert result.right is None


def test_lt(db_field: DBFieldClassDefinition):
    result = db_field < 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.LT
    assert result.right == 5


def test_le(db_field):
    result = db_field <= 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.LE
    assert result.right == 5


def test_gt(db_field: DBFieldClassDefinition):
    result = db_field > 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.GT
    assert result.right == 5


def test_ge(db_field: DBFieldClassDefinition):
    result = db_field >= 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.GE
    assert result.right == 5


def test_in(db_field: DBFieldClassDefinition):
    result = db_field.in_([1, 2, 3])
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.IN
    assert result.right == [1, 2, 3]


def test_not_in(db_field: DBFieldClassDefinition):
    result = db_field.not_in([1, 2, 3])
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.NOT_IN
    assert result.right == [1, 2, 3]


def test_contains(db_field: DBFieldClassDefinition):
    result = db_field.like("test")
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.LIKE
    assert result.right == "test"


def test_compare(db_field: DBFieldClassDefinition):
    result = db_field._compare(ComparisonType.EQ, 10)
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.EQ
    assert result.right == 10


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        0,
        [],
        {},
        True,
        False,
        3.14,
        complex(1, 2),
        DBFieldClassDefinition(
            root_model=TableBase, key="other_key", field_definition=DBFieldInfo()
        ),
    ],
)
def test_comparison_with_different_types(db_field: DBFieldClassDefinition, value: Any):
    for method in [
        db_field.__eq__,
        db_field.__ne__,
        db_field.__lt__,
        db_field.__le__,
        db_field.__gt__,
        db_field.__ge__,
        db_field.in_,
        db_field.not_in,
        db_field.like,
    ]:
        result = method(value)
        assert isinstance(result, FieldComparison)
        assert result.left == db_field
        assert isinstance(result.comparison, ComparisonType)
        assert result.right == value


#
# Typehinting
# These checks are run as part of the static typechecking we do
# for our codebase, not as part of the pytest runtime.
#


def test_typehint_like():
    class UserDemo(TableBase):
        id: int
        value_str: str
        value_int: int

    str_col = column(UserDemo.value_str)
    int_col = column(UserDemo.value_int)

    assert_type(str_col, DBFieldClassDefinition[str])
    assert_type(int_col, DBFieldClassDefinition[int])

    assert_type(str_col.ilike("test"), bool)
    assert_type(str_col.not_ilike("test"), bool)
    assert_type(str_col.like("test"), bool)
    assert_type(str_col.not_like("test"), bool)

    with pyright_raises(
        "reportAttributeAccessIssue",
        matches=re_compile('Cannot access attribute "ilike"'),
    ):
        int_col.ilike(5)  # type: ignore

    with pyright_raises(
        "reportAttributeAccessIssue",
        matches=re_compile('Cannot access attribute "ilike"'),
    ):
        int_col.not_ilike(5)  # type: ignore

    with pyright_raises(
        "reportAttributeAccessIssue",
        matches=re_compile('Cannot access attribute "ilike"'),
    ):
        int_col.like(5)  # type: ignore

    with pyright_raises(
        "reportAttributeAccessIssue",
        matches=re_compile('Cannot access attribute "ilike"'),
    ):
        int_col.not_like(5)  # type: ignore


def test_typehint_in():
    class UserDemo(TableBase):
        id: int
        value_str: str
        value_int: int

    str_col = column(UserDemo.value_str)
    int_col = column(UserDemo.value_int)

    assert_type(str_col.in_(["test"]), bool)
    assert_type(int_col.in_([5]), bool)

    assert_type(str_col.not_in(["test"]), bool)
    assert_type(int_col.not_in([5]), bool)

    with pyright_raises(
        "reportArgumentType",
        matches=re_compile('cannot be assigned to parameter "other"'),
    ):
        str_col.in_(["test", 5])  # type: ignore

    with pyright_raises(
        "reportArgumentType",
        matches=re_compile('cannot be assigned to parameter "other"'),
    ):
        str_col.not_in(["test", 5])  # type: ignore


@pytest.mark.parametrize(
    "comparison_type,expected_sql",
    [
        (ComparisonType.IN, '"userdemo"."name" = ANY($1)'),
        (ComparisonType.NOT_IN, '"userdemo"."name" != ALL($1)'),
    ],
)
def test_in_not_in_formatting(comparison_type: ComparisonType, expected_sql: str):
    """
    Test that in_ and not_in operators correctly format to ANY and ALL in SQL.
    """
    comparison = FieldComparison(
        left=column(UserDemo.name), comparison=comparison_type, right=["John", "Jane"]
    )
    query, variables = comparison.to_query()
    assert isinstance(query, QueryLiteral)
    assert str(query) == expected_sql
    assert variables == [["John", "Jane"]]


def test_default_eq_ne_are_null_safe(db_field: DBFieldClassDefinition):
    """
    Test that the default == and != operators use null-safe comparisons
    """
    # Test == None uses IS NULL
    eq_none = db_field == None  # noqa: E711
    assert isinstance(eq_none, FieldComparison)
    assert eq_none.comparison == ComparisonType.IS

    # Test != None uses IS NOT NULL
    ne_none = db_field != None  # noqa: E711
    assert isinstance(ne_none, FieldComparison)
    assert ne_none.comparison == ComparisonType.IS_NOT

    # Test == column uses IS NOT DISTINCT FROM
    other_field = DBFieldClassDefinition(
        root_model=TableBase, key="other_key", field_definition=DBFieldInfo()
    )
    eq_col = db_field == other_field
    assert isinstance(eq_col, FieldComparison)
    assert eq_col.comparison == ComparisonType.IS_NOT_DISTINCT_FROM

    # Test != column uses IS DISTINCT FROM
    ne_col = db_field != other_field
    assert isinstance(ne_col, FieldComparison)
    assert ne_col.comparison == ComparisonType.IS_DISTINCT_FROM


@pytest.mark.parametrize(
    "magic_method,value",
    [
        ("__eq__", 5),
        ("__ne__", 5),
        ("__lt__", 5),
        ("__le__", 5),
        ("__gt__", 5),
        ("__ge__", 5),
    ],
)
def test_python_magic_methods_set_expression_flag(
    db_field: DBFieldClassDefinition, magic_method: str, value: Any
):
    """
    Test that all Python magic methods set python_expression to True
    """
    comparison = getattr(db_field, magic_method)(value)
    assert isinstance(comparison, FieldComparison)
    assert comparison.python_expression is True


@pytest.mark.parametrize(
    "initial_comparison, python_expression, expected_comparison",
    [
        (ComparisonType.IS_NOT_DISTINCT_FROM, True, ComparisonType.EQ),
        (ComparisonType.IS_DISTINCT_FROM, True, ComparisonType.NE),
        (
            ComparisonType.IS_NOT_DISTINCT_FROM,
            False,
            ComparisonType.IS_NOT_DISTINCT_FROM,
        ),
        (ComparisonType.IS_DISTINCT_FROM, False, ComparisonType.IS_DISTINCT_FROM),
    ],
)
def test_force_join_constraints(
    initial_comparison: ComparisonType,
    python_expression: bool,
    expected_comparison: ComparisonType,
):
    """
    Test that force_join_constraints correctly transforms comparison types
    """
    db_field = DBFieldClassDefinition(
        root_model=TableBase, key="test_key", field_definition=DBFieldInfo()
    )
    other_field = DBFieldClassDefinition(
        root_model=TableBase, key="other_key", field_definition=DBFieldInfo()
    )

    comparison = FieldComparison(
        left=db_field,
        comparison=initial_comparison,
        right=other_field,
        python_expression=python_expression,
    )
    forced = comparison.force_join_constraints()
    assert forced.comparison == expected_comparison


@pytest.mark.parametrize(
    "sql_type_string, expected_column_type",
    [
        ("timestamp", ColumnType.TIMESTAMP_WITHOUT_TIME_ZONE),  # Tests aliasing
        ("timestamp without time zone", ColumnType.TIMESTAMP_WITHOUT_TIME_ZONE),
        ("timestamp with time zone", ColumnType.TIMESTAMP_WITH_TIME_ZONE),
        ("time", ColumnType.TIME_WITHOUT_TIME_ZONE),  # Tests aliasing
        ("time without time zone", ColumnType.TIME_WITHOUT_TIME_ZONE),
        ("time with time zone", ColumnType.TIME_WITH_TIME_ZONE),
    ],
)
def test_postgres_datetime_timezone_casting(
    sql_type_string: str, expected_column_type: ColumnType
):
    """
    Test that PostgresDateTime fields with different timezone configurations
    are properly handled by the ColumnType enum, specifically testing that
    PostgreSQL's storage format ('timestamp without time zone') can be parsed.
    This also tests that SQL standard aliases like "timestamp" correctly map
    to "timestamp without time zone".
    """

    # Test that ColumnType enum can handle PostgreSQL's storage formats and aliases
    assert ColumnType(sql_type_string) == expected_column_type
