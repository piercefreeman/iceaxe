import gc
import weakref
from typing import Any, cast
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
from pydantic import ValidationError, field_validator, model_validator

from iceaxe import Field, TableBase, alias, func, select
from iceaxe.custom_typehints import get_simple_subclass_kind
from iceaxe.field import DBFieldInfo
from iceaxe.session import DBConnection
from iceaxe.session_optimized import optimize_exec_casting
from iceaxe.typing import column, is_base_table, is_column, is_function_metadata


@pytest.fixture
def clear_table():
    # Exercise the decoder directly without the autouse database fixture.
    pass


def decode(values, selections):
    query = select(selections)
    types = [
        (is_base_table(raw), is_column(raw), is_function_metadata(raw))
        for raw in query._select_raw
    ]
    return optimize_exec_casting(values, query._select_raw, types)


def test_model_decoding_preserves_validation_and_initialization():
    calls = []

    class Row(TableBase, autodetect=False):
        id: int
        payload: dict[str, int] = Field(is_json=True)
        hidden: str = Field(default="default", exclude=True)

        def __init__(self, **data):
            calls.append("init")
            super().__init__(**data)

        @field_validator("id")
        @classmethod
        def positive_id(cls, value):
            calls.append("field")
            if value < 0:
                raise ValueError("negative ID")
            return value

        @model_validator(mode="after")
        def record_validation(self):
            calls.append("model")
            return self

        def model_post_init(self, context):
            calls.append("post_init")

    raw = {"row_id": "1", "row_payload": '{"count": "2"}'}
    results = decode([raw, raw], Row)
    for result in results:
        assert result.id == 1
        assert result.payload == {"count": 2}
        assert result.hidden == "default"
        assert result.get_modified_attributes() == {}
    assert calls == ["init", "field", "post_init", "model"] * 2
    results[0].id = 3
    assert results[0].get_modified_attributes() == {"id": 3}
    assert results[1].get_modified_attributes() == {}
    assert (
        results[0].modified_attrs_callbacks is not results[1].modified_attrs_callbacks
    )
    with pytest.raises(ValidationError, match="negative ID"):
        decode([{**raw, "row_id": -1}], Row)
    with pytest.raises(ValidationError):
        decode([{**raw, "row_payload": '{"count": "invalid"}'}], Row)


def test_mixed_selections_and_null_outer_join():
    class Row(TableBase, autodetect=False):
        id: int
        payload: dict[str, int] | None = Field(is_json=True)

    count = func.count(Row.id)
    query = select((Row, Row.payload, count, alias("label", str)))
    count_raw = query._select_raw[2]
    assert is_function_metadata(count_raw)
    count_name = count_raw.local_name
    raw = {
        "row_id": 0,
        "row_payload": '{"count": 0}',
        count_name: 2,
        "label": "first",
    }
    empty = {"row_id": None, "row_payload": None, count_name: 0, "label": None}
    results = optimize_exec_casting(
        [raw, empty],
        query._select_raw,
        [
            (True, False, False),
            (False, True, False),
            (False, False, True),
            (False, False, False),
        ],
    )
    assert results[0][0].id == 0
    assert results[0][0].payload == {"count": 0}
    assert results[0][1:] == ({"count": 0}, 2, "first")
    assert results[1] == (None, None, 0, None)
    assert decode([], Row) == []
    assert decode([empty], Row) == [None]


def test_unicode_names_and_missing_columns():
    class Row(TableBase, autodetect=False):
        table_name = "\u00e9v\u00e9nement"
        clé: int

    assert decode([{"événement_clé": 1}], Row)[0].clé == 1
    assert decode([{"événement_clé": 1}], Row.clé) == [1]
    for selection in (Row, Row.clé):
        with pytest.raises(KeyError, match="Key 'événement_clé' not found"):
            decode([{}], selection)


def test_error_releases_already_constructed_models():
    references = []

    class Row(TableBase, autodetect=False):
        id: int

        def model_post_init(self, context):
            references.append(weakref.ref(self))

    def fail():
        with pytest.raises(KeyError, match="missing"):
            decode([{"row_id": 1}], (Row, alias("missing", int)))

    for _ in range(10):
        fail()
    gc.collect()
    assert len(references) == 10
    assert all(reference() is None for reference in references)


def test_column_casts_are_precomputed_and_preserve_other_results():
    class CustomUUID(UUID):
        pass

    class Row(TableBase, autodetect=False):
        id: CustomUUID
        ids: list[CustomUUID] | None
        count: int

    connection = DBConnection(AsyncMock())
    raw = UUID(int=1)
    selections = [Row.count, Row.id, Row.ids]
    with patch(
        "iceaxe.custom_typehints.get_simple_subclass_kind",
        wraps=get_simple_subclass_kind,
    ) as classify:
        results = connection._cast_column_select_results(
            [(3, raw, [raw]), (4, None, None)] * 100, selections
        )
    assert classify.call_count == 5  # One per field, plus two target subclasses.
    assert results[0] == (3, raw, [raw])
    assert type(results[0][1]) is CustomUUID
    assert type(results[0][2][0]) is CustomUUID
    assert results[1] == (4, None, None)
    unchanged = [1, 2]
    assert connection._cast_column_select_results(unchanged, [Row.count]) is unchanged


def test_column_casts_preserve_custom_field_deserializers():
    class CustomField(DBFieldInfo):
        def from_db_value(self, value):
            return f"custom:{value}"

    class Row(TableBase, autodetect=False):
        id: int

    selected = column(Row.id)
    selected.field_definition = CustomField()
    connection = DBConnection(AsyncMock())
    assert connection._cast_column_select_results([1, None], [selected]) == [
        "custom:1",
        "custom:None",
    ]


def test_invalid_select_metadata():
    with pytest.raises(ValueError, match="same length"):
        optimize_exec_casting([], [int], [])
    with pytest.raises(TypeError, match="Unsupported select"):
        optimize_exec_casting([], [cast(Any, int)], [(False, False, False)])
