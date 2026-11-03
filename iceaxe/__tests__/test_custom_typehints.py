from datetime import date, datetime, time, timedelta, timezone
from enum import IntEnum
from typing import Annotated, Any, NewType, cast
from unittest.mock import patch
from uuid import UUID

import pytest
from asyncpg.pgproto.pgproto import UUID as AsyncpgUUID
from pydantic import ValidationError, field_validator, model_validator

from iceaxe import Field, TableBase
from iceaxe.custom_typehints import (
    coerce_single_subclass_value,
    convert_simple_subclass_value,
    get_simple_subclass_converter,
    get_simple_subclass_kind,
)


@pytest.fixture
def clear_table():
    # These coercion tests don't need the autouse database fixture.
    pass


@pytest.mark.parametrize(
    "base_type, value",
    [
        (UUID, UUID(int=0)),
        (UUID, UUID(int=(1 << 128) - 1)),
        (UUID, AsyncpgUUID("12345678-1234-5678-1234-567812345678")),
        (datetime, datetime(2024, 11, 3, 1, 30, tzinfo=timezone.utc, fold=1)),
        (date, date(2024, 2, 29)),
        (time, time(1, 30, 45, 123456, tzinfo=timezone.utc, fold=1)),
        (timedelta, timedelta(days=-2, seconds=3, microseconds=4)),
        (bytes, b"\x00\xff"),
        (str, "caf\u00e9"),
        (int, 2**63 - 1),
        (float, 1.25),
    ],
)
def test_simple_subclass_round_trip(base_type, value):
    subclass = type("Subclass", (base_type,), {})
    converter = get_simple_subclass_converter(subclass, to_db=False)
    assert converter is not None
    with patch(
        "iceaxe.custom_typehints.get_simple_subclass_kind",
        side_effect=AssertionError("Reclassified a value inside the row loop"),
    ):
        converted = converter(value)
        assert type(converted) is subclass
        assert converted == value
        assert converter(converted) is converted
        assert converter(None) is None

    stored = convert_simple_subclass_value(converted, subclass, to_db=True)
    assert type(stored) is base_type
    assert stored == value
    if isinstance(value, (datetime, time)):
        assert converted.tzinfo is stored.tzinfo is value.tzinfo
        assert converted.fold == stored.fold == value.fold


def test_wrapped_subclass_arrays():
    class CustomUUID(UUID):
        pass

    Identifier = NewType("Identifier", CustomUUID)
    annotation = Annotated[list[Identifier] | None, {"unhashable": []}]
    converter = get_simple_subclass_converter(annotation, to_db=False)
    assert converter is not None
    raw = [UUID(int=0), UUID(int=2**128 - 1)]
    converted = converter(raw)
    assert converted == raw
    assert all(type(item) is CustomUUID for item in converted)
    assert converter([]) == []
    assert converter(None) is None
    assert all(
        type(item) is UUID
        for item in convert_simple_subclass_value(converted, annotation, to_db=True)
    )


def test_uuid_custom_constructors_are_preserved():
    calls = []

    class CustomInit(UUID):
        def __init__(self, value):
            calls.append(("init", value))
            assert isinstance(value, str)
            super().__init__(value)
            if self.int == 0:
                raise ValueError("Zero is forbidden")

    class CustomNew(UUID):
        def __new__(cls, value):
            calls.append(("new", value))
            assert isinstance(value, str)
            return super().__new__(cls)

    raw = UUID(int=1)
    for subtype in (CustomInit, CustomNew):
        converter = get_simple_subclass_converter(subtype, to_db=False)
        assert converter is not None
        converted = converter(raw)
        assert type(converted) is subtype
        assert converted == raw
    assert calls == [("init", str(raw)), ("new", str(raw))]
    with pytest.raises(ValueError, match="Zero is forbidden"):
        coerce_single_subclass_value(UUID(int=0), CustomInit)


def test_uuid_strings_and_invalid_values():
    class CustomUUID(UUID):
        pass

    converter = get_simple_subclass_converter(CustomUUID, to_db=False)
    assert converter is not None
    raw = UUID(int=42)
    assert converter(str(raw)) == raw
    assert type(converter(str(raw))) is CustomUUID
    with pytest.raises(ValueError):
        converter("not a UUID")


def test_plain_types_and_enums_are_not_coerced():
    class Status(IntEnum):
        ACTIVE = 1

    for annotation in (UUID, int, str, Status, dict[str, Any], None):
        assert get_simple_subclass_converter(annotation, to_db=False) is None
    assert get_simple_subclass_kind(Status) is None
    with pytest.raises(TypeError, match="Unsupported simple subclass"):
        coerce_single_subclass_value(1, object)


def test_model_validation_and_subclass_identity_are_preserved():
    class CustomUUID(UUID):
        pass

    calls = []

    class Row(TableBase, autodetect=False):
        id: CustomUUID
        optional_id: CustomUUID | None = None
        ids: list[CustomUUID]
        score: int = Field(ge=0)

        @field_validator("id")
        @classmethod
        def validate_id(cls, value):
            assert type(value) is CustomUUID
            calls.append("field")
            return value

        @model_validator(mode="after")
        def validate_row(self):
            calls.append("model")
            if self.id in self.ids:
                raise ValueError("Duplicate ID")
            return self

    raw = AsyncpgUUID("12345678-1234-5678-1234-567812345678")
    with patch(
        "iceaxe.custom_typehints.get_simple_subclass_kind",
        side_effect=AssertionError("Reclassified a value during validation"),
    ):
        row = cast(Any, Row)(id=raw, ids=[str(UUID(int=1))], score="2")
    assert type(row.id) is CustomUUID
    assert type(row.ids[0]) is CustomUUID
    assert row.optional_id is None
    assert row.score == 2
    assert row.get_modified_attributes() == {}
    assert calls == ["field", "model"]
    assert Row.model_validate_json(row.model_dump_json()) == row
    row.score = 3
    assert row.get_modified_attributes() == {"score": 3}
    for invalid in (
        {"id": "invalid", "ids": [], "score": 1},
        {"id": raw, "ids": [raw], "score": 1},
        {"id": raw, "ids": [], "score": -1},
    ):
        with pytest.raises(ValidationError):
            cast(Any, Row)(**invalid)
