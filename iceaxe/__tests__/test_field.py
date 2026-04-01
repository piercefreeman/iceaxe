from typing import Any, cast

from pydantic.fields import FieldInfo

from iceaxe.base import TableBase
from iceaxe.field import DBFieldClassDefinition, DBFieldInfo


def test_db_field_class_definition_instantiation():
    field_def = DBFieldClassDefinition(
        root_model=TableBase, key="test_key", field_definition=DBFieldInfo()
    )
    assert field_def.root_model == TableBase
    assert field_def.key == "test_key"
    assert isinstance(field_def.field_definition, DBFieldInfo)


def test_extend_field_accepts_db_kwargs_already_in_attributes_set():
    # Simulate Pydantic normalizing an Iceaxe field back to FieldInfo while
    # preserving Iceaxe-specific entries in _attributes_set.
    field = FieldInfo(default=None)
    raw_field = cast(Any, field)
    raw_field.annotation = dict[str, Any] | None
    raw_field._attributes_set = {
        "primary_key": False,
        "postgres_config": None,
        "foreign_key": None,
        "unique": False,
        "index": False,
        "check_expression": None,
        "is_json": True,
        "explicit_type": None,
        "default": None,
        "annotation": dict[str, Any] | None,
    }

    extended = DBFieldInfo.extend_field(
        field,
        primary_key=False,
        postgres_config=None,
        foreign_key=None,
        unique=False,
        index=False,
        check_expression=None,
        is_json=False,
        explicit_type=None,
    )

    assert isinstance(extended, DBFieldInfo)
    assert extended.annotation == dict[str, Any] | None
    assert extended.default is None
    assert extended.is_json is True
