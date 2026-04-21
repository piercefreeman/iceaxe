from enum import StrEnum
from typing import Annotated, Any, Generic, TypeVar, cast
from uuid import UUID

from iceaxe.base import (
    DBModelMetaclass,
    TableBase,
)
from iceaxe.field import DBFieldInfo, Field
from iceaxe.schemas.db_memory_serializer import DatabaseMemorySerializer


class _AnnotatedDummy:
    def __get_pydantic_core_schema__(self, source_type, handler):
        return handler(source_type)


AnnotatedPayload = Annotated[dict[str, Any] | None, _AnnotatedDummy()]


def test_autodetect():
    class WillAutodetect(TableBase):
        pass

    assert WillAutodetect in DBModelMetaclass.get_registry()


def test_not_autodetect():
    class WillNotAutodetect(TableBase, autodetect=False):
        pass

    assert WillNotAutodetect not in DBModelMetaclass.get_registry()


def test_not_autodetect_generic(clear_registry):
    T = TypeVar("T")

    class GenericSuperclass(TableBase, Generic[T], autodetect=False):
        value: T

    class WillAutodetect(GenericSuperclass[int]):
        pass

    assert DBModelMetaclass.get_registry() == [WillAutodetect]


def test_generic_concrete_subclass_preserves_bound_annotations(clear_registry):
    T = TypeVar("T", bound=StrEnum)

    class MyEnum(StrEnum):
        A = "A"

    class GenericBase(TableBase, Generic[T], autodetect=False):
        typed_value: T
        user_id: UUID

    class Concrete(GenericBase[MyEnum], TableBase):
        pass

    assert {
        key: info.annotation for key, info in Concrete.get_client_fields().items()
    } == {
        "typed_value": MyEnum,
        "user_id": UUID,
    }
    assert list(DatabaseMemorySerializer().delegate([Concrete]))


def test_model_fields():
    class User(TableBase):
        id: int
        name: str

    # Check the main fields
    assert isinstance(User.model_fields["id"], DBFieldInfo)
    assert User.model_fields["id"].annotation == int  # noqa: E721
    assert User.model_fields["id"].is_required() is True

    assert isinstance(User.model_fields["name"], DBFieldInfo)
    assert User.model_fields["name"].annotation == str  # noqa: E721
    assert User.model_fields["name"].is_required() is True

    # Check that the special fields exist with the right types
    assert isinstance(User.model_fields["modified_attrs"], DBFieldInfo)
    assert isinstance(User.model_fields["modified_attrs_callbacks"], DBFieldInfo)


def test_model_fields_assignment_is_supported():
    class User(TableBase, autodetect=False):
        id: int

    user_cls = cast(Any, User)
    fields = user_cls.model_fields
    user_cls.model_fields = fields

    assert user_cls.model_fields is fields


def test_instance_model_fields_access_is_supported():
    class User(TableBase, autodetect=False):
        id: int

    user = User(id=1)
    user.id = 2

    assert user.id == 2
    assert user.model_fields["id"] is User.model_fields["id"]
    assert user.modified_attrs["id"] == 2


def test_model_fields_with_annotated_metadata():
    class Event(TableBase, autodetect=False):
        metadata: AnnotatedPayload = Field(default=None, is_json=True)

    field = Event.model_fields["metadata"]
    assert isinstance(field, DBFieldInfo)
    assert field.annotation == dict[str, Any] | None
    assert field.default is None
    assert field.is_json is True


def test_model_fields_with_simple_uuid_subclass():
    class CustomUUID(UUID):
        pass

    class Event(TableBase, autodetect=False):
        id: CustomUUID
        maybe_id: CustomUUID | None = None
        ids: list[CustomUUID]

    raw_uuid = UUID("12345678-1234-5678-1234-567812345678")
    event = cast(
        Any,
        Event,
    )(
        id=raw_uuid,
        maybe_id=str(raw_uuid),
        ids=[raw_uuid, str(raw_uuid)],
    )

    assert event.model_fields["id"].annotation == CustomUUID
    assert isinstance(event.id, CustomUUID)
    assert isinstance(event.maybe_id, CustomUUID)
    assert all(isinstance(value, CustomUUID) for value in event.ids)
