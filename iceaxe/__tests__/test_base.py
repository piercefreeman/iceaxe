from typing import Generic, TypeVar

from iceaxe.base import (
    DBModelMetaclass,
    TableBase,
)
from iceaxe.field import DBFieldInfo


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
