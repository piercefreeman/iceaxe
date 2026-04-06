from __future__ import annotations

import types
from datetime import date, datetime, time, timedelta
from enum import Enum, IntEnum, StrEnum
from inspect import isclass
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Type,
    TypeGuard,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from uuid import UUID

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema

if TYPE_CHECKING:
    from iceaxe.alias_values import Alias
    from iceaxe.base import (
        DBFieldClassDefinition,
        TableBase,
    )
    from iceaxe.comparison import FieldComparison, FieldComparisonGroup
    from iceaxe.functions import FunctionMetadata


ALL_ENUM_TYPES = Type[Enum | StrEnum | IntEnum]
PRIMITIVE_TYPES = int | float | str | bool | bytes | UUID
PRIMITIVE_WRAPPER_TYPES = list[PRIMITIVE_TYPES] | PRIMITIVE_TYPES
DATE_TYPES = datetime | date | time | timedelta
JSON_WRAPPER_FALLBACK = list[Any] | dict[Any, Any]
SIMPLE_SUBCLASS_BASE_TYPES = (
    datetime,
    date,
    time,
    timedelta,
    UUID,
    bytes,
    str,
    int,
    float,
    bool,
)

T = TypeVar("T")


class SimpleSubclassAnnotation:
    def __init__(self, subtype: type[Any], base_type: type[Any]):
        self.subtype = subtype
        self.base_type = base_type

    def __get_pydantic_core_schema__(
        self,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        schema = handler.generate_schema(self.base_type)
        return core_schema.no_info_after_validator_function(
            self._cast_value,
            schema,
        )

    def _cast_value(self, value: Any):
        return _coerce_simple_subclass_value(value, self.subtype)


def _is_union_type(annotation: Any) -> bool:
    origin = get_origin(annotation)
    return origin is Union or isinstance(annotation, types.UnionType)


def _rebuild_annotation(origin: Any, args: tuple[Any, ...]):
    if not args:
        return origin
    if len(args) == 1:
        item = args[0]
        if hasattr(origin, "__class_getitem__"):
            return origin.__class_getitem__(item)
        return origin[item]
    if hasattr(origin, "__class_getitem__"):
        return origin.__class_getitem__(args)
    return origin[args]


def unwrap_annotated(annotation: Any) -> Any:
    while get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    return annotation


def get_simple_subclass_base_type(annotation: Any) -> type[Any] | None:
    annotation = unwrap_annotated(annotation)
    if not isclass(annotation) or annotation in SIMPLE_SUBCLASS_BASE_TYPES:
        return None
    if issubclass(annotation, Enum):
        return None

    mro = annotation.mro()
    matches = [
        (mro.index(base_type), base_type)
        for base_type in SIMPLE_SUBCLASS_BASE_TYPES
        if base_type in mro
    ]
    if not matches:
        return None

    return min(matches, key=lambda match: match[0])[1]


def normalize_simple_subclass_annotation(annotation: Any) -> Any:
    origin = get_origin(annotation)

    if origin is Annotated:
        inner, *metadata = get_args(annotation)
        if any(isinstance(item, SimpleSubclassAnnotation) for item in metadata):
            normalized_inner = normalize_simple_subclass_annotation(inner)
            if normalized_inner == inner:
                return annotation
            return Annotated[normalized_inner, *metadata]

        base_type = get_simple_subclass_base_type(inner)
        if base_type is not None:
            return Annotated[
                inner,
                *metadata,
                SimpleSubclassAnnotation(inner, base_type),
            ]

        normalized_inner = normalize_simple_subclass_annotation(inner)
        if normalized_inner == inner:
            return annotation
        return Annotated[normalized_inner, *metadata]

    if _is_union_type(annotation):
        normalized_args = tuple(
            normalize_simple_subclass_annotation(arg) for arg in get_args(annotation)
        )
        if normalized_args == get_args(annotation):
            return annotation
        return Union[normalized_args]  # type: ignore

    if origin is not None:
        normalized_args = tuple(
            normalize_simple_subclass_annotation(arg) for arg in get_args(annotation)
        )
        if normalized_args == get_args(annotation):
            return annotation
        return _rebuild_annotation(origin, normalized_args)

    base_type = get_simple_subclass_base_type(annotation)
    if base_type is None:
        return annotation

    return Annotated[annotation, SimpleSubclassAnnotation(annotation, base_type)]


def get_db_storage_annotation(annotation: Any) -> tuple[Any, bool]:
    annotation = unwrap_annotated(annotation)

    if _is_union_type(annotation):
        non_null_args = tuple(
            arg
            for arg in get_args(annotation)
            if unwrap_annotated(arg) is not type(None)
        )
        if len(non_null_args) == 1:
            return get_db_storage_annotation(non_null_args[0])
        return annotation, False

    origin = get_origin(annotation)
    if origin is list:
        (value_type,) = get_args(annotation)
        resolved_type, _ = get_db_storage_annotation(value_type)
        return resolved_type, True

    base_type = get_simple_subclass_base_type(annotation)
    if base_type is not None:
        return base_type, False
    return annotation, False


def convert_value_to_db_storage(value: Any, annotation: Any) -> Any:
    return _convert_simple_subclass_value(value, annotation, to_db=True)


def convert_value_from_db_storage(value: Any, annotation: Any) -> Any:
    return _convert_simple_subclass_value(value, annotation, to_db=False)


def _convert_simple_subclass_value(value: Any, annotation: Any, *, to_db: bool) -> Any:
    if value is None:
        return None

    annotation = unwrap_annotated(annotation)
    if _is_union_type(annotation):
        non_null_args = tuple(
            arg
            for arg in get_args(annotation)
            if unwrap_annotated(arg) is not type(None)
        )
        if len(non_null_args) == 1:
            return _convert_simple_subclass_value(
                value,
                non_null_args[0],
                to_db=to_db,
            )
        return value

    origin = get_origin(annotation)
    if origin is list:
        (value_type,) = get_args(annotation)
        return [
            _convert_simple_subclass_value(item, value_type, to_db=to_db)
            for item in value
        ]

    base_type = get_simple_subclass_base_type(annotation)
    if base_type is None:
        return value

    target_type = base_type if to_db else annotation
    return _coerce_simple_subclass_value(value, target_type)


def _coerce_simple_subclass_value(value: Any, target_type: type[Any]) -> Any:
    if type(value) is target_type:
        return value

    if issubclass(target_type, UUID):
        return target_type(str(value))

    if issubclass(target_type, datetime):
        return target_type(
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            tzinfo=value.tzinfo,
            fold=value.fold,
        )

    if issubclass(target_type, date):
        return target_type(
            value.year,
            value.month,
            value.day,
        )

    if issubclass(target_type, time):
        return target_type(
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            tzinfo=value.tzinfo,
            fold=value.fold,
        )

    if issubclass(target_type, timedelta):
        return target_type(
            days=value.days,
            seconds=value.seconds,
            microseconds=value.microseconds,
        )

    return target_type(value)


def is_base_table(obj: Any) -> TypeGuard[type[TableBase]]:
    from iceaxe.base import TableBase

    return isclass(obj) and issubclass(obj, TableBase)


def is_column(obj: T) -> TypeGuard[DBFieldClassDefinition[T]]:
    from iceaxe.base import DBFieldClassDefinition

    return isinstance(obj, DBFieldClassDefinition)


def is_comparison(obj: Any) -> TypeGuard[FieldComparison]:
    from iceaxe.comparison import FieldComparison

    return isinstance(obj, FieldComparison)


def is_comparison_group(obj: Any) -> TypeGuard[FieldComparisonGroup]:
    from iceaxe.comparison import FieldComparisonGroup

    return isinstance(obj, FieldComparisonGroup)


def is_function_metadata(obj: Any) -> TypeGuard[FunctionMetadata]:
    from iceaxe.functions import FunctionMetadata

    return isinstance(obj, FunctionMetadata)


def is_alias(obj: Any) -> TypeGuard[Alias]:
    from iceaxe.alias_values import Alias

    return isinstance(obj, Alias)


def column(obj: T) -> DBFieldClassDefinition[T]:
    if not is_column(obj):
        raise ValueError(f"Invalid column: {obj}")
    return obj
