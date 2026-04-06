from __future__ import annotations

import types
from dataclasses import dataclass
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


@dataclass(frozen=True)
class ResolvedFieldAnnotation:
    runtime_annotation: Any
    storage_annotation: Any
    is_list: bool
    is_nullable: bool
    is_simple_subclass: bool


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


def _rebuild_annotation(annotation: Any, args: tuple[Any, ...]):
    if _is_union_type(annotation):
        return Union[args]  # type: ignore

    origin = get_origin(annotation)
    if origin is None:
        return annotation

    item = args[0] if len(args) == 1 else args
    return origin[item]


def unwrap_annotated(annotation: Any) -> Any:
    while get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    return annotation


def _get_optional_annotation_inner(annotation: Any) -> Any | None:
    if not _is_union_type(annotation):
        return None

    non_null_args = tuple(
        arg for arg in get_args(annotation) if unwrap_annotated(arg) is not type(None)
    )
    if len(non_null_args) != 1:
        return None

    return non_null_args[0]


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


def resolve_field_annotation(annotation: Any) -> ResolvedFieldAnnotation:
    is_list = False
    is_nullable = False
    current = annotation

    while True:
        current = unwrap_annotated(current)

        optional_inner = _get_optional_annotation_inner(current)
        if optional_inner is not None:
            is_nullable = True
            current = optional_inner
            continue

        if not is_list and get_origin(current) is list:
            (current,) = get_args(current)
            is_list = True
            continue

        break

    current = unwrap_annotated(current)
    base_type = get_simple_subclass_base_type(current)

    return ResolvedFieldAnnotation(
        runtime_annotation=current,
        storage_annotation=base_type or current,
        is_list=is_list,
        is_nullable=is_nullable,
        is_simple_subclass=base_type is not None,
    )


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

    if origin is not None:
        normalized_args = tuple(
            normalize_simple_subclass_annotation(arg) for arg in get_args(annotation)
        )
        if normalized_args == get_args(annotation):
            return annotation
        return _rebuild_annotation(annotation, normalized_args)

    resolved = resolve_field_annotation(annotation)
    if not resolved.is_simple_subclass:
        return annotation

    return Annotated[
        annotation,
        SimpleSubclassAnnotation(
            resolved.runtime_annotation,
            resolved.storage_annotation,
        ),
    ]


def get_db_storage_annotation(annotation: Any) -> tuple[Any, bool]:
    resolved = resolve_field_annotation(annotation)
    return resolved.storage_annotation, resolved.is_list


def convert_value_to_db_storage(value: Any, annotation: Any) -> Any:
    return _convert_simple_subclass_value(value, annotation, to_db=True)


def convert_value_from_db_storage(value: Any, annotation: Any) -> Any:
    return _convert_simple_subclass_value(value, annotation, to_db=False)


def _convert_simple_subclass_value(value: Any, annotation: Any, *, to_db: bool) -> Any:
    if value is None:
        return None

    resolved = resolve_field_annotation(annotation)
    if not resolved.is_simple_subclass:
        return value

    target_type = resolved.storage_annotation if to_db else resolved.runtime_annotation
    if resolved.is_list:
        return [_coerce_simple_subclass_value(item, target_type) for item in value]

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
