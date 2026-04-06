from __future__ import annotations

from datetime import date, datetime, time, timedelta
from enum import Enum
from inspect import isclass
from typing import Annotated, Any, Literal, assert_never, get_args, get_origin
from uuid import UUID

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema

from iceaxe.typing import resolve_typehint, unwrap_annotated

# This literal definition seems overly verbose for what we're doing (ie. defining the types
# of simple subclasses for which we support type coercion). But it's required so we can properly
# throw a type error during static analysis if we don't properly support a handler for one
# of these supported types.
SimpleSubclassKind = Literal[
    "datetime",
    "date",
    "time",
    "timedelta",
    "uuid",
    "bytes",
    "str",
    "int",
    "float",
    "bool",
]

SIMPLE_SUBCLASS_BASE_TYPES_BY_KIND: dict[SimpleSubclassKind, type[Any]] = {
    "datetime": datetime,
    "date": date,
    "time": time,
    "timedelta": timedelta,
    "uuid": UUID,
    "bytes": bytes,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
}
SIMPLE_SUBCLASS_BASE_TYPES = tuple(SIMPLE_SUBCLASS_BASE_TYPES_BY_KIND.values())


class SimpleSubclassAnnotation:
    """
    Pydantic metadata wrapper for "validate as base type, return subclass".

    Simple subclasses such as `CustomUUID(UUID)` are structurally compatible
    with their parent type for database storage, but Pydantic does not know how
    to build a schema for those subclasses by default. If Iceaxe passes the raw
    subclass annotation through unchanged, model construction fails because
    Pydantic treats it as an unknown arbitrary type.

    This metadata object is attached via `Annotated[...]` during
    `transform_typehint(..., wrap_simple_subclass_annotation)`. When Pydantic
    sees that annotation,
    it calls `__get_pydantic_core_schema__`, which lets us:
    - reuse the existing schema for the storage/base type (`UUID`, `date`, etc.)
    - keep all of Pydantic's normal parsing behavior for that base type
    - run one final post-validation cast that reconstructs the requested subclass

    The important constraint is that this only works for "simple" subclasses
    whose runtime value can be losslessly rebuilt from the validated base value.
    We are not defining a brand new schema here; we are explicitly piggybacking
    on the parent type's schema and restoring the subclass identity afterward.

    """

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
        return coerce_single_subclass_value(value, self.subtype)


def wrap_simple_subclass_annotation(annotation: Any) -> Any:
    """
    Wrap one annotation node with `SimpleSubclassAnnotation` when it represents
    a supported simple subclass.

    This function is intentionally small in scope: it does not walk nested type
    structures itself. Instead it is designed to be passed into
    `transform_typehint`, which recursively traverses unions, `Annotated`, and
    other generic wrappers and invokes this function on each node. At each node
    we decide whether the current type needs Pydantic metadata so it can be
    validated as its base storage type and then reconstructed as the subclass.

    If the node is already wrapped, or if it is not one of the supported
    subclass shapes, the annotation is returned unchanged.

    """
    if get_origin(annotation) is Annotated:
        inner, *metadata = get_args(annotation)
        if any(isinstance(item, SimpleSubclassAnnotation) for item in metadata):
            return annotation

        base_type = get_simple_subclass_base_type(inner)
        if base_type is None:
            return annotation

        return Annotated[
            inner,
            *metadata,
            SimpleSubclassAnnotation(inner, base_type),
        ]

    base_type = get_simple_subclass_base_type(annotation)
    if base_type is None:
        return annotation

    return Annotated[annotation, SimpleSubclassAnnotation(annotation, base_type)]


def get_simple_subclass_base_type(annotation: Any) -> type[Any] | None:
    """
    Resolve the storage/base type for a supported simple subclass annotation.

    For a value type like `CustomUUID(UUID)`, the runtime annotation is the
    subclass but the storage behavior should follow `UUID`. This helper maps the
    subclass back to that base type so callers can reason about database storage
    and coercion without losing track of the original runtime type.

    The return value is intentionally `None` for non-subclasses and for base
    types themselves. That lets callers distinguish "this annotation should be
    treated specially" from "this is already a normal built-in/base type".

    """
    annotation = unwrap_annotated(annotation)
    kind = get_simple_subclass_kind(annotation)
    if kind is None:
        return None

    base_type = SIMPLE_SUBCLASS_BASE_TYPES_BY_KIND[kind]
    if annotation is base_type:
        return None

    return base_type


def get_simple_subclass_kind(annotation: Any) -> SimpleSubclassKind | None:
    """
    Classify an annotation into one of the supported simple-subclass families.

    The subclass feature only supports a bounded set of base runtime/storage
    types, captured in `SIMPLE_SUBCLASS_BASE_TYPES_BY_KIND`. Rather than
    repeatedly branching on `issubclass(..., UUID)` / `issubclass(..., date)` in
    multiple places, we first collapse a candidate type into one stable literal
    kind. Downstream code can then switch on that kind and get both clearer
    control flow and exhaustiveness checking from static analysis.

    The function returns `None` for values that are not classes, enums, or do
    not inherit from one of the supported base types.

    """
    annotation = unwrap_annotated(annotation)
    if not isclass(annotation):
        return None
    if issubclass(annotation, Enum):
        return None

    mro = annotation.mro()
    matches: list[tuple[int, SimpleSubclassKind]] = [
        (mro.index(base_type), kind)
        for kind, base_type in SIMPLE_SUBCLASS_BASE_TYPES_BY_KIND.items()
        if base_type in mro
    ]
    if not matches:
        return None

    return min(matches, key=lambda match: match[0])[1]


def convert_simple_subclass_value(value: Any, annotation: Any, *, to_db: bool) -> Any:
    if value is None:
        return None

    resolved = resolve_typehint(annotation)
    storage_type = get_simple_subclass_base_type(resolved.runtime_type)
    if storage_type is None:
        return value

    target_type = storage_type if to_db else resolved.runtime_type
    if resolved.is_list:
        return [coerce_single_subclass_value(item, target_type) for item in value]

    return coerce_single_subclass_value(value, target_type)


def coerce_single_subclass_value(value: Any, target_type: type[Any]) -> Any:
    if type(value) is target_type:
        return value

    kind = get_simple_subclass_kind(target_type)
    if kind is None:
        raise TypeError(f"Unsupported simple subclass target type: {target_type}")

    match kind:
        case "uuid":
            return target_type(str(value))
        case "datetime":
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
        case "date":
            return target_type(
                value.year,
                value.month,
                value.day,
            )
        case "time":
            return target_type(
                value.hour,
                value.minute,
                value.second,
                value.microsecond,
                tzinfo=value.tzinfo,
                fold=value.fold,
            )
        case "timedelta":
            return target_type(
                days=value.days,
                seconds=value.seconds,
                microseconds=value.microseconds,
            )
        case "bytes" | "str" | "int" | "float" | "bool":
            return target_type(value)
        case unexpected:
            assert_never(unexpected)
