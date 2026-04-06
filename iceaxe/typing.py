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
    Callable,
    Type,
    TypeGuard,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from uuid import UUID

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

T = TypeVar("T")


#
# Simple type utility function
#


def is_union_type(annotation: Any) -> bool:
    origin = get_origin(annotation)
    return origin is Union or isinstance(annotation, types.UnionType)


def rebuild_typehint(annotation: Any, args: tuple[Any, ...]):
    if is_union_type(annotation):
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


def get_optional_inner(annotation: Any) -> Any | None:
    if not is_union_type(annotation):
        return None

    non_null_args = tuple(
        arg for arg in get_args(annotation) if unwrap_annotated(arg) is not type(None)
    )
    if len(non_null_args) != 1:
        return None

    return non_null_args[0]


#
# Type introspection
#


@dataclass(frozen=True)
class ResolvedTypehint:
    runtime_type: Any
    is_list: bool


def resolve_typehint(annotation: Any) -> ResolvedTypehint:
    """
    Normalize a field annotation into the subset of typing structure Iceaxe
    needs for runtime coercion and schema inference.

    Python annotations can describe the same logical field in several wrapped
    forms:
    - `Annotated[T, ...]` carries metadata but doesn't change the core type.
    - `T | None` / `Optional[T]` is represented as a union and needs to be
      unwrapped to reach the concrete value type.
    - `list[T]` means the ORM should treat the column as an array while still
      reasoning about the element type `T`.

    Callers that need to infer database/storage behavior should not each have to
    reimplement `get_origin()` / `get_args()` handling or care about the exact
    runtime shape Python uses for unions and annotated metadata. This helper
    resolves those wrappers into a canonical form:
    - `runtime_type`: the innermost non-`Annotated`, non-nullable element type
    - `is_list`: whether the annotation represents a top-level `list[...]`

    The resolver is intentionally narrow. It understands the container/wrapper
    shapes Iceaxe needs structurally, but it does not try to semantically reduce
    arbitrary generic types. For example, nested generics are preserved inside
    `runtime_type` once the top-level list/optional wrappers have been handled.

    """
    current = annotation
    is_list = False

    while True:
        current = unwrap_annotated(current)

        optional_inner = get_optional_inner(current)
        if optional_inner is not None:
            current = optional_inner
            continue

        if not is_list and get_origin(current) is list:
            (current,) = get_args(current)
            is_list = True
            continue

        break

    return ResolvedTypehint(
        runtime_type=unwrap_annotated(current),
        is_list=is_list,
    )


def transform_typehint(
    annotation: Any,
    transform: Callable[[Any], Any],
) -> Any:
    """
    Recursively rebuild an annotation tree while applying a callback to each node.

    Python type hints are often nested combinations of wrappers such as
    `Annotated[...]`, unions, and container generics. Callers sometimes need to
    inject or rewrite metadata inside that structure without losing the overall
    typing shape. This helper performs that traversal once and hands each rebuilt
    node to `transform`, allowing feature-specific code to focus on "what should
    this node become?" rather than repeatedly reimplementing `get_origin()` /
    `get_args()` recursion.

    Some examples of the supported traversal behavior:
    - `CustomUUID | None` visits `CustomUUID`, applies the transform there, and
      then rebuilds the nullable union around the transformed result.
    - `list[CustomUUID]` visits `CustomUUID`, applies the transform there, and
      then rebuilds the outer list as `list[<transformed CustomUUID>]`.
    - `dict[str, CustomUUID]` preserves the `dict[str, ...]` shape while still
      transforming the nested value type.
    - `Annotated[list[CustomUUID], Meta()]` first transforms the inner
      `list[CustomUUID]`, then rebuilds the `Annotated[...]` wrapper with the
      original metadata still attached.

    In all of these cases, child nodes are transformed before their parent
    wrapper is rebuilt. That lets callers inspect the already-normalized inner
    annotation when they receive a parent node such as `Annotated[...]`.

    """
    origin = get_origin(annotation)

    if origin is Annotated:
        inner, *metadata = get_args(annotation)
        return transform(Annotated[transform_typehint(inner, transform), *metadata])

    if origin is not None:
        args = tuple(transform_typehint(arg, transform) for arg in get_args(annotation))
        return transform(rebuild_typehint(annotation, args))

    return transform(annotation)


#
# Typeguards
#


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
