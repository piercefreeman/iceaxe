"""Reproduce https://github.com/piercefreeman/iceaxe/issues/116.

After editing Cython: uv run --with cython --with setuptools python setup.py build_ext --inplace
Run: uv run pytest iceaxe/__tests__/benchmarks/test_uuid_select.py -m integration_tests -s
Reports median warm fetch, decode-only, and full exec times for identical SQL.
Timing is informational; assertions check values, runtime types, and tracking.
"""

import gc
from statistics import median
from time import perf_counter
from typing import Any
from uuid import UUID

import pytest

from iceaxe import Field, TableBase, select
from iceaxe.logging import CONSOLE
from iceaxe.session import DBConnection
from iceaxe.session_optimized import optimize_exec_casting
from iceaxe.typing import is_base_table, is_column, is_function_metadata


class RowID(UUID):
    pass


class OwnerID(UUID):
    pass


class GroupID(UUID):
    pass


class ParentID(UUID):
    pass


class UUIDRow(TableBase, autodetect=False):
    table_name = "uuid_select_benchmark"

    id: RowID = Field(primary_key=True)
    owner_id: OwnerID
    group_id: GroupID
    parent_id: ParentID | None
    score: float
    active: bool
    name: str


class PlainUUIDRow(TableBase, autodetect=False):
    table_name = "uuid_select_benchmark"

    id: UUID = Field(primary_key=True)
    owner_id: UUID
    group_id: UUID
    parent_id: UUID | None
    score: float
    active: bool
    name: str


@pytest.mark.asyncio
@pytest.mark.integration_tests
@pytest.mark.parametrize("selection", ["model", "column", "tuple", "plain_model"])
async def test_uuid_select_benchmark(db_connection: DBConnection, selection: str):
    row_count = 16_000
    repeats = 7
    await db_connection.conn.execute(
        """
        CREATE TEMP TABLE uuid_select_benchmark AS
        SELECT md5(i::text)::uuid AS id,
               md5((i + 1)::text)::uuid AS owner_id,
               md5((i + 2)::text)::uuid AS group_id,
               CASE WHEN i % 2 = 0 THEN NULL
                    ELSE md5((i + 3)::text)::uuid END AS parent_id,
               i::double precision / 10 AS score,
               i % 2 = 0 AS active,
               'Row ' || i AS name
        FROM generate_series(1, $1::int) AS i
        """,
        row_count,
    )
    queries = {
        "model": select(UUIDRow),
        "column": select(UUIDRow.id),
        "tuple": select((UUIDRow.id, UUIDRow.owner_id, UUIDRow.parent_id)),
        "plain_model": select(PlainUUIDRow),
    }
    query = queries[selection].order_by(UUIDRow.id)
    sql, variables = query.build()
    select_types = [
        (is_base_table(raw), is_column(raw), is_function_metadata(raw))
        for raw in query._select_raw
    ]
    values = await db_connection.conn.fetch(sql, *variables)

    def decode():
        return db_connection._cast_column_select_results(
            optimize_exec_casting(values, query._select_raw, select_types),
            query._select_raw,
        )

    def check_results(results: list[Any]):
        assert len(results) == row_count
        prefix = "uuid_select_benchmark_"
        for raw, result in zip(values, results, strict=True):
            if selection in ("model", "plain_model"):
                assert result.model_dump(
                    exclude={"modified_attrs", "modified_attrs_callbacks"}
                ) == {name.removeprefix(prefix): value for name, value in raw.items()}
                expected_types = (
                    (RowID, OwnerID, GroupID, ParentID)
                    if selection == "model"
                    else (UUID,) * 4
                )
                for name, expected_type in zip(
                    ("id", "owner_id", "group_id", "parent_id"), expected_types
                ):
                    value = getattr(result, name)
                    if value is not None:
                        assert isinstance(value, expected_type)
                assert result.get_modified_attributes() == {}
            elif selection == "column":
                assert type(result) is RowID
                assert result == raw[prefix + "id"]
            else:
                assert result == tuple(raw.values())
                assert type(result[0]) is RowID
                assert type(result[1]) is OwnerID
                assert result[2] is None or type(result[2]) is ParentID

    check_results(decode())
    check_results(await db_connection.exec(query))
    timings: dict[str, list[float]] = {"fetch": [], "decode": [], "exec": []}
    for _ in range(repeats):
        for operation in timings:
            gc.collect()
            start = perf_counter()
            if operation == "fetch":
                results = await db_connection.conn.fetch(sql, *variables)
            elif operation == "decode":
                results = decode()
            else:
                results = await db_connection.exec(query)
            timings[operation].append((perf_counter() - start) * 1000)
            assert len(results) == row_count
            del results

    medians = {operation: median(samples) for operation, samples in timings.items()}
    CONSOLE.print(
        f"{selection}: {row_count:,} rows, median of {repeats}: "
        f"fetch={medians['fetch']:.2f} ms, decode={medians['decode']:.2f} ms, "
        f"exec={medians['exec']:.2f} ms ({medians['exec'] / medians['fetch']:.2f}x raw)"
    )
