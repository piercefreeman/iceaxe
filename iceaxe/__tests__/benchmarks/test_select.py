from enum import Enum
from time import monotonic_ns
from typing import Any

import asyncpg
import pytest

from iceaxe.__tests__.conf_models import UserDemo, run_profile
from iceaxe.logging import CONSOLE, LOGGER
from iceaxe.queries import QueryBuilder
from iceaxe.session import DBConnection


class FetchType(Enum):
    ID = "id"
    OBJ = "obj"


async def insert_users(conn: asyncpg.Connection, num_users: int):
    users = [(f"User {i}", f"user{i}@example.com") for i in range(num_users)]
    await conn.executemany("INSERT INTO userdemo (name, email) VALUES ($1, $2)", users)


async def fetch_users_raw(conn: asyncpg.Connection, fetch_type: FetchType) -> list[Any]:
    if fetch_type == FetchType.OBJ:
        return await conn.fetch("SELECT * FROM userdemo")  # type: ignore
    elif fetch_type == FetchType.ID:
        return await conn.fetch("SELECT id FROM userdemo")  # type: ignore
    else:
        raise ValueError(f"Invalid run profile: {fetch_type}")


def build_iceaxe_query(fetch_type: FetchType):
    if fetch_type == FetchType.OBJ:
        return QueryBuilder().select(UserDemo)
    elif fetch_type == FetchType.ID:
        return QueryBuilder().select(UserDemo.id)
    else:
        raise ValueError(f"Invalid run profile: {fetch_type}")


@pytest.mark.asyncio
@pytest.mark.integration_tests
@pytest.mark.parametrize(
    "fetch_type, allowed_overhead",
    [
        (FetchType.ID, 10),
        (FetchType.OBJ, 800),
    ],
)
async def test_benchmark(
    db_connection: DBConnection, request, fetch_type: FetchType, allowed_overhead: float
):
    num_users = 500_000
    num_loops = 100

    # Insert users using raw asyncpg
    await insert_users(db_connection.conn, num_users)

    # Benchmark raw asyncpg query
    start_time = monotonic_ns()
    raw_results: list[Any] = []
    for _ in range(num_loops):
        raw_results = await fetch_users_raw(db_connection.conn, fetch_type)
    raw_time = monotonic_ns() - start_time
    raw_time_seconds = raw_time / 1e9
    raw_time_per_query = (raw_time / num_loops) / 1e9

    LOGGER.info(
        f"Raw asyncpg query time: {raw_time_per_query:.4f} (total: {raw_time_seconds:.4f}) seconds"
    )
    CONSOLE.print(
        f"Raw asyncpg query time: {raw_time_per_query:.4f} (total: {raw_time_seconds:.4f}) seconds"
    )

    # Benchmark DBConnection.exec query
    start_time = monotonic_ns()
    query = build_iceaxe_query(fetch_type)
    db_results: list[UserDemo] | list[int] = []
    for _ in range(num_loops):
        db_results = await db_connection.exec(query)
    db_time = monotonic_ns() - start_time
    db_time_seconds = db_time / 1e9
    db_time_per_query = (db_time / num_loops) / 1e9

    LOGGER.info(
        f"DBConnection.exec query time: {db_time_per_query:.4f} (total: {db_time_seconds:.4f}) seconds"
    )
    CONSOLE.print(
        f"DBConnection.exec query time: {db_time_per_query:.4f} (total: {db_time_seconds:.4f}) seconds"
    )

    # Slower than the raw run since we need to run the performance instrumentation
    if False:
        with run_profile(request):
            # Right now we don't cache results so we can run multiple times to get a better measure of samples
            for _ in range(num_loops):
                query = build_iceaxe_query(fetch_type)
                db_results = await db_connection.exec(query)

    # Compare results
    assert len(raw_results) == len(db_results) == num_users, "Result count mismatch"

    # Calculate performance difference
    performance_diff = (db_time - raw_time) / raw_time * 100
    LOGGER.info(f"Performance difference: {performance_diff:.2f}%")
    CONSOLE.print(f"Performance difference: {performance_diff:.2f}%")

    # Assert that DBConnection.exec is at most X% slower than raw query
    assert performance_diff <= allowed_overhead, (
        f"DBConnection.exec is {performance_diff:.2f}% slower than raw query, which exceeds the {allowed_overhead}% threshold"
    )

    LOGGER.info("Benchmark completed successfully.")
