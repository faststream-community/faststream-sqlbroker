import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream_sqlbroker.sqlbroker.broker.broker import SqlBroker
from faststream_sqlbroker.sqlbroker.message import SqlBrokerMessageState
from faststream_sqlbroker.sqlbroker.observability import (
    ArchivedMessageStateSummary,
    MessageStateSummary,
    SqlBrokerStateMetrics,
    SqlBrokerStateMetricsConfig,
    SqlBrokerStateSampler,
    SqlBrokerStateSnapshot,
)
from faststream_sqlbroker.sqlbroker.schema import (
    SqlBrokerSchemaConfig,
    SqlBrokerSchemaType,
    define_sqlbroker_schema,
)


def _sample(registry: CollectorRegistry, name: str, labels: dict[str, str]) -> float:
    value = registry.get_sample_value(name, labels)
    assert value is not None
    return value


async def _wait_for_sample(
    registry: CollectorRegistry,
    name: str,
    labels: dict[str, str],
    expected: float,
) -> None:
    for _ in range(200):
        if registry.get_sample_value(name, labels) == expected:
            return
        await asyncio.sleep(0.01)
    assert registry.get_sample_value(name, labels) == expected


@pytest.mark.asyncio()
async def test_snapshot_and_sampler_populate_persisted_state(
    engine: AsyncEngine, recreate_tables: None
) -> None:
    schema_config = SqlBrokerSchemaConfig()
    schema = define_sqlbroker_schema(config=schema_config)
    table = schema.tables[SqlBrokerSchemaType.MESSAGE]
    archive_table = schema.tables[SqlBrokerSchemaType.MESSAGE_ARCHIVE]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    created_at = now - timedelta(seconds=60)
    next_attempt_at = now - timedelta(seconds=10)
    async with engine.begin() as connection:
        await connection.execute(
            insert(table),
            [
                {
                    "queue": "orders",
                    "payload": b"1",
                    "state": SqlBrokerMessageState.PENDING,
                    "created_at": created_at,
                    "next_attempt_at": next_attempt_at,
                    "attempts_count": 0,
                    "deliveries_count": 0,
                },
                {
                    "queue": "orders",
                    "payload": b"2",
                    "state": SqlBrokerMessageState.PROCESSING,
                    "created_at": created_at,
                    "next_attempt_at": next_attempt_at,
                    "attempts_count": 0,
                    "deliveries_count": 1,
                },
            ],
        )
        await connection.execute(
            insert(archive_table),
            {
                "queue": "orders",
                "payload": b"3",
                "state": SqlBrokerMessageState.COMPLETED,
                "created_at": created_at,
                "attempts_count": 1,
                "deliveries_count": 1,
                "archived_at": now,
            },
        )

    registry = CollectorRegistry()
    sampler = SqlBrokerStateSampler(
        engine=engine,
        schema=schema_config,
        config=SqlBrokerStateMetricsConfig(registry=registry),
    )
    await sampler.collect()

    assert (
        _sample(registry, "sqlbroker_messages", {"queue": "orders", "state": "pending"})
        == 1
    )
    assert (
        _sample(
            registry, "sqlbroker_messages", {"queue": "orders", "state": "processing"}
        )
        == 1
    )
    assert (
        _sample(
            registry,
            "sqlbroker_archived_messages",
            {"queue": "orders", "state": "completed"},
        )
        == 1
    )
    age = _sample(
        registry,
        "sqlbroker_most_overdue_message_age_seconds",
        {"queue": "orders", "state": "pending"},
    )
    assert 9 <= age < 20
    assert _sample(
        registry, "sqlbroker_state_collection_last_success_timestamp_seconds", {}
    )


@pytest.mark.asyncio()
async def test_running_consuming_broker_updates_state_metrics(
    engine: AsyncEngine, recreate_tables: None
) -> None:
    registry = CollectorRegistry()
    broker = SqlBroker(
        engine=engine,
        state_metrics_config=SqlBrokerStateMetricsConfig(
            registry=registry, interval=0.01
        ),
    )
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()

    @broker.subscriber(
        queues=["orders"],
        max_workers=1,
        max_fetch_interval=0.01,
        min_fetch_interval=0.01,
        fetch_batch_size=1,
        flush_interval=0.01,
    )
    async def handler() -> None:
        handler_started.set()
        await release_handler.wait()

    await broker.connect()
    await broker.publish({"order_id": 1}, queue="orders")
    await broker.start()
    try:
        await asyncio.wait_for(handler_started.wait(), timeout=2)
        await _wait_for_sample(
            registry,
            "sqlbroker_messages",
            {"queue": "orders", "state": "processing"},
            1,
        )

        release_handler.set()
        await _wait_for_sample(
            registry,
            "sqlbroker_archived_messages",
            {"queue": "orders", "state": "completed"},
            1,
        )
    finally:
        await broker.stop()


def test_metrics_remove_stale_state_series() -> None:
    registry = CollectorRegistry()
    metrics = SqlBrokerStateMetrics(registry=registry)

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    metrics.apply(
        SqlBrokerStateSnapshot(
            collected_at=now,
            messages=(
                MessageStateSummary(
                    queue="orders",
                    state=SqlBrokerMessageState.PENDING,
                    message_count=1,
                    oldest_next_attempt_at=now,
                ),
            ),
            archived_messages=(
                ArchivedMessageStateSummary(
                    queue="orders",
                    state=SqlBrokerMessageState.COMPLETED,
                    message_count=1,
                ),
            ),
        )
    )
    metrics.apply(SqlBrokerStateSnapshot(collected_at=now, messages=()))

    assert (
        registry.get_sample_value(
            "sqlbroker_messages", {"queue": "orders", "state": "pending"}
        )
        is None
    )
    assert (
        registry.get_sample_value(
            "sqlbroker_archived_messages", {"queue": "orders", "state": "completed"}
        )
        is None
    )
