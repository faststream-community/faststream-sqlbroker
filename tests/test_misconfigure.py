import warnings
from typing import Any, cast

import pytest
import pytest_asyncio
from faststream import AckPolicy
from faststream.exceptions import SetupError
from sqlalchemy.ext.asyncio import create_async_engine

from faststream_sqlbroker.sqlbroker import SqlBroker
from faststream_sqlbroker.sqlbroker.retry import ConstantRetryStrategy, NoRetryStrategy
from faststream_sqlbroker.sqlbroker.schema import (
    SqlBrokerSchemaConfig,
    SqlBrokerSchemaVariant,
)


@pytest_asyncio.fixture
async def broker() -> SqlBroker:
    return SqlBroker(engine=create_async_engine("sqlite+aiosqlite:///:memory:"))


@pytest_asyncio.fixture
async def broker_without_archive() -> SqlBroker:
    return SqlBroker(
        engine=create_async_engine("sqlite+aiosqlite:///:memory:"),
        schema=SqlBrokerSchemaConfig(message_archive_table_name=None),
    )


@pytest.mark.asyncio()
async def test_warn_on_max_deliveries(broker: SqlBroker) -> None:
    with pytest.warns(
        UserWarning,
        match="max_deliveries violates the at-most-once processing guarantee",
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            overfetch_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            max_deliveries=3,
        )


@pytest.mark.asyncio()
async def test_subscriber_defaults(broker: SqlBroker) -> None:
    with warnings.catch_warnings(record=True) as caught:
        subscriber = broker.subscriber(
            queues=["test"],
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            flush_interval=0.1,
        )

    assert isinstance(subscriber.config.retry_strategy, NoRetryStrategy)
    assert subscriber.config.max_workers == 1
    assert subscriber.config.ack_policy is AckPolicy.REJECT_ON_ERROR
    assert subscriber.config.overfetch_factor == 1.5
    assert subscriber.config.max_deliveries is None
    assert subscriber.config.release_stuck_interval == 60
    assert subscriber.config.release_stuck_timeout == 60 * 10
    assert caught == []


@pytest.mark.asyncio()
async def test_warn_when_retry_strategy_ignored(broker: SqlBroker) -> None:
    with pytest.warns(
        UserWarning,
        match="retry_strategy is ignored when AckPolicy.REJECT_ON_ERROR is used",
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=1.0,
                max_total_delay_seconds=None,
                max_attempts=3,
            ),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            overfetch_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )


@pytest.mark.asyncio()
async def test_warn_when_nack_without_retry_strategy(broker: SqlBroker) -> None:
    with pytest.warns(
        UserWarning,
        match="AckPolicy.NACK_ON_ERROR has the same effect as AckPolicy.REJECT_ON_ERROR for this broker",
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=None,
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            overfetch_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ("retain_in_archive_on_ack", "retain_in_archive_on_reject"),
    ((True, True), (True, False), (False, True)),
)
async def test_fail_when_archiving_without_archive_table(
    broker_without_archive: SqlBroker,
    retain_in_archive_on_ack: bool,
    retain_in_archive_on_reject: bool,
) -> None:
    with pytest.raises(SetupError, match="require an archive table"):
        broker_without_archive.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            overfetch_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            retain_in_archive_on_ack=retain_in_archive_on_ack,
            retain_in_archive_on_reject=retain_in_archive_on_reject,
        )


@pytest.mark.asyncio()
async def test_no_fail_when_archiving_disabled_without_archive_table(
    broker_without_archive: SqlBroker,
) -> None:
    broker_without_archive.subscriber(
        queues=["test"],
        max_workers=1,
        retry_strategy=NoRetryStrategy(),
        max_fetch_interval=1.0,
        min_fetch_interval=0.1,
        fetch_batch_size=10,
        overfetch_factor=1.0,
        flush_interval=0.1,
        release_stuck_interval=10.0,
        release_stuck_timeout=10.0,
        retain_in_archive_on_ack=False,
        retain_in_archive_on_reject=False,
    )


@pytest.mark.asyncio()
async def test_warn_when_ack_first_used(broker: SqlBroker) -> None:
    with pytest.warns(
        UserWarning,
        match="AckPolicy.ACK_FIRST has the same effect as AckPolicy.ACK for this broker",
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            overfetch_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            ack_policy=AckPolicy.ACK_FIRST,
        )


@pytest.mark.asyncio()
async def test_fail_on_unsupported_schema_version() -> None:
    broker = SqlBroker(
        engine=create_async_engine("sqlite+aiosqlite:///:memory:"),
        schema=SqlBrokerSchemaConfig(
            variant=SqlBrokerSchemaVariant.WORK_QUEUE,
            version=cast("Any", 2),
        ),
    )

    with pytest.raises(SetupError, match="Unsupported SqlBroker schema version"):
        await broker.connect()
