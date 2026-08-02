import warnings
from typing import TYPE_CHECKING, Any, cast

import pytest
import pytest_asyncio
from faststream import AckPolicy
from faststream.exceptions import SetupError
from sqlalchemy.ext.asyncio import create_async_engine

from faststream_sqlbroker.sqlbroker import SqlBroker
from faststream_sqlbroker.sqlbroker.broker.router import SqlBrokerRoute, SqlBrokerRouter
from faststream_sqlbroker.sqlbroker.retry import ConstantRetryStrategy, NoRetryStrategy
from faststream_sqlbroker.sqlbroker.schema import (
    SqlBrokerSchemaConfig,
    SqlBrokerSchemaVariant,
)

if TYPE_CHECKING:
    from faststream_sqlbroker.sqlbroker.subscriber.usecase import SqlBrokerSubscriber


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
async def test_subscriber_defaults(broker: SqlBroker) -> None:
    with warnings.catch_warnings(record=True) as caught:
        subscriber = broker.subscriber(
            queues=["test"],
            max_fetch_interval=1.0,
            fetch_batch_size=10,
            flush_interval=0.1,
        )

    assert isinstance(subscriber.config.retry_strategy, NoRetryStrategy)
    assert subscriber.config.max_workers == 1
    assert subscriber.config.ack_policy is AckPolicy.REJECT_ON_ERROR
    assert subscriber.config.max_not_processed_factor == 1.5
    assert subscriber.config.max_not_persisted_factor == 2.0
    assert subscriber.config.min_fetch_interval == 1.0
    assert subscriber.config.max_deliveries is None
    assert subscriber.config.release_stuck_interval == 60
    assert subscriber.config.release_stuck_timeout == 60 * 10
    assert caught == []


@pytest.mark.asyncio()
async def test_batch_subscriber_defaults(
    broker: SqlBroker,
) -> None:
    subscriber = broker.subscriber(
        queues=["test"],
        max_fetch_interval=1.0,
        fetch_batch_size=10,
        flush_interval=0.1,
        batch=True,
    )

    assert subscriber._batch_max_records == 10  # type: ignore[attr-defined]


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
            max_not_processed_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            max_deliveries=3,
        )


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
            max_not_processed_factor=1.0,
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
            max_not_processed_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "kwargs",
    (
        {"batch_max_records": 5},
        {"batch_max_accumulation_timeout_factor": 1.0},
        {
            "batch_max_records": 5,
            "batch_max_accumulation_timeout_factor": 1.0,
        },
    ),
)
async def test_warn_when_batch_params_with_batch_false(
    broker: SqlBroker,
    kwargs: dict[str, Any],
) -> None:
    with pytest.warns(
        UserWarning,
        match=(
            "batch_max_records and batch_max_accumulation_timeout_factor "
            "are ignored when batch=False"
        ),
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            max_not_processed_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            batch=False,
            **kwargs,
        )


@pytest.mark.asyncio()
async def test_fail_when_batch_with_max_workers_gt_1(broker: SqlBroker) -> None:
    with pytest.raises(SetupError, match="batch=True requires max_workers=1"):
        broker.subscriber(
            queues=["test"],
            max_workers=2,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            max_not_processed_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            batch=True,
        )


@pytest.mark.asyncio()
async def test_fail_when_batch_max_records_gt_fetch_batch_size_times_max_not_processed_factor(
    broker: SqlBroker,
) -> None:
    with pytest.raises(
        SetupError,
        match=(
            "batch_max_records must be less than or equal to "
            r"fetch_batch_size \* max_not_processed_factor"
        ),
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            max_not_processed_factor=0.5,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            batch=True,
            batch_max_records=10,
        )


@pytest.mark.asyncio()
async def test_fail_when_batch_max_records_gt_fetch_batch_size_times_max_not_persisted_factor(
    broker: SqlBroker,
) -> None:
    with pytest.raises(
        SetupError,
        match=(
            "batch_max_records must be less than or equal to "
            r"fetch_batch_size \* max_not_persisted_factor"
        ),
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            max_not_processed_factor=2.0,
            max_not_persisted_factor=0.5,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            batch=True,
            batch_max_records=10,
        )


@pytest.mark.asyncio()
async def test_warn_when_max_not_persisted_factor_lt_max_not_processed_factor(
    broker: SqlBroker,
) -> None:
    with pytest.warns(
        UserWarning,
        match="max_not_persisted_factor is less than max_not_processed_factor",
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            max_not_processed_factor=2.0,
            max_not_persisted_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
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
            max_not_processed_factor=1.0,
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
        max_not_processed_factor=1.0,
        flush_interval=0.1,
        release_stuck_interval=10.0,
        release_stuck_timeout=10.0,
        retain_in_archive_on_ack=False,
        retain_in_archive_on_reject=False,
    )


@pytest.mark.asyncio()
async def test_route_can_disable_archive_without_archive_table(
    broker_without_archive: SqlBroker,
) -> None:
    async def handler() -> None:
        pass

    router = SqlBrokerRouter(
        handlers=(
            SqlBrokerRoute(
                handler,
                queues=["test"],
                max_fetch_interval=1.0,
                fetch_batch_size=10,
                flush_interval=0.1,
                retain_in_archive_on_ack=False,
                retain_in_archive_on_reject=False,
            ),
        ),
    )
    broker_without_archive.include_router(router)

    assert router.config.message_archive_table_name is None
    assert len(router.subscribers) == 1
    subscriber = cast("SqlBrokerSubscriber", router.subscribers[0])
    assert subscriber.config.retain_in_archive_on_ack is False
    assert subscriber.config.retain_in_archive_on_reject is False


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
            max_not_processed_factor=1.0,
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
            variant=SqlBrokerSchemaVariant.COMPETING_CONSUMERS,
            version=cast("Any", 2),
        ),
    )

    with pytest.raises(SetupError, match="Unsupported SqlBroker schema version"):
        await broker.connect()
