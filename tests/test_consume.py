import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from faststream import AckPolicy
from faststream._internal.context import ContextRepo
from faststream.annotations import (
    ContextRepo as ContextRepoAnnotation,
    Logger as LoggerAnnotation,
)
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream_sqlbroker.sqlbroker.annotations import (
    SqlBroker as SqlBrokerAnnotation,
    SqlBrokerBatchMessage as SqlBrokerBatchMessageAnnotation,
    SqlBrokerMessage as SqlBrokerMessageAnnotation,
)
from faststream_sqlbroker.sqlbroker.broker.broker import SqlBroker
from faststream_sqlbroker.sqlbroker.message import (
    SqlBrokerBatchMessage,
    SqlBrokerMessage,
    SqlBrokerMessageState,
)
from faststream_sqlbroker.sqlbroker.retry import ConstantRetryStrategy, NoRetryStrategy
from tests.basic import SqlBrokerTestcaseConfig
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.helpers import as_datetime


@pytest.mark.connected()
@pytest.mark.slow()
class TestConsume(SqlBrokerTestcaseConfig, BrokerRealConsumeTestcase):
    async def test_get_one_conflicts_with_handler(self) -> None: ...

    async def test_get_one(self) -> None: ...

    async def test_get_one_timeout(self) -> None: ...

    async def test_iteration(self) -> None: ...

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("retain_in_archive_on_ack", (True, False))
    async def test_consume(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
        retain_in_archive_on_ack: bool,
    ) -> None:
        """Message was processed and archived."""
        attempted = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
            retain_in_archive_on_ack=retain_in_archive_on_ack,
        )
        async def handler(msg: Any) -> None:
            nonlocal attempted
            attempted.append(msg)

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        assert len(attempted) == 1

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        if retain_in_archive_on_ack:
            result = result.mappings().one()
            assert result["queue"] == "default1"
            assert json.loads(result["payload"]) == {"message": "hello1"}
            assert result["state"] == SqlBrokerMessageState.COMPLETED.name
            assert result["attempts_count"] == 1
            assert result["deliveries_count"] == 1
            assert as_datetime(result["created_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["first_attempt_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["first_attempt_at"]) > as_datetime(
                result["created_at"]
            )
            assert as_datetime(result["last_attempt_at"]) == as_datetime(
                result["first_attempt_at"]
            )
            assert as_datetime(result["archived_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["archived_at"]) > as_datetime(
                result["first_attempt_at"]
            )
        else:
            assert len(result.all()) == 0

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_nack_retry(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """On exception message was marked as retryable with next attempts scheduled."""

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=10, max_total_delay_seconds=None, max_attempts=None
            ),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            return 1 / 0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        result = result.mappings().one()
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == SqlBrokerMessageState.RETRYABLE.name
        assert result["attempts_count"] == 1
        assert result["deliveries_count"] == 1
        assert as_datetime(result["created_at"]) < datetime.now(tz=timezone.utc).replace(
            tzinfo=None
        ).replace(tzinfo=None)
        assert as_datetime(result["first_attempt_at"]) < datetime.now(
            tz=timezone.utc
        ).replace(tzinfo=None).replace(tzinfo=None)
        assert as_datetime(result["first_attempt_at"]) > as_datetime(result["created_at"])
        assert as_datetime(result["last_attempt_at"]) == as_datetime(
            result["first_attempt_at"]
        )

        assert as_datetime(result["next_attempt_at"]) >= as_datetime(
            result["first_attempt_at"]
        ) + timedelta(seconds=10)
        assert result["acquired_at"] is None

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("retain_in_archive_on_reject", (True, False))
    async def test_consume_nack_no_retry(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
        retain_in_archive_on_reject: bool,
    ) -> None:
        """On exception message was marked as failed and was archived."""

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
            retain_in_archive_on_reject=retain_in_archive_on_reject,
        )
        async def handler(msg: Any) -> None:
            return 1 / 0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        if retain_in_archive_on_reject:
            result = result.mappings().one()
            assert result["queue"] == "default1"
            assert json.loads(result["payload"]) == {"message": "hello1"}
            assert result["state"] == SqlBrokerMessageState.FAILED.name
            assert result["attempts_count"] == 1
            assert result["deliveries_count"] == 1
            assert as_datetime(result["created_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["first_attempt_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["first_attempt_at"]) > as_datetime(
                result["created_at"]
            )
            assert as_datetime(result["last_attempt_at"]) == as_datetime(
                result["first_attempt_at"]
            )

            assert as_datetime(result["archived_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["archived_at"]) > as_datetime(
                result["first_attempt_at"]
            )
        else:
            assert len(result.all()) == 0

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("max_deliveries", (1, None))
    @pytest.mark.parametrize("retain_in_archive_on_reject", (True, False))
    @pytest.mark.parametrize("batch", (False, True))
    async def test_consume_max_deliveries(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        max_deliveries: int | None,
        retain_in_archive_on_reject: bool,
        batch: bool,
    ) -> None:
        """Message that was attempted but got stuck was not allowed a retry due to
        reached delivery limit.
        """
        logger = MagicMock()
        async with self.get_broker(
            engine=engine, logger=logger, graceful_timeout=0.1
        ) as broker:
            attempted = []

            @broker.subscriber(
                queues=["default1"],
                max_workers=1,
                retry_strategy=ConstantRetryStrategy(
                    delay_seconds=0, max_total_delay_seconds=None, max_attempts=None
                ),
                max_fetch_interval=0.1,
                min_fetch_interval=0.1,
                fetch_batch_size=5,
                max_not_processed_factor=1,
                flush_interval=0.1,
                release_stuck_interval=10,
                release_stuck_timeout=1,
                max_deliveries=max_deliveries,
                ack_policy=AckPolicy.NACK_ON_ERROR,
                retain_in_archive_on_reject=retain_in_archive_on_reject,
                batch=batch,
                batch_max_records=1 if batch else None,
                batch_max_accumulation_timeout_factor=0,
            )
            async def handler(msg: Any) -> None:
                nonlocal attempted
                if batch:
                    attempted.extend(msg)
                else:
                    attempted.append(msg)
                await asyncio.sleep(1)

            await broker.publish({"message": "hello1"}, queue="default1")
            await broker.start()
            await asyncio.sleep(0.5)
            # stop with short graceful_shutdown_timeout so that message becomes stuck
            await broker.stop()
            assert len(attempted) == 1
            await asyncio.sleep(0.5)

            async with engine.begin() as conn:
                result = await conn.execute(text("SELECT * FROM message;"))

            result = result.mappings().one()
            assert result["state"] == SqlBrokerMessageState.PROCESSING.name
            assert result["attempts_count"] == 0
            assert result["deliveries_count"] == 1

            await broker.start()
            await asyncio.sleep(0.5)

            if max_deliveries:
                assert len(attempted) == 1

                async with engine.begin() as conn:
                    result = await conn.execute(text("SELECT * FROM message_archive;"))

                if retain_in_archive_on_reject:
                    result = result.mappings().one()
                    assert result["queue"] == "default1"
                    assert json.loads(result["payload"]) == {"message": "hello1"}
                    assert result["state"] == SqlBrokerMessageState.FAILED.name
                    assert result["attempts_count"] == 0
                    assert result["deliveries_count"] == 2
                    assert as_datetime(result["created_at"]) < datetime.now(
                        tz=timezone.utc
                    ).replace(tzinfo=None)
                    assert result["first_attempt_at"] is None
                    assert result["last_attempt_at"] is None

                    assert as_datetime(result["archived_at"]) < datetime.now(
                        tz=timezone.utc
                    ).replace(tzinfo=None)
                else:
                    assert len(result.all()) == 0

                logs = [x for x in logger.log.call_args_list if x[0][0] == logging.ERROR]
                assert len(logs) == 2
                assert "Message delivery limit was exceeded for message" in logs[-1][0][1]

                async with engine.begin() as conn:
                    result = await conn.execute(text("SELECT * FROM message;"))
                assert len(result.all()) == 0
            else:
                assert len(attempted) == 2

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("retain_in_archive_on_reject", (True, False))
    async def test_consume_full_retry_flow(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
        retain_in_archive_on_reject: bool,
    ) -> None:
        attempted = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=0.01, max_total_delay_seconds=None, max_attempts=3
            ),
            max_fetch_interval=0.01,
            min_fetch_interval=0.01,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
            retain_in_archive_on_reject=retain_in_archive_on_reject,
        )
        async def handler(msg: Any) -> None:
            nonlocal attempted
            attempted.append(msg)
            return 1 / 0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        assert len(attempted) == 3

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        if retain_in_archive_on_reject:
            result = result.mappings().one()
            assert result["queue"] == "default1"
            assert json.loads(result["payload"]) == {"message": "hello1"}
            assert result["state"] == SqlBrokerMessageState.FAILED.name
            assert result["attempts_count"] == 3
            assert result["deliveries_count"] == 3
            assert as_datetime(result["created_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["first_attempt_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["first_attempt_at"]) > as_datetime(
                result["created_at"]
            )
            assert as_datetime(result["last_attempt_at"]) > as_datetime(
                result["first_attempt_at"]
            )
            assert as_datetime(result["last_attempt_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
        else:
            assert len(result.all()) == 0

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_no_retry_strategy(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """On exception message was marked as failed and was archived."""

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=None,
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            return 1 / 0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().one()
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == SqlBrokerMessageState.FAILED.name
        assert result["attempts_count"] == 1
        assert result["deliveries_count"] == 1
        assert as_datetime(result["created_at"]) < datetime.now(tz=timezone.utc).replace(
            tzinfo=None
        )
        assert as_datetime(result["first_attempt_at"]) < datetime.now(
            tz=timezone.utc
        ).replace(tzinfo=None)
        assert as_datetime(result["first_attempt_at"]) > as_datetime(result["created_at"])
        assert as_datetime(result["last_attempt_at"]) == as_datetime(
            result["first_attempt_at"]
        )

        assert as_datetime(result["archived_at"]) < datetime.now(tz=timezone.utc).replace(
            tzinfo=None
        )
        assert as_datetime(result["archived_at"]) > as_datetime(
            result["first_attempt_at"]
        )

    @pytest.mark.asyncio()
    async def test_consume_by_queues(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """Messages from the specified queues were consumed."""
        messages = []

        @broker.subscriber(
            queues=["default1", "default2"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            nonlocal messages
            messages.append(msg["message"])
            if msg["message"] == "hello2":
                event.set()

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello3"}, queue="default3")
        await broker.publish({"message": "hello2"}, queue="default2")
        await broker.start()

        await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert messages == ["hello1", "hello2"]

    @pytest.mark.asyncio()
    async def test_consume_by_next_attempt_at(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        messages = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=0.01,
            min_fetch_interval=0.01,
            fetch_batch_size=1,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            nonlocal messages
            messages.append(msg["message"])

        await broker.publish(
            {"message": "hello1"},
            queue="default1",
            next_attempt_at=datetime.now(tz=timezone.utc) - timedelta(seconds=10),
        )
        await broker.publish(
            {"message": "hello2"},
            queue="default1",
            next_attempt_at=datetime.now(tz=timezone.utc) + timedelta(seconds=10),
        )
        await broker.publish(
            {"message": "hello3"},
            queue="default1",
            next_attempt_at=datetime.now(tz=timezone.utc) - timedelta(seconds=20),
        )
        await broker.start()

        await asyncio.sleep(0.5)

        assert messages == ["hello3", "hello1"]

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("retain_in_archive_on_ack", (True, False))
    @pytest.mark.parametrize("retain_in_archive_on_reject", (True, False))
    async def test_consume_requeue_on_stop(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
        retain_in_archive_on_ack: bool,
        retain_in_archive_on_reject: bool,
    ) -> None:
        """Processing of attempted messages completed and results were flushed.
        Acquired but not attempted messages were requeued.
        """

        @broker.subscriber(
            queues=["default1"],
            max_workers=2,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=4,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
            retain_in_archive_on_ack=retain_in_archive_on_ack,
            retain_in_archive_on_reject=retain_in_archive_on_reject,
        )
        async def handler(msg: Any) -> None:
            event.set()
            await asyncio.sleep(1)

        # attempted
        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")
        # not attempted
        await broker.publish({"message": "hello3"}, queue="default1")
        await broker.publish({"message": "hello4"}, queue="default1")

        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)
        await broker.stop()

        async with engine.begin() as conn:
            result_1 = await conn.execute(text("SELECT * FROM message_archive;"))
            result_2 = await conn.execute(text("SELECT * FROM message;"))

        if retain_in_archive_on_ack:
            result_1 = result_1.mappings().all()
            assert len(result_1) == 2
            assert result_1[0]["state"] == SqlBrokerMessageState.COMPLETED.name
            assert result_1[0]["attempts_count"] == 1
            assert result_1[0]["deliveries_count"] == 1
            assert as_datetime(result_1[0]["created_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result_1[0]["first_attempt_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result_1[0]["first_attempt_at"]) > as_datetime(
                result_1[0]["created_at"]
            )
            assert as_datetime(result_1[0]["last_attempt_at"]) == as_datetime(
                result_1[0]["first_attempt_at"]
            )
            assert as_datetime(result_1[0]["archived_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result_1[0]["archived_at"]) > as_datetime(
                result_1[0]["first_attempt_at"]
            )
        else:
            assert len(result_1.all()) == 0

        result_2 = result_2.mappings().all()
        assert len(result_2) == 2
        assert result_2[0]["state"] == SqlBrokerMessageState.PENDING.name
        assert result_2[0]["attempts_count"] == 0
        assert result_2[0]["deliveries_count"] == 0
        assert as_datetime(result_2[0]["created_at"]) < datetime.now(
            tz=timezone.utc
        ).replace(tzinfo=None)
        assert result_2[0]["acquired_at"] is None
        assert result_2[0]["first_attempt_at"] is None
        assert as_datetime(result_2[0]["next_attempt_at"]) < datetime.now(
            tz=timezone.utc
        ).replace(tzinfo=None)
        assert result_2[0]["last_attempt_at"] is None

    @pytest.mark.asyncio()
    async def test_consume_manual_ack_takes_precedence(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """Manual Ack overrode automatic Reject."""

        @broker.subscriber(
            queues=["default1"],
            max_workers=2,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: SqlBrokerMessageAnnotation, msg_body: dict) -> None:
            await msg.ack()
            return 1 / 0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().all()
        assert len(result) == 2
        assert result[0]["state"] == SqlBrokerMessageState.COMPLETED.name
        assert result[1]["state"] == SqlBrokerMessageState.COMPLETED.name

    @pytest.mark.asyncio()
    async def test_consume_manual_nack_takes_precedence(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """Manual Nack overrode automatic Ack."""

        @broker.subscriber(
            queues=["default1"],
            max_workers=2,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=10, max_total_delay_seconds=None, max_attempts=3
            ),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.ACK,
        )
        async def handler(msg: SqlBrokerMessageAnnotation, msg_body: dict) -> None:
            await msg.nack()

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        result = result.mappings().all()
        assert len(result) == 2
        assert result[0]["state"] == SqlBrokerMessageState.RETRYABLE.name
        assert result[1]["state"] == SqlBrokerMessageState.RETRYABLE.name

        result = result[0]
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == SqlBrokerMessageState.RETRYABLE.name
        assert result["attempts_count"] == 1
        assert result["deliveries_count"] == 1
        assert as_datetime(result["created_at"]) < datetime.now(tz=timezone.utc).replace(
            tzinfo=None
        ).replace(tzinfo=None)
        assert as_datetime(result["first_attempt_at"]) < datetime.now(
            tz=timezone.utc
        ).replace(tzinfo=None).replace(tzinfo=None)
        assert as_datetime(result["first_attempt_at"]) > as_datetime(result["created_at"])
        assert as_datetime(result["last_attempt_at"]) == as_datetime(
            result["first_attempt_at"]
        )

        assert as_datetime(result["next_attempt_at"]) >= as_datetime(
            result["first_attempt_at"]
        ) + timedelta(seconds=10)
        assert result["acquired_at"] is None

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_manual_reject_takes_precedence(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """Manual Reject overrode automatic Ack."""

        @broker.subscriber(
            queues=["default1"],
            max_workers=2,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=0, max_total_delay_seconds=None, max_attempts=3
            ),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: SqlBrokerMessageAnnotation, msg_body: dict) -> None:
            await msg.reject()

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().all()
        assert len(result) == 2
        assert result[0]["state"] == SqlBrokerMessageState.FAILED.name
        assert result[1]["state"] == SqlBrokerMessageState.FAILED.name

    @pytest.mark.asyncio()
    async def test_consume_context_fields(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        body_ = None
        message_ = None
        broker_ = None
        context_ = None
        logger_ = None

        @broker.subscriber(
            queues=["default1"],
            max_workers=2,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(
            body: Any,
            message: SqlBrokerMessageAnnotation,
            broker: SqlBrokerAnnotation,
            context: ContextRepoAnnotation,
            logger: LoggerAnnotation,
        ) -> None:
            nonlocal body_, message_, broker_, context_, logger_
            body_ = body
            message_ = message
            broker_ = broker
            context_ = context
            logger_ = logger
            event.set()

        await broker.publish(
            {"message": "hello1"}, queue="default1", headers={"header_1": "value_1"}
        )
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert body_ == {"message": "hello1"}
        assert isinstance(message_, SqlBrokerMessage)
        assert message_.headers == {
            "content-type": "application/json",
            "header_1": "value_1",
        }
        assert isinstance(broker_, SqlBroker)
        assert isinstance(context_, ContextRepo)
        assert isinstance(logger_, logging.Logger)

    @pytest.mark.asyncio()
    async def test_consume_pydantic_model(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """A handler can be annotated with a Pydantic model."""

        class MyModel(BaseModel):
            message: str

        body_: Any = None

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )
        async def handler(body: MyModel) -> None:
            nonlocal body_
            body_ = body
            event.set()

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)
        await asyncio.sleep(0.3)

        assert body_ == MyModel(message="hello1")

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_per_field_deserialization(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """The body's fields can be spread across the handler's arguments."""
        received: Any = None

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )
        async def handler(name: str, user_id: int) -> None:
            nonlocal received
            received = (name, user_id)
            event.set()

        await broker.publish({"name": "john", "user_id": "1"}, queue="default1")
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)
        await asyncio.sleep(0.3)

        assert received == ("john", 1)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_custom_decoder(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """A custom decoder delegating to the default gets the decoded body."""
        received: Any = None

        async def decoder(
            msg: SqlBrokerMessage,
            original_decoder: Callable[[SqlBrokerMessage], Awaitable[Any]],
        ) -> Any:
            decoded = await original_decoder(msg)
            return {**decoded, "seen": True}

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
            decoder=decoder,
        )
        async def handler(body: dict[str, Any]) -> None:
            nonlocal received
            received = body
            event.set()

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert received == {"message": "hello1", "seen": True}

    @pytest.mark.asyncio()
    async def test_consume_concurrency(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        attempted = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=4,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=0,
            fetch_batch_size=4,
            max_not_processed_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            await asyncio.sleep(1)
            nonlocal attempted
            attempted.append(msg)

        for idx in range(8):
            await broker.publish({"message": f"hello{idx + 1}"}, queue="default1")
        await broker.start()

        await asyncio.sleep(1.5)
        assert len(attempted) == 4

        async with engine.begin() as conn:
            result = await conn.execute(
                text("SELECT * FROM message_archive WHERE state = 'COMPLETED';")
            )
        result = result.mappings().all()
        assert len(result) == 4

        await asyncio.sleep(1)
        assert len(attempted) == 8

        async with engine.begin() as conn:
            result = await conn.execute(
                text("SELECT * FROM message_archive WHERE state = 'COMPLETED';")
            )
        result = result.mappings().all()
        assert len(result) == 8

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("batch", (False, True))
    async def test_consume_fetch_intervals_fetch_on_freed_capacity(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        broker: SqlBroker,
        batch: bool,
    ) -> None:
        """After first batch was fully processed, next fetch
        happened immediately.
        """
        attempted = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=1 if batch else 4,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=0,
            fetch_batch_size=4,
            max_not_processed_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
            batch=batch,
            batch_max_records=4 if batch else None,
            batch_max_accumulation_timeout_factor=0,
        )
        async def handler(msg: Any) -> None:
            nonlocal attempted
            if batch:
                attempted.extend(msg)
            else:
                attempted.append(msg)
            await asyncio.sleep(1)

        for _ in range(7):
            await broker.publish({"message": "hello"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)
        assert len(attempted) == 4

        await asyncio.sleep(1)
        assert len(attempted) == 7

    @pytest.mark.asyncio()
    async def test_consume_fetch_intervals_immediate_fetch_to_fill_capacity(
        self, engine: AsyncEngine, recreate_tables: None, broker: SqlBroker
    ) -> None:
        """After first fetch, next fetch happened immediately to
        fill up capacity, because of the overfetch factor.
        """
        attempted = []
        client = broker.config.broker_config.client
        client.fetch = AsyncMock(wraps=client.fetch)

        @broker.subscriber(
            queues=["default1"],
            max_workers=4,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=0,
            fetch_batch_size=4,
            max_not_processed_factor=2,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            nonlocal attempted
            attempted.append(msg)
            await asyncio.sleep(4)

        for _ in range(7):
            await broker.publish({"message": "hello"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        assert len(attempted) == 4
        assert client.fetch.await_count == 2

    @pytest.mark.asyncio()
    async def test_consume_fetch_intervals_nonfull_fetch(
        self, engine: AsyncEngine, recreate_tables: None, broker: SqlBroker
    ) -> None:
        """Because the first fetch wasn't full, next fetch happened after
        max_fetch_interval despite batch being exhausted.
        """
        attempted = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=4,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=0,
            fetch_batch_size=4,
            max_not_processed_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            nonlocal attempted
            attempted.append(msg)

        for _ in range(3):
            await broker.publish({"message": "hello"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)
        await broker.publish({"message": "hello"}, queue="default1")

        await asyncio.sleep(0.5)

        assert len(attempted) == 3

    @pytest.mark.asyncio()
    async def test_consume_max_not_persisted_factor(
        self, engine: AsyncEngine, recreate_tables: None, broker: SqlBroker
    ) -> None:
        """`max_not_persisted_factor` was saturated after 4 fetches."""
        client = broker.config.broker_config.client
        client.fetch = AsyncMock(wraps=client.fetch)
        fetched = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=4,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=0,
            fetch_batch_size=4,
            max_not_processed_factor=2,
            max_not_persisted_factor=4,
            flush_interval=5,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            nonlocal fetched
            await asyncio.sleep(0)
            fetched.append(msg)

        for _ in range(20):
            await broker.publish({"message": "hello"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        assert len(fetched) == 16
        assert client.fetch.await_count == 4

    @pytest.mark.asyncio()
    async def test_consume_max_not_persisted_factor_freed_capacity(
        self, engine: AsyncEngine, recreate_tables: None, broker: SqlBroker
    ) -> None:
        """Top up freed `max_not_persisted_factor` capacity."""
        client = broker.config.broker_config.client
        client.fetch = AsyncMock(wraps=client.fetch)
        fetched = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=4,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=0,
            fetch_batch_size=4,
            max_not_processed_factor=2,
            max_not_persisted_factor=4,
            flush_interval=2,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            nonlocal fetched
            await asyncio.sleep(0)
            fetched.append(msg)

        for _ in range(20):
            await broker.publish({"message": "hello"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        assert len(fetched) == 16
        assert client.fetch.await_count == 4

        await asyncio.sleep(2)

        assert len(fetched) == 20

    @pytest.mark.asyncio()
    async def test_consume_release_stuck(
        self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event
    ) -> None:
        """Broker was stopped mid-processing, processing wasn't finalized/flushed,
        messages were requeued on next startup.
        """
        async with self.get_broker(engine=engine, graceful_timeout=0.1) as broker:
            attempted = []

            @broker.subscriber(
                queues=["default1"],
                max_workers=2,
                retry_strategy=ConstantRetryStrategy(
                    delay_seconds=0, max_total_delay_seconds=None, max_attempts=2
                ),
                max_fetch_interval=0,
                min_fetch_interval=0,
                fetch_batch_size=5,
                max_not_processed_factor=1,
                flush_interval=10,
                release_stuck_interval=10,
                release_stuck_timeout=0.5,
                max_deliveries=20,
                ack_policy=AckPolicy.NACK_ON_ERROR,
            )
            async def handler(msg: Any) -> None:
                nonlocal attempted
                attempted.append(msg)
                await asyncio.sleep(1)

            await broker.publish({"message": "hello1"}, queue="default1")
            await broker.publish({"message": "hello2"}, queue="default1")

            # message is attempted but not finalized and not flushed
            await broker.start()
            await asyncio.sleep(0.5)
            await broker.stop()

            assert len(attempted) == 2
            async with engine.begin() as conn:
                result = await conn.execute(text("SELECT * FROM message;"))
            result = result.mappings().all()
            assert len(result) == 2
            assert result[0]["state"] == SqlBrokerMessageState.PROCESSING.name
            assert result[0]["attempts_count"] == 0
            assert result[0]["deliveries_count"] == 1

            await broker.start()
            await asyncio.sleep(0.5)

            assert len(attempted) == 4

    @pytest.mark.asyncio()
    async def test_consume_work_sharing(
        self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event
    ) -> None:
        async with (
            self.get_broker(engine=engine) as broker_1,
            self.get_broker(engine=engine) as broker_2,
            self.get_broker(engine=engine) as broker_3,
        ):
            attempt_counts = {}

            @broker_3.subscriber(
                queues=["default1"],
                max_workers=10,
                retry_strategy=NoRetryStrategy(),
                max_fetch_interval=0,
                min_fetch_interval=0,
                fetch_batch_size=10,
                max_not_processed_factor=1,
                flush_interval=1,
                release_stuck_interval=10,
                release_stuck_timeout=10,
                max_deliveries=20,
                ack_policy=AckPolicy.NACK_ON_ERROR,
            )
            @broker_2.subscriber(
                queues=["default1"],
                max_workers=10,
                retry_strategy=NoRetryStrategy(),
                max_fetch_interval=0,
                min_fetch_interval=0,
                fetch_batch_size=10,
                max_not_processed_factor=1,
                flush_interval=1,
                release_stuck_interval=10,
                release_stuck_timeout=10,
                max_deliveries=20,
                ack_policy=AckPolicy.NACK_ON_ERROR,
            )
            @broker_1.subscriber(
                queues=["default1"],
                max_workers=10,
                retry_strategy=NoRetryStrategy(),
                max_fetch_interval=0,
                min_fetch_interval=0,
                fetch_batch_size=10,
                max_not_processed_factor=1,
                flush_interval=1,
                release_stuck_interval=10,
                release_stuck_timeout=10,
                max_deliveries=20,
                ack_policy=AckPolicy.NACK_ON_ERROR,
            )
            async def handler(msg: Any) -> None:
                nonlocal attempt_counts
                attempt_counts[msg["message"]] = attempt_counts.get(msg["message"], 0) + 1

            msg_count = 1000
            for idx in range(msg_count):
                await broker_1.publish({"message": f"{idx + 1}"}, queue="default1")

            await broker_1.start()
            await broker_2.start()
            await broker_3.start()

            while True:
                if len(attempt_counts) != msg_count:
                    await asyncio.sleep(0.1)
                else:
                    break

            for idx in attempt_counts.values():
                assert idx == 1

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("retain_in_archive_on_ack", (True, False))
    async def test_consume_batch(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
        retain_in_archive_on_ack: bool,
    ) -> None:
        received: list[Any] = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            batch=True,
            batch_max_records=3,
            batch_max_accumulation_timeout_factor=0.04,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
            retain_in_archive_on_ack=retain_in_archive_on_ack,
        )
        async def handler(bodies: list[dict[str, Any]]) -> None:
            received.append(bodies)
            event.set()

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            {"message": "hello3"},
            queue="default1",
        )
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)
        await asyncio.sleep(0.3)

        assert len(received) == 1
        assert received[0] == [
            {"message": "hello1"},
            {"message": "hello2"},
            {"message": "hello3"},
        ]

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        if retain_in_archive_on_ack:
            result = result.mappings().all()
            assert len(result) == 3
            result = sorted(result, key=lambda x: json.loads(x["payload"])["message"])[0]  # noqa: FURB192
            assert result["queue"] == "default1"
            assert json.loads(result["payload"]) == {"message": "hello1"}
            assert result["state"] == SqlBrokerMessageState.COMPLETED.name
            assert result["attempts_count"] == 1
            assert result["deliveries_count"] == 1
            assert as_datetime(result["created_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["first_attempt_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["first_attempt_at"]) > as_datetime(
                result["created_at"]
            )
            assert as_datetime(result["last_attempt_at"]) == as_datetime(
                result["first_attempt_at"]
            )
            assert as_datetime(result["archived_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["archived_at"]) > as_datetime(
                result["first_attempt_at"]
            )

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_batch_max_records(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        received: list[Any] = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            batch=True,
            batch_max_records=2,
            batch_max_accumulation_timeout_factor=0.49,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )
        async def handler(bodies: list[dict[str, Any]]) -> None:
            received.append(bodies)
            if sum(len(batch) for batch in received) >= 4:
                event.set()

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            {"message": "hello3"},
            {"message": "hello4"},
            queue="default1",
        )
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)
        await asyncio.sleep(0.3)

        assert len(received) == 2
        assert all(len(batch) == 2 for batch in received)
        assert sorted([m["message"] for batch in received for m in batch]) == sorted([
            "hello1",
            "hello2",
            "hello3",
            "hello4",
        ])

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        assert len(result.all()) == 4

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_batch_timeout(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        received: list[Any] = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=0.1,
            min_fetch_interval=0,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            batch=True,
            batch_max_records=5,
            batch_max_accumulation_timeout_factor=9,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )
        async def handler(bodies: list[dict[str, Any]]) -> None:
            received.append(bodies)

        await broker.start()
        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")
        await asyncio.sleep(0.1)
        assert len(received) == 0

        await asyncio.sleep(1.1)

        assert len(received) == 1
        assert received[0] == [{"message": "hello1"}, {"message": "hello2"}]

    @pytest.mark.asyncio()
    async def test_consume_batch_stop_in_awaited_gathering(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        broker: SqlBroker,
    ) -> None:
        """Partial batch gathered before stop was requeued without being handled."""
        received: list[Any] = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            batch=True,
            batch_max_records=5,
            batch_max_accumulation_timeout_factor=0.99,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )
        async def handler(bodies: list[dict[str, Any]]) -> None:
            received.append(bodies)

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            queue="default1",
        )
        await broker.start()
        await asyncio.sleep(0.3)
        assert received == []

        await broker.stop()

        assert received == []

        await asyncio.sleep(0.1)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        rows = result.mappings().all()
        assert len(rows) == 2
        for row in rows:
            assert row["state"] == SqlBrokerMessageState.PENDING.name
            assert row["attempts_count"] == 0
            assert row["deliveries_count"] == 0
            assert row["acquired_at"] is None
            assert row["first_attempt_at"] is None
            assert row["last_attempt_at"] is None

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_batch_stop_in_processing(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        broker: SqlBroker,
    ) -> None:
        received: list[Any] = []
        event = asyncio.Event()

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            batch=True,
            batch_max_records=2,
            batch_max_accumulation_timeout_factor=0.99,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )
        async def handler(bodies: list[dict[str, Any]]) -> None:
            event.set()
            received.append(bodies)
            await asyncio.sleep(1)

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            queue="default1",
        )
        await broker.start()
        await event.wait()

        await broker.stop()
        await asyncio.sleep(0.1)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        assert len(result.all()) == 2

    @pytest.mark.asyncio()
    async def test_consume_batch_automatic_nack(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=10, max_total_delay_seconds=None, max_attempts=None
            ),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            batch=True,
            batch_max_records=2,
            batch_max_accumulation_timeout_factor=0.04,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(bodies: list[dict[str, Any]]) -> None:
            return 1 / 0

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            queue="default1",
        )
        await broker.start()
        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        rows = result.mappings().all()
        assert len(rows) == 2
        for row in rows:
            assert row["state"] == SqlBrokerMessageState.RETRYABLE.name
            assert row["attempts_count"] == 1

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_batch_manual_ack(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=10, max_total_delay_seconds=None, max_attempts=None
            ),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            batch=True,
            batch_max_records=3,
            batch_max_accumulation_timeout_factor=0.04,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(batch: SqlBrokerBatchMessageAnnotation) -> None:
            await batch.messages[0].ack()
            await batch.messages[1].reject()
            await batch.messages[2].nack()
            return 1 / 0

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            {"message": "hello3"},
            queue="default1",
        )
        await broker.start()
        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        rows = result.mappings().all()
        assert len(rows) == 2
        assert sorted(row["state"] for row in rows) == [
            SqlBrokerMessageState.COMPLETED.name,
            SqlBrokerMessageState.FAILED.name,
        ]

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        rows = result.mappings().all()
        assert len(rows) == 1
        assert rows[0]["state"] == SqlBrokerMessageState.RETRYABLE.name

    @pytest.mark.asyncio()
    async def test_consume_batch_automatic_ack_via_batch_annotation(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=10, max_total_delay_seconds=None, max_attempts=None
            ),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            batch=True,
            batch_max_records=2,
            batch_max_accumulation_timeout_factor=0.04,
            ack_policy=AckPolicy.ACK,
        )
        async def handler(
            batch: SqlBrokerBatchMessageAnnotation,
        ) -> None:
            return 1 / 0

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            queue="default1",
        )
        await broker.start()
        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        rows = result.mappings().all()
        assert len(rows) == 2
        assert {row["state"] for row in rows} == {SqlBrokerMessageState.COMPLETED.name}

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_batch_manual_ack_via_batch_annotation(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """Manual Ack on SqlBrokerBatchMessage overrides automatic Nack."""

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=10, max_total_delay_seconds=None, max_attempts=None
            ),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            batch=True,
            batch_max_records=2,
            batch_max_accumulation_timeout_factor=0.04,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(
            bodies: list[dict[str, Any]],
            batch: SqlBrokerBatchMessageAnnotation,
        ) -> None:
            await batch.ack()
            return 1 / 0

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            queue="default1",
        )
        await broker.start()
        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        rows = result.mappings().all()
        assert len(rows) == 2
        assert {row["state"] for row in rows} == {SqlBrokerMessageState.COMPLETED.name}

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("retain_in_archive_on_reject", (True, False))
    async def test_consume_batch_full_retry_flow(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
        retain_in_archive_on_reject: bool,
    ) -> None:
        attempted = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=0.01, max_total_delay_seconds=None, max_attempts=3
            ),
            max_fetch_interval=0.01,
            min_fetch_interval=0.01,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
            retain_in_archive_on_reject=retain_in_archive_on_reject,
            batch=True,
            batch_max_records=3,
        )
        async def handler(bodies: list[dict[str, Any]]) -> None:
            nonlocal attempted
            attempted.extend(bodies)
            return 1 / 0

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            {"message": "hello3"},
            {"message": "hello4"},
            {"message": "hello5"},
            queue="default1",
        )
        await broker.start()

        await asyncio.sleep(1)

        assert len(attempted) == 15

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        if retain_in_archive_on_reject:
            result = result.mappings().all()
            result = sorted(result, key=lambda x: json.loads(x["payload"])["message"])[0]  # noqa: FURB192
            assert result["queue"] == "default1"
            assert json.loads(result["payload"]) == {"message": "hello1"}
            assert result["state"] == SqlBrokerMessageState.FAILED.name
            assert result["attempts_count"] == 3
            assert result["deliveries_count"] == 3
            assert as_datetime(result["created_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["first_attempt_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
            assert as_datetime(result["first_attempt_at"]) > as_datetime(
                result["created_at"]
            )
            assert as_datetime(result["last_attempt_at"]) > as_datetime(
                result["first_attempt_at"]
            )
            assert as_datetime(result["last_attempt_at"]) < datetime.now(
                tz=timezone.utc
            ).replace(tzinfo=None)
        else:
            assert len(result.all()) == 0

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_batch_context_fields(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        bodies_ = None
        batch_ = None
        broker_ = None
        context_ = None
        logger_ = None

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            batch=True,
            batch_max_records=2,
            batch_max_accumulation_timeout_factor=0.04,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(
            bodies: Any,
            batch: SqlBrokerBatchMessageAnnotation,
            broker: SqlBrokerAnnotation,
            context: ContextRepoAnnotation,
            logger: LoggerAnnotation,
        ) -> None:
            nonlocal bodies_, batch_, broker_, context_, logger_
            bodies_ = bodies
            batch_ = batch
            broker_ = broker
            context_ = context
            logger_ = logger
            event.set()

        await broker.publish(
            {"message": "hello1"}, queue="default1", headers={"header_1": "value_1"}
        )
        await broker.publish(
            {"message": "hello2"}, queue="default1", headers={"header_2": "value_2"}
        )
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert bodies_ == [{"message": "hello1"}, {"message": "hello2"}]
        assert isinstance(batch_, SqlBrokerBatchMessage)
        assert [m.headers for m in batch_.messages] == [
            {"content-type": "application/json", "header_1": "value_1"},
            {"content-type": "application/json", "header_2": "value_2"},
        ]
        assert isinstance(broker_, SqlBroker)
        assert isinstance(context_, ContextRepo)
        assert isinstance(logger_, logging.Logger)

    @pytest.mark.asyncio()
    async def test_consume_batch_pydantic_model(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """A batch handler can be annotated with a list of a Pydantic model."""

        class MyModel(BaseModel):
            message: str

        received: list[Any] = []

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            batch=True,
            batch_max_records=2,
            batch_max_accumulation_timeout_factor=0.04,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )
        async def handler(bodies: list[MyModel]) -> None:
            received.extend(bodies)
            event.set()

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            queue="default1",
        )
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)
        await asyncio.sleep(0.3)

        assert received == [MyModel(message="hello1"), MyModel(message="hello2")]

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0

    @pytest.mark.asyncio()
    async def test_consume_batch_custom_decoder(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        """A custom decoder delegating to the default gets decoded payloads."""
        received: list[Any] = []

        async def decoder(
            msg: SqlBrokerBatchMessage,
            original_decoder: Callable[[SqlBrokerBatchMessage], Awaitable[Any]],
        ) -> Any:
            decoded = await original_decoder(msg)
            return [{**item, "seen": True} for item in decoded]

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            batch=True,
            batch_max_records=2,
            batch_max_accumulation_timeout_factor=0.04,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
            decoder=decoder,
        )
        async def handler(bodies: list[dict[str, Any]]) -> None:
            received.extend(bodies)
            event.set()

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            queue="default1",
        )
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert received == [
            {"message": "hello1", "seen": True},
            {"message": "hello2", "seen": True},
        ]

    @pytest.mark.asyncio()
    async def test_consume_batch_headers(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        broker: SqlBroker,
    ) -> None:
        bodies_: list[Any] | None = None
        messages_: list[SqlBrokerMessage] | None = None
        batch_: SqlBrokerBatchMessage | None = None

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            max_not_processed_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            batch=True,
            batch_max_records=2,
            batch_max_accumulation_timeout_factor=0.04,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )
        async def handler(
            bodies: list[dict[str, Any]],
            batch: SqlBrokerBatchMessageAnnotation,
        ) -> None:
            nonlocal bodies_, messages_, batch_
            bodies_ = bodies
            messages_ = batch.messages
            batch_ = batch
            event.set()

        await broker.publish(
            {"message": "hello1"},
            queue="default1",
            headers={"header_1": "value_1"},
        )
        await broker.publish(
            {"message": "hello2"},
            queue="default1",
            headers={"header_2": "value_2"},
        )
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert bodies_ == [{"message": "hello1"}, {"message": "hello2"}]
        assert messages_ is not None
        assert len(messages_) == 2
        assert all(isinstance(m, SqlBrokerMessage) for m in messages_)
        # Per-record wrappers carry the raw payload, exactly as in single mode.
        assert [m.body for m in messages_] == [
            b'{"message":"hello1"}',
            b'{"message":"hello2"}',
        ]
        # And they have a decoder wired, so `decode()` works inside the handler.
        assert await messages_[0].decode() == {"message": "hello1"}
        assert messages_[0].headers["header_1"] == "value_1"
        assert messages_[1].headers["header_2"] == "value_2"
        assert isinstance(batch_, SqlBrokerBatchMessage)
        assert batch_.body == [b'{"message":"hello1"}', b'{"message":"hello2"}']
        assert len(batch_.batch_headers) == 2
        assert batch_.batch_headers[0]["header_1"] == "value_1"
        assert batch_.batch_headers[1]["header_2"] == "value_2"
