import datetime
import json

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream_sqlbroker.sqlbroker import SqlBroker, SqlBrokerPublishMessage
from faststream_sqlbroker.sqlbroker.exceptions import DatetimeMissingTimezoneException
from tests.brokers.base.publish import BrokerPublishTestcase
from tests.helpers import as_datetime

from .basic import SqlBrokerTestcaseConfig


@pytest.mark.connected()
@pytest.mark.slow()
class TestPublish(SqlBrokerTestcaseConfig, BrokerPublishTestcase):
    @pytest.mark.asyncio()
    async def test_reply_to(self) -> None: ...

    @pytest.mark.asyncio()
    async def test_no_reply(self) -> None: ...

    test_reusable_publishers = pytest.mark.flaky(reruns=3, reruns_delay=1)(
        BrokerPublishTestcase.test_reusable_publishers
    )

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_with_next_attempt_at_without_timezone(
        self, mode: str, broker: SqlBroker
    ) -> None:
        publisher = broker.publisher("default1")

        with pytest.raises(DatetimeMissingTimezoneException):  # noqa: PT012
            match mode:
                case "publish":
                    await broker.publish(
                        {"message": "hello1"},
                        queue="default1",
                        next_attempt_at=datetime.datetime.now(),  # noqa: DTZ005
                    )
                case "publisher":
                    await publisher.publish(
                        {"message": "hello1"},
                        next_attempt_at=datetime.datetime.now(),  # noqa: DTZ005
                    )

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_with_next_attempt_at_converts_timezone_to_utc(
        self, engine: AsyncEngine, mode: str, broker: SqlBroker
    ) -> None:
        publisher = broker.publisher("default1")

        match mode:
            case "publish":
                await broker.publish(
                    {"message": "hello1"},
                    queue="default1",
                    next_attempt_at=datetime.datetime(
                        year=2026,
                        month=1,
                        day=1,
                        hour=12,
                        minute=0,
                        second=0,
                        tzinfo=datetime.timezone(datetime.timedelta(hours=3), "MSC"),
                    ),
                )
            case "publisher":
                await publisher.publish(
                    {"message": "hello1"},
                    next_attempt_at=datetime.datetime(
                        year=2026,
                        month=1,
                        day=1,
                        hour=12,
                        minute=0,
                        second=0,
                        tzinfo=datetime.timezone(datetime.timedelta(hours=3), "MSC"),
                    ),
                )

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        result = result.mappings().all()

        assert as_datetime(result[0]["next_attempt_at"]) == datetime.datetime(  # noqa: DTZ001
            year=2026,
            month=1,
            day=1,
            hour=9,
            minute=0,
            second=0,
        )

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_uses_per_message_args(
        self, engine: AsyncEngine, mode: str, broker: SqlBroker
    ) -> None:
        next_attempt_at = datetime.datetime(
            year=2026,
            month=1,
            day=2,
            hour=12,
            minute=0,
            second=0,
            tzinfo=datetime.timezone(datetime.timedelta(hours=3), "MSC"),
        )
        message = SqlBrokerPublishMessage(
            {"message": "hello1"},
            queue="override-queue",
            headers={"x-shared": "override"},
            correlation_id="override-correlation",
            next_attempt_at=next_attempt_at,
        )

        match mode:
            case "publish":
                await broker.publish(
                    message,
                    queue="default-queue",
                    headers={"x-default": "value", "x-shared": "default"},
                )
            case "publisher":
                publisher = broker.publisher("default-queue")
                await publisher.publish(
                    message,
                    headers={"x-default": "value", "x-shared": "default"},
                )

        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT queue, headers, next_attempt_at FROM message")
            )
            row = result.one()

        assert row.queue == "override-queue"
        headers = (
            row.headers if isinstance(row.headers, dict) else json.loads(row.headers)
        )
        assert headers == {
            "content-type": "application/json",
            "correlation_id": "override-correlation",
            "x-default": "value",
            "x-shared": "override",
        }
        assert as_datetime(row.next_attempt_at) == datetime.datetime(  # noqa: DTZ001
            year=2026, month=1, day=2, hour=9, minute=0, second=0
        )


@pytest.mark.connected()
class TestPublishTransaction(SqlBrokerTestcaseConfig):
    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_wo_transaction(
        self, engine: AsyncEngine, mode: str, broker: SqlBroker
    ) -> None:
        publisher = broker.publisher("default1")

        match mode:
            case "publish":
                await broker.publish({"message": "hello1"}, queue="default1")
            case "publisher":
                await publisher.publish({"message": "hello1"})

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 1

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_in_transaction(
        self, engine: AsyncEngine, mode: str, broker: SqlBroker
    ) -> None:
        publisher = broker.publisher("default1")

        async with engine.begin() as conn:
            match mode:
                case "publish":
                    await broker.publish(
                        {"message": "hello1"}, queue="default1", connection=conn
                    )
                case "publisher":
                    await publisher.publish({"message": "hello1"}, connection=conn)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 1

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_in_transaction_rollback(
        self, engine: AsyncEngine, mode: str, broker: SqlBroker
    ) -> None:
        publisher = broker.publisher("default1")

        async with engine.begin() as conn:
            match mode:
                case "publish":
                    await broker.publish(
                        {"message": "hello1"}, queue="default1", connection=conn
                    )
                case "publisher":
                    await publisher.publish({"message": "hello1"}, connection=conn)
            await conn.rollback()

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0


async def _do_publish_batch(
    broker: SqlBroker,
    mode: str,
    *messages: object,
    queue: str = "batch-queue",
    **kwargs: object,
) -> None:
    match mode:
        case "broker":
            await broker.publish_batch(*messages, queue=queue, **kwargs)
        case "publisher":
            publisher = broker.publisher(queue)
            await publisher.publish_batch(*messages, **kwargs)


@pytest.mark.connected()
@pytest.mark.slow()
@pytest.mark.parametrize("mode", ("broker", "publisher"))
class TestPublishBatch(SqlBrokerTestcaseConfig):
    @pytest.mark.asyncio()
    async def test_publish_batch_inserts_all_messages(
        self, engine: AsyncEngine, broker: SqlBroker, mode: str
    ) -> None:
        await _do_publish_batch(
            broker,
            mode,
            {"message": "hello1"},
            {"message": "hello2"},
            {"message": "hello3"},
        )

        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT queue, payload FROM message ORDER BY id")
            )
            rows = result.all()

        assert len(rows) == 3
        assert all(row.queue == "batch-queue" for row in rows)
        payloads = [json.loads(bytes(row.payload).decode()) for row in rows]
        assert payloads == [
            {"message": "hello1"},
            {"message": "hello2"},
            {"message": "hello3"},
        ]

    @pytest.mark.asyncio()
    async def test_publish_batch_uses_single_sql_statement(
        self, engine: AsyncEngine, broker: SqlBroker, mode: str
    ) -> None:
        inserts: list[str] = []

        def _capture(conn, cursor, statement, parameters, context, executemany) -> None:
            if statement.lstrip().upper().startswith("INSERT INTO MESSAGE"):
                inserts.append(statement)

        event.listen(engine.sync_engine, "before_cursor_execute", _capture)
        try:
            await _do_publish_batch(broker, mode, b"a", b"b", b"c", b"d")
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", _capture)

        assert len(inserts) == 1

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM message"))
            assert result.scalar() == 4

    @pytest.mark.asyncio()
    async def test_publish_batch_empty_is_noop(
        self, engine: AsyncEngine, broker: SqlBroker, mode: str
    ) -> None:
        await _do_publish_batch(broker, mode)

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM message"))
            assert result.scalar() == 0

    @pytest.mark.asyncio()
    async def test_publish_batch_stores_headers_per_message(
        self, engine: AsyncEngine, broker: SqlBroker, mode: str
    ) -> None:
        await _do_publish_batch(
            broker,
            mode,
            {"message": "hello1"},
            b"raw",
            headers={"x-custom": "value"},
        )

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT headers FROM message ORDER BY id"))
            rows = result.all()

        headers = [
            row.headers if isinstance(row.headers, dict) else json.loads(row.headers)
            for row in rows
        ]
        assert headers[0] == {
            "content-type": "application/json",
            "x-custom": "value",
        }
        assert headers[1] == {"x-custom": "value"}

    @pytest.mark.asyncio()
    async def test_publish_batch_uses_per_message_args(
        self, engine: AsyncEngine, broker: SqlBroker, mode: str
    ) -> None:
        default_next_attempt_at = datetime.datetime(
            year=2026,
            month=1,
            day=1,
            hour=12,
            minute=0,
            second=0,
            tzinfo=datetime.timezone(datetime.timedelta(hours=3), "MSC"),
        )
        first_next_attempt_at = datetime.datetime(
            year=2026,
            month=1,
            day=2,
            hour=12,
            minute=0,
            second=0,
            tzinfo=datetime.timezone(datetime.timedelta(hours=3), "MSC"),
        )
        second_next_attempt_at = datetime.datetime(
            year=2026,
            month=1,
            day=3,
            hour=12,
            minute=0,
            second=0,
            tzinfo=datetime.timezone(datetime.timedelta(hours=3), "MSC"),
        )

        await _do_publish_batch(
            broker,
            mode,
            SqlBrokerPublishMessage(
                {"message": "hello1"},
                queue="first-queue",
                headers={"x-shared": "first"},
                correlation_id="first-correlation",
                next_attempt_at=first_next_attempt_at,
            ),
            SqlBrokerPublishMessage(
                {"message": "hello2"},
                queue="second-queue",
                headers={"x-shared": "second"},
                correlation_id="second-correlation",
                next_attempt_at=second_next_attempt_at,
            ),
            {"message": "hello3"},
            headers={"x-default": "value", "x-shared": "default"},
            next_attempt_at=default_next_attempt_at,
        )

        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT queue, headers, next_attempt_at FROM message ORDER BY id")
            )
            rows = result.all()

        assert [row.queue for row in rows] == [
            "first-queue",
            "second-queue",
            "batch-queue",
        ]
        headers = [
            row.headers if isinstance(row.headers, dict) else json.loads(row.headers)
            for row in rows
        ]
        assert headers == [
            {
                "content-type": "application/json",
                "correlation_id": "first-correlation",
                "x-default": "value",
                "x-shared": "first",
            },
            {
                "content-type": "application/json",
                "correlation_id": "second-correlation",
                "x-default": "value",
                "x-shared": "second",
            },
            {
                "content-type": "application/json",
                "x-default": "value",
                "x-shared": "default",
            },
        ]
        assert [as_datetime(row.next_attempt_at) for row in rows] == [
            datetime.datetime(  # noqa: DTZ001
                year=2026, month=1, day=2, hour=9, minute=0, second=0
            ),
            datetime.datetime(  # noqa: DTZ001
                year=2026, month=1, day=3, hour=9, minute=0, second=0
            ),
            datetime.datetime(  # noqa: DTZ001
                year=2026, month=1, day=1, hour=9, minute=0, second=0
            ),
        ]

    @pytest.mark.asyncio()
    async def test_publish_batch_with_next_attempt_at(
        self, engine: AsyncEngine, broker: SqlBroker, mode: str
    ) -> None:
        next_attempt_at = datetime.datetime(
            year=2026,
            month=1,
            day=1,
            hour=12,
            minute=0,
            second=0,
            tzinfo=datetime.timezone(datetime.timedelta(hours=3), "MSC"),
        )

        await _do_publish_batch(
            broker,
            mode,
            {"message": "hello1"},
            {"message": "hello2"},
            next_attempt_at=next_attempt_at,
        )

        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT next_attempt_at FROM message ORDER BY id")
            )
            rows = result.all()

        expected = datetime.datetime(  # noqa: DTZ001
            year=2026, month=1, day=1, hour=9, minute=0, second=0
        )
        for row in rows:
            assert as_datetime(row.next_attempt_at) == expected

    @pytest.mark.asyncio()
    async def test_publish_batch_without_timezone_raises(
        self, broker: SqlBroker, mode: str
    ) -> None:
        with pytest.raises(DatetimeMissingTimezoneException):
            await _do_publish_batch(
                broker,
                mode,
                {"message": "hello1"},
                {"message": "hello2"},
                next_attempt_at=datetime.datetime.now(),  # noqa: DTZ005
            )


@pytest.mark.connected()
@pytest.mark.parametrize("mode", ("broker", "publisher"))
class TestPublishBatchTransaction(SqlBrokerTestcaseConfig):
    @pytest.mark.asyncio()
    async def test_publish_batch_in_transaction(
        self, engine: AsyncEngine, broker: SqlBroker, mode: str
    ) -> None:
        async with engine.begin() as conn:
            await _do_publish_batch(broker, mode, b"a", b"b", connection=conn)

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM message"))
        assert result.scalar() == 2

    @pytest.mark.asyncio()
    async def test_publish_batch_in_transaction_rollback(
        self, engine: AsyncEngine, broker: SqlBroker, mode: str
    ) -> None:
        async with engine.begin() as conn:
            await _do_publish_batch(broker, mode, b"a", b"b", connection=conn)
            await conn.rollback()

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM message"))
        assert result.scalar() == 0
