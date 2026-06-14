import asyncio
from typing import Any

import pytest
from faststream import FastStream, TestApp
from faststream.kafka import (
    KafkaBroker,
    KafkaMessage,
    KafkaPublishMessage,
    TestKafkaBroker,
)
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream_sqlbroker import SqlBroker, SqlBrokerMessage


def _unexpected_publish_mode_error(publish_mode: str) -> AssertionError:
    return AssertionError(f"Unexpected publish mode: {publish_mode}")


def _build_publish_kwargs(
    *,
    msg: SqlBrokerMessage,
    handled_headers: dict[str, str] | None,
) -> tuple[dict[str, Any], dict[str, str]]:
    handled_headers = msg.headers
    return (
        {
            "headers": {
                "x-test-header": msg.headers["x-test-header"],
            },
            "key": msg.headers["x-kafka-key-source"].encode(),
        },
        handled_headers,
    )


def _build_publish_message(
    *,
    msg_body: dict[str, str],
    msg: SqlBrokerMessage,
    handled_headers: dict[str, str] | None,
) -> tuple[KafkaPublishMessage, dict[str, str]]:
    publish_kwargs, handled_headers = _build_publish_kwargs(
        msg=msg,
        handled_headers=handled_headers,
    )
    return KafkaPublishMessage(msg_body, **publish_kwargs), handled_headers


def _register_outbox_handler(
    *,
    publish_mode: str,
    broker_kafka: KafkaBroker,
    publisher_kafka: Any,
    kafka_topic: str,
    subscriber: Any,
    build_publish_kwargs: Any,
    build_publish_message: Any,
) -> None:
    match publish_mode:
        case "broker_decorator":

            @broker_kafka.publisher(kafka_topic)
            @subscriber
            async def handle_msg(
                msg_body: dict[str, str],
                msg: SqlBrokerMessage,
            ) -> KafkaPublishMessage:
                return build_publish_message(msg_body, msg)

        case "publisher_decorator":

            @publisher_kafka
            @subscriber
            async def handle_msg(
                msg_body: dict[str, str],
                msg: SqlBrokerMessage,
            ) -> KafkaPublishMessage:
                return build_publish_message(msg_body, msg)

        case "broker_publish":

            @subscriber
            async def handle_msg(
                msg_body: dict[str, str],
                msg: SqlBrokerMessage,
            ) -> None:
                await broker_kafka.publish(
                    msg_body,
                    topic=kafka_topic,
                    **build_publish_kwargs(msg),
                )

        case "publisher_publish":

            @subscriber
            async def handle_msg(
                msg_body: dict[str, str],
                msg: SqlBrokerMessage,
            ) -> None:
                await publisher_kafka.publish(
                    msg_body,
                    **build_publish_kwargs(msg),
                )

        case _:
            raise _unexpected_publish_mode_error(publish_mode)


def _register_observer(
    *,
    broker_kafka: KafkaBroker,
    kafka_topic: str,
    queue: str,
    observe_message: Any,
) -> None:
    @broker_kafka.subscriber(
        kafka_topic,
        group_id=f"{queue}-observer",
        auto_offset_reset="earliest",
    )
    async def observe_msg(msg_body: dict[str, str], msg: KafkaMessage) -> None:
        observe_message(msg_body, msg)


@pytest.mark.connected()
@pytest.mark.slow()
@pytest.mark.kafka()
@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "publish_mode",
    (
        "broker_decorator",
        "publisher_decorator",
        "broker_publish",
        "publisher_publish",
    ),
)
async def test_kafka_outbox(
    engine: AsyncEngine,
    queue: str,
    publish_mode: str,
) -> None:
    if engine.dialect.name != "postgresql":
        pytest.skip()

    sql_queue = f"sql-{queue}"
    kafka_topic = f"kafka-{queue}"
    payload = {"message": "Hello, SqlBroker!"}
    publish_headers = {
        "x-test-header": "outbox",
        "x-kafka-key-source": "outbox-key",
    }

    broker_sqlbroker = SqlBroker(engine=engine)
    broker_kafka = KafkaBroker()
    app = FastStream(broker_sqlbroker, on_startup=[broker_kafka.start])
    publisher_sqlbroker = broker_sqlbroker.publisher()
    publisher_kafka = broker_kafka.publisher(kafka_topic)
    subscriber = broker_sqlbroker.subscriber(
        queues=[sql_queue],
        max_fetch_interval=1,
        min_fetch_interval=0,
        fetch_batch_size=10,
        flush_interval=3,
    )
    received: dict[str, str] | None = None
    handled_headers: dict[str, str] | None = None
    received_headers: dict[str, str] | None = None
    received_key: bytes | None = None
    received_event = asyncio.Event()

    def build_publish_kwargs(msg: SqlBrokerMessage) -> dict[str, Any]:
        nonlocal handled_headers
        publish_kwargs, handled_headers = _build_publish_kwargs(
            msg=msg,
            handled_headers=handled_headers,
        )
        return publish_kwargs

    def build_publish_message(
        msg_body: dict[str, str],
        msg: SqlBrokerMessage,
    ) -> KafkaPublishMessage:
        nonlocal handled_headers
        publish_message, handled_headers = _build_publish_message(
            msg_body=msg_body,
            msg=msg,
            handled_headers=handled_headers,
        )
        return publish_message

    _register_outbox_handler(
        publish_mode=publish_mode,
        broker_kafka=broker_kafka,
        publisher_kafka=publisher_kafka,
        kafka_topic=kafka_topic,
        subscriber=subscriber,
        build_publish_kwargs=build_publish_kwargs,
        build_publish_message=build_publish_message,
    )

    def observe_message(msg_body: dict[str, str], msg: KafkaMessage) -> None:
        nonlocal received, received_headers, received_key
        received = msg_body
        received_headers = msg.headers
        received_key = msg.raw_message.key
        received_event.set()

    _register_observer(
        broker_kafka=broker_kafka,
        kafka_topic=kafka_topic,
        queue=queue,
        observe_message=observe_message,
    )

    try:
        async with TestKafkaBroker(broker_kafka), TestApp(app):
            async with engine.begin() as connection:
                await publisher_sqlbroker.publish(
                    payload,
                    queue=sql_queue,
                    headers=publish_headers,
                    connection=connection,
                )

            await asyncio.wait_for(received_event.wait(), timeout=15)
            assert received == payload
            assert handled_headers == {
                "content-type": "application/json",
                **publish_headers,
            }
            assert received_headers is not None
            assert received_headers["x-test-header"] == publish_headers["x-test-header"]
            assert "x-kafka-key-source" not in received_headers
            assert received_key == publish_headers["x-kafka-key-source"].encode()
    finally:
        await broker_kafka.stop()
