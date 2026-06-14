from sqlalchemy.ext.asyncio import create_async_engine

from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaPublishMessage

from faststream_sqlbroker import SqlBroker, SqlBrokerMessage

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker_sqlbroker = SqlBroker(engine=engine)
broker_kafka = KafkaBroker("127.0.0.1:9092")
app = FastStream(broker_sqlbroker, on_startup=[broker_kafka.connect])
publisher_sqlbroker = broker_sqlbroker.publisher()


@app.after_startup # just an example
async def publish_examples():
    async with engine.begin() as connection:
        # ... your other database operations using `connection` ...
        await publisher_sqlbroker.publish(
            {"message": "Hello, SqlBroker!"},
            queue="sqlbroker_queue",
            headers={
                "x-test-header": "outbox",
                "x-kafka-key-source": "outbox-key",
            },
            connection=connection,
        )


publisher_kafka = broker_kafka.publisher("kafka_topic")


@publisher_kafka
@broker_sqlbroker.subscriber(
    queues=["sqlbroker_queue"],
    max_fetch_interval=1,
    min_fetch_interval=0,
    fetch_batch_size=10,
    flush_interval=3,
)
async def handle_msg(
    msg_body: dict,
    msg: SqlBrokerMessage,
) -> KafkaPublishMessage:
    return KafkaPublishMessage(
        msg_body,
        headers={
            "x-test-header": msg.headers["x-test-header"],
        },
        key=msg.headers["x-kafka-key-source"].encode(),
    )
