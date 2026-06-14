[![Downloads](https://static.pepy.tech/personalized-badge/faststream-sqlbroker?period=month&units=international_system&left_color=grey&right_color=green&left_text=downloads/month)](https://www.pepy.tech/projects/faststream-sqlbroker)
[![Package version](https://img.shields.io/pypi/v/faststream-sqlbroker?label=PyPI)](https://pypi.org/project/faststream-sqlbroker)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://faststream-community.github.io/faststream-sqlbroker/)

# faststream-sqlbroker

A SQL-backed broker for [FastStream](https://github.com/ag2ai/FastStream). [Documentation](https://faststream-community.github.io/faststream-sqlbroker/sqlbroker/tutorial/).


## Transactional outbox

Implementing the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) becomes as simple as the following.

Publish messages transactionally with your other database operations.

```python linenums="1"
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
```

And relay the messages from the database to another broker.

```python linenums="1"
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
```

## Origins

Originated as a [PR to FastStream](https://github.com/ag2ai/faststream/pull/2704).
