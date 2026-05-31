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

from faststream import AckPolicy, FastStream
from faststream.kafka import KafkaBroker

from faststream_sqlbroker.sqlbroker import SqlBroker
from faststream_sqlbroker.sqlbroker.retry import ExponentialBackoffRetryStrategy

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
            connection=connection,
        )

```

And relay the messages from the database to another broker.

```python linenums="1"
publisher_kafka = broker_kafka.publisher("kafka_topic")


@publisher_kafka
@broker_sqlbroker.subscriber(
    queues=["sqlbroker_queue"],
    max_workers=10,
    retry_strategy=ExponentialBackoffRetryStrategy(
        initial_delay_seconds=1,
        multiplier=2,
        max_delay_seconds=60 * 5,
        max_total_delay_seconds=60 * 60 * 6,
        max_attempts=None,
    ),
    max_fetch_interval=1,
    min_fetch_interval=0,
    fetch_batch_size=10,
    overfetch_factor=1.5,
    flush_interval=3,
    release_stuck_interval=5,
    release_stuck_timeout=60 * 60,
    max_deliveries=20,
    ack_policy=AckPolicy.NACK_ON_ERROR,
)
async def handle_msg(msg_body: dict) -> dict:
    return msg_body

```

## Origins

Originated as a [PR to FastStream](https://github.com/ag2ai/faststream/pull/2704).
