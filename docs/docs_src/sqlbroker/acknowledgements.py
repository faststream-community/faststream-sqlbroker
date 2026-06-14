from sqlalchemy.ext.asyncio import create_async_engine

from faststream import AckPolicy, FastStream

from faststream_sqlbroker import ConstantRetryStrategy, SqlBroker, SqlBrokerMessage

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker = SqlBroker(engine=engine)
app = FastStream(broker)


@broker.subscriber(
    queues=["my_queue"],
    ack_policy=AckPolicy.NACK_ON_ERROR,
    retry_strategy=ConstantRetryStrategy(
        delay_seconds=5,
        max_attempts=3,
        max_total_delay_seconds=None,
    ),
    max_fetch_interval=1.0,
    min_fetch_interval=0.1,
    fetch_batch_size=10,
    flush_interval=1.0,
)
async def automatic_handler(msg: str) -> None:
    print(msg)


@broker.subscriber(
    queues=["my_queue"],
    ack_policy=AckPolicy.MANUAL,
    max_fetch_interval=1.0,
    min_fetch_interval=0.1,
    fetch_batch_size=10,
    flush_interval=1.0,
)
async def manual_handler(msg: SqlBrokerMessage, body: str) -> None:
    await msg.ack()
    print(body)
