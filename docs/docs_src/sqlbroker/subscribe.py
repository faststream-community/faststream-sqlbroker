from sqlalchemy.ext.asyncio import create_async_engine

from faststream import FastStream

from faststream_sqlbroker import ConstantRetryStrategy
from faststream_sqlbroker import SqlBroker

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker = SqlBroker(engine=engine)
app = FastStream(broker)


@broker.subscriber(
    queues=["my_queue"],
    retry_strategy=ConstantRetryStrategy(
        delay_seconds=5,
        max_attempts=3,
        max_total_delay_seconds=None,
    ),
    min_fetch_interval=0.1,
    max_fetch_interval=1,
    fetch_batch_size=10,
    flush_interval=1,
)
async def handler(msg: str):
    print(msg)
