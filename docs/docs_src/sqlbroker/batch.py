from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine

from faststream import FastStream

from faststream_sqlbroker import SqlBroker, SqlBrokerBatchMessage

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker = SqlBroker(engine=engine)
app = FastStream(broker)


class MyModel(BaseModel):
    message: str


@broker.subscriber(
    queues=["my_queue"],
    max_workers=1,
    max_fetch_interval=1,
    fetch_batch_size=10,
    flush_interval=1,
    batch=True,
    batch_max_records=10,
)
async def handler(
    bodies: list[MyModel],
    batch: SqlBrokerBatchMessage,  # optional
) -> None:
    for body, message in zip(bodies, batch.messages, strict=True):
        print(body)  # message body
        await message.ack()  # per-message acknowledgement
    await batch.ack()  # alternatively, acknowledge the whole batch
