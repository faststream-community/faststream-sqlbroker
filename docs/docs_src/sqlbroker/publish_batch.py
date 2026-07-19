from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import create_async_engine

from faststream import FastStream
from faststream_sqlbroker import SqlBroker, SqlBrokerPublishMessage

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker = SqlBroker(engine=engine)
app = FastStream(broker)

publisher_sqlbroker = broker.publisher()

@app.after_startup
async def publish_batch_examples():
    await broker.publish_batch(
        "Hello, SqlBroker!",
        "Another message",
        queue="my_queue",
    )

    await publisher_sqlbroker.publish_batch(
        "Hello, SqlBroker!",
        "Another message",
        queue="my_queue",
    )

    await broker.publish_batch(
        SqlBrokerPublishMessage(
            "Order placed",
            queue="orders",
            headers={"x-source": "checkout"},
            correlation_id="order-1",
        ),
        SqlBrokerPublishMessage(
            "Retry later",
            next_attempt_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        ),
        "Uses batch defaults",
        queue="my_queue",
        headers={"x-default": "batch"},
    )
