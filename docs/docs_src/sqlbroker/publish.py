from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import create_async_engine

from faststream import FastStream
from faststream.sqlbroker import SqlBroker

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker = SqlBroker(engine=engine)
app = FastStream(broker)

publisher_sqlbroker = broker.publisher()

@app.after_startup
async def publish_examples():
    await publisher_sqlbroker.publish("Hello, SqlBroker!", queue="my_queue")

    await publisher_sqlbroker.publish(
        "Process me later",
        queue="my_queue",
        next_attempt_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )

    async with engine.begin() as connection:
        # ... your other database operations using `connection` ...
        await publisher_sqlbroker.publish(
            "Transactional message",
            queue="my_queue",
            connection=connection,
        )
