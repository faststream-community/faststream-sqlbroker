from sqlalchemy.ext.asyncio import create_async_engine

from faststream_sqlbroker import (
    SqlBroker,
    SqlBrokerSchemaConfig,
    SqlBrokerSchemaVariant,
    SqlBrokerWorkQueueSchemaVersion,
)

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker = SqlBroker(
    engine=engine,
    schema=SqlBrokerSchemaConfig(
        message_table_name="message",
        message_archive_table_name="message_archive",
        variant=SqlBrokerSchemaVariant.WORK_QUEUE,
        version=SqlBrokerWorkQueueSchemaVersion.V1,
    ),
)
