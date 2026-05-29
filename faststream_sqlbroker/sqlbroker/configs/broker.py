from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.configs.broker import BrokerConfig
from faststream._internal.producer import ProducerUnset
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream_sqlbroker.sqlbroker.client import (
    SqlBrokerBaseClient,
    create_sqlbroker_client,
)

if TYPE_CHECKING:
    from faststream_sqlbroker.sqlbroker.publisher.producer import SqlBrokerProducer


@dataclass(kw_only=True)
class SqlBrokerConfig(BrokerConfig):
    producer: "SqlBrokerProducer" = field(default_factory=ProducerUnset)  # type: ignore[assignment]
    validate_schema_on_start: bool = True
    message_table_name: str = "message"
    message_archive_table_name: str = "message_archive"
    engine: AsyncEngine | None = None
    client: SqlBrokerBaseClient | None = None

    async def connect(self, *, engine: AsyncEngine) -> None:
        self.engine = engine
        self.producer.connect(
            connection=None,
            serializer=self.fd_config._serializer,
        )
        self.client = create_sqlbroker_client(
            engine,
            message_table_name=self.message_table_name,
            message_archive_table_name=self.message_archive_table_name,
        )
        if self.validate_schema_on_start:
            await self.client.validate_schema()
