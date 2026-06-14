from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.configs.broker import BrokerConfig
from faststream._internal.producer import ProducerUnset
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream_sqlbroker.sqlbroker.client import (
    SqlBrokerBaseClient,
    create_sqlbroker_client,
)
from faststream_sqlbroker.sqlbroker.schema import SqlBrokerSchemaConfig

if TYPE_CHECKING:
    from faststream_sqlbroker.sqlbroker.publisher.producer import SqlBrokerProducer


@dataclass(kw_only=True)
class SqlBrokerConfig(BrokerConfig):
    producer: "SqlBrokerProducer" = field(default_factory=ProducerUnset)  # type: ignore[assignment]
    validate_schema_on_start: bool = True
    schema: SqlBrokerSchemaConfig = field(default_factory=SqlBrokerSchemaConfig)
    engine: AsyncEngine | None = None
    client: SqlBrokerBaseClient | None = None

    @property
    def message_table_name(self) -> str:
        return self.schema.message_table_name

    @property
    def message_archive_table_name(self) -> str | None:
        return self.schema.message_archive_table_name

    async def connect(self, *, engine: AsyncEngine) -> None:
        self.engine = engine
        self.producer.connect(
            connection=None,
            serializer=self.fd_config._serializer,
        )
        self.client = create_sqlbroker_client(
            engine,
            schema=self.schema,
        )
        if self.validate_schema_on_start:
            await self.client.validate_schema()
