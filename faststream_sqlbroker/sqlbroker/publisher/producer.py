from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional, cast

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.producer import ProducerProto
from faststream.exceptions import FeatureNotSupportedException
from faststream.message.utils import encode_message
from sqlalchemy.ext.asyncio import AsyncEngine
from typing_extensions import override

from faststream_sqlbroker.sqlbroker.parser import SqlBrokerParser
from faststream_sqlbroker.sqlbroker.response import SqlBrokerPublishCommand

if TYPE_CHECKING:
    from datetime import datetime

    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.types import AsyncCallable, CustomCallable

    from faststream_sqlbroker.sqlbroker.client import SqlBrokerBaseClient
    from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig


class SqlBrokerProducerProto(ProducerProto[SqlBrokerPublishCommand]):
    def connect(
        self,
        connection: Any,
        serializer: Optional["SerializerProto"],
    ) -> None: ...

    def disconnect(self) -> None: ...

    @abstractmethod
    async def publish(self, cmd: "SqlBrokerPublishCommand") -> None: ...

    async def request(self, cmd: "SqlBrokerPublishCommand") -> None:
        msg = "SqlBroker doesn't support synchronous requests."
        raise FeatureNotSupportedException(msg)

    @abstractmethod
    async def publish_batch(self, cmd: "SqlBrokerPublishCommand") -> None: ...


class SqlBrokerProducer(SqlBrokerProducerProto):
    _decoder: "AsyncCallable"
    _parser: "AsyncCallable"

    def __init__(
        self,
        *,
        engine: AsyncEngine,  # todo
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
        # message_table_name: str,
        # message_archive_table_name: str,
        # validate_schema_on_start: bool,
        config: "SqlBrokerConfig",
    ) -> None:
        self.config = config

        self.serializer: SerializerProto | None = None

        default = SqlBrokerParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)

    def connect(
        self,
        connection: Any,
        serializer: Optional["SerializerProto"],
    ) -> None:
        self.serializer = serializer

    @override
    async def publish(self, cmd: "SqlBrokerPublishCommand") -> None:
        payload, content_type = encode_message(cmd.body, self.serializer)

        headers_to_send = {
            **({"content-type": content_type} if content_type else {}),
            **cmd.headers_to_publish_for(0),
        }

        await cast("SqlBrokerBaseClient", self.config.client).enqueue(
            payload=payload,
            queue=cmd.queue_for(0),
            headers=headers_to_send,
            next_attempt_at=cmd.next_attempt_at_for(0),
            connection=cmd.connection,
        )

    @override
    async def publish_batch(self, cmd: "SqlBrokerPublishCommand") -> None:
        items: list[tuple[bytes, str, dict[str, str], datetime | None]] = []
        for index, body in enumerate(cmd.batch_bodies):
            payload, content_type = encode_message(body, self.serializer)
            headers = {
                **({"content-type": content_type} if content_type else {}),
                **cmd.headers_to_publish_for(index),
            }
            items.append((
                payload,
                cmd.queue_for(index),
                headers,
                cmd.next_attempt_at_for(index),
            ))

        await cast("SqlBrokerBaseClient", self.config.client).enqueue_batch(
            items,
            connection=cmd.connection,
        )
