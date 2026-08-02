from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from faststream.message.utils import decode_message

from faststream_sqlbroker.sqlbroker.message import (
    SqlBrokerBatchMessage,
    SqlBrokerInnerMessage,
    SqlBrokerMessage,
)

if TYPE_CHECKING:
    from faststream._internal.basic_types import DecodedMessage
    from faststream.message import StreamMessage


@dataclass
class SqlBrokerParser:
    async def parse_message(
        self,
        message: SqlBrokerInnerMessage,
    ) -> SqlBrokerMessage:
        return SqlBrokerMessage(
            raw_message=message,
            body=message.payload,
            headers=message.headers,
            content_type=message.headers.get("content-type"),
            correlation_id=message.headers.get("correlation_id"),
        )

    async def parse_batch(
        self,
        message: tuple[SqlBrokerInnerMessage, ...],
    ) -> "StreamMessage[tuple[SqlBrokerInnerMessage, ...]]":
        messages: list[SqlBrokerMessage] = []
        for item in message:
            parsed = await self.parse_message(item)
            # The framework only wires a decoder onto the top-level parsed
            # message, so per-record wrappers need one set explicitly to keep
            # `await message.decode()` working inside batch handlers.
            parsed.set_decoder(self.decode_message)
            messages.append(parsed)

        body: list[Any] = [item.body for item in messages]
        batch_headers: list[dict[str, Any]] = [item.headers for item in messages]

        headers = batch_headers[0]
        first = message[0]
        last = message[-1]

        return SqlBrokerBatchMessage(
            raw_message=message,
            body=body,
            headers=headers,
            batch_headers=batch_headers,
            content_type=headers.get("content-type"),
            correlation_id=headers.get("correlation_id"),
            message_id=f"{first.id}-{last.id}",
            messages=messages,
        )

    async def decode_message(
        self,
        msg: Any,
    ) -> "DecodedMessage":
        return decode_message(msg)

    async def decode_batch(
        self,
        msg: SqlBrokerBatchMessage,
    ) -> list["DecodedMessage"]:
        return [decode_message(item) for item in msg.messages]
