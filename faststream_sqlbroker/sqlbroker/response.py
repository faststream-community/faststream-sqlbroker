from datetime import datetime, timezone
from typing import TYPE_CHECKING, Union

from faststream.response.publish_type import PublishType
from faststream.response.response import BatchPublishCommand, PublishCommand, Response
from sqlalchemy.ext.asyncio import AsyncConnection

from faststream_sqlbroker.sqlbroker.exceptions import DatetimeMissingTimezoneException

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage


def _normalize_next_attempt_at(value: datetime | None) -> datetime | None:
    if value is None:
        return None

    _validate_next_attempt_at(value)
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _validate_next_attempt_at(value: datetime | None) -> None:
    if value is not None and value.tzinfo is None:
        raise DatetimeMissingTimezoneException


class SqlBrokerResponse(Response):
    """An outgoing SQL broker message with optional per-message arguments."""

    def __init__(
        self,
        body: "SendableMessage",
        *,
        queue: str | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        next_attempt_at: datetime | None = None,
    ) -> None:
        super().__init__(
            body=body,
            headers=headers,
            correlation_id=correlation_id,
        )
        self.queue = queue
        _validate_next_attempt_at(next_attempt_at)
        self.next_attempt_at = next_attempt_at

    def as_publish_command(self) -> "SqlBrokerPublishCommand":
        return SqlBrokerPublishCommand(
            self.body,
            queue=self.queue or "",
            headers=self.headers,
            correlation_id=self.correlation_id,
            next_attempt_at=self.next_attempt_at,
        )


class SqlBrokerPublishCommand(BatchPublishCommand):
    """TODO: per-message args aren't very native in FastStream.

    Per-message overrides (queue/headers/correlation_id/next_attempt_at) are
    carried on ``SqlBrokerResponse`` bodies and looked up positionally against
    ``batch_bodies`` at publish time. This is fragile: anything that reorders or
    filters ``batch_bodies`` after construction (e.g. a publish middleware)
    desyncs the overrides from their bodies. This mirrors FastStream's own Kafka
    per-message keys and is awaiting a more comprehensive upstream fix — see
    https://github.com/ag2ai/faststream/issues/2943.
    """

    def __init__(
        self,
        message: "SendableMessage",
        /,
        *messages: "SendableMessage",
        queue: str,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        next_attempt_at: datetime | None = None,
        connection: AsyncConnection | None = None,
    ) -> None:
        super().__init__(
            message,
            *messages,
            destination=queue,
            headers=headers,
            correlation_id=correlation_id,
            _publish_type=PublishType.PUBLISH,
        )
        self.next_attempt_at = _normalize_next_attempt_at(next_attempt_at)
        self.connection = connection

        self._per_message_args = tuple(
            body if isinstance(body, SqlBrokerResponse) else None
            for body in self.batch_bodies
        )
        if any(self._per_message_args):
            self.batch_bodies = tuple(
                body.body if isinstance(body, SqlBrokerResponse) else body
                for body in self.batch_bodies
            )

    @classmethod
    def from_cmd(
        cls,
        cmd: Union["PublishCommand", "SqlBrokerPublishCommand"],
        *,
        batch: bool = False,
    ) -> "SqlBrokerPublishCommand":
        if isinstance(cmd, SqlBrokerPublishCommand):
            return cmd

        return cls(
            cmd.body,
            queue=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
        )

    def headers_to_publish(self) -> dict[str, str]:
        headers = {}

        if self.correlation_id:
            headers["correlation_id"] = self.correlation_id

        return headers | (self.headers or {})

    def queue_for(self, index: int) -> str:
        args = self._args_for(index)
        if args is not None and args.queue is not None:
            return args.queue
        return self.destination

    def headers_to_publish_for(self, index: int) -> dict[str, str]:
        headers = self.headers_to_publish()
        args = self._args_for(index)
        if args is None:
            return headers

        if args.correlation_id:
            headers["correlation_id"] = args.correlation_id

        return headers | args.headers

    def next_attempt_at_for(self, index: int) -> datetime | None:
        args = self._args_for(index)
        if args is not None and args.next_attempt_at is not None:
            return _normalize_next_attempt_at(args.next_attempt_at)
        return self.next_attempt_at

    def _args_for(self, index: int) -> SqlBrokerResponse | None:
        try:
            return self._per_message_args[index]
        except IndexError:
            return None


SqlBrokerPublishMessage = SqlBrokerResponse
