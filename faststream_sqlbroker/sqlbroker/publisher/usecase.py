from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any, Union

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream.exceptions import FeatureNotSupportedException
from sqlalchemy.ext.asyncio import AsyncConnection
from typing_extensions import override

from faststream_sqlbroker.sqlbroker.response import SqlBrokerPublishCommand

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.endpoint.publisher import PublisherSpecification
    from faststream._internal.types import PublisherMiddleware
    from faststream.response.response import PublishCommand

    from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig
    from faststream_sqlbroker.sqlbroker.publisher.config import SqlBrokerPublisherConfig


class LogicPublisher(PublisherUsecase):
    _outer_config: "SqlBrokerConfig"

    def __init__(
        self,
        config: "SqlBrokerPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)
        self._queue = config.queue
        self.headers = config.headers or {}

    @property
    def queue(self) -> str:
        return f"{self._outer_config.prefix}{self._queue}"

    @override
    async def publish(
        self,
        message: "SendableMessage",
        queue: str = "",
        headers: dict[str, str] | None = None,
        next_attempt_at: datetime | None = None,
        connection: AsyncConnection | None = None,
        correlation_id: str | None = None,
    ) -> None:
        cmd = SqlBrokerPublishCommand(
            message,
            queue=queue or self.queue,
            headers=self.headers | (headers or {}),
            next_attempt_at=next_attempt_at,
            connection=connection,
        )

        await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "SqlBrokerPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        cmd = SqlBrokerPublishCommand.from_cmd(cmd)

        cmd.destination = self.queue
        cmd.add_headers(self.headers, override=False)

        await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def request(
        self,
        message: "SendableMessage",
        /,
        *,
        correlation_id: str | None = None,
    ) -> Any | None:
        msg = "SqlBroker doesn't support synchronous requests."
        raise FeatureNotSupportedException(msg)
