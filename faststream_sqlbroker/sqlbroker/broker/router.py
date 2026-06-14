from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream.middlewares import AckPolicy

from faststream_sqlbroker.sqlbroker.broker.registrator import SqlBrokerRegistrator
from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig
from faststream_sqlbroker.sqlbroker.message import SqlBrokerInnerMessage
from faststream_sqlbroker.sqlbroker.retry import NoRetryStrategy, RetryStrategyProto

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
    )

_DEFAULT_RETRY_STRATEGY = NoRetryStrategy()


class SqlBrokerPublisher(ArgsContainer):
    """Delayed SqlBrokerPublisher registration object.

    Just a copy of `SqlBrokerRegistrator.publisher(...)` arguments.
    """

    def __init__(
        self,
        queue: str = "",
        *,
        headers: dict[str, str] | None = None,
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> None:
        """Initialize SqlBrokerPublisher.

        Args:
            queue: Queue name where the message will be published.
            headers:
                Message headers to store metainformation.
                **content-type** and **correlation_id** will be set automatically by framework anyway.
                Can be overridden by `publish.headers` if specified.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema:
                AsyncAPI publishing message type.
                Should be any python-native object annotation or `pydantic.BaseModel`.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            queue=queue,
            headers=headers,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class SqlBrokerRoute(SubscriberRoute):
    """Class to store delayed SqlBroker subscriber registration."""

    def __init__(
        self,
        call: Callable[..., "SendableMessage"]
        | Callable[..., Awaitable["SendableMessage"]],
        queues: list[str],
        *,
        publishers: Iterable[SqlBrokerPublisher] = (),
        max_workers: int = 1,
        retry_strategy: RetryStrategyProto | None = _DEFAULT_RETRY_STRATEGY,
        max_fetch_interval: float,
        min_fetch_interval: float,
        fetch_batch_size: int,
        overfetch_factor: float = 1.5,
        flush_interval: float,
        release_stuck_interval: float = 60,
        release_stuck_timeout: float = 60 * 10,
        max_deliveries: int | None = None,
        ack_policy: AckPolicy = AckPolicy.REJECT_ON_ERROR,
        # broker args
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        # AsyncAPI args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        """Initialize SqlBrokerRoute.

        Args:
            call:
                Message handler function
                to wrap the same with `@broker.subscriber(...)` way.
            queues: Queue names to consume messages from.
            publishers: SqlBroker publishers to broadcast the handler result.
            max_workers: Number of workers to process messages concurrently.
            retry_strategy:
                Called to determine if and when a message might be retried.
                Defaults to `NoRetryStrategy()`.
            max_fetch_interval:
                The maximum allowed interval between consecutive fetches.
            min_fetch_interval:
                The minimum allowed interval between consecutive fetches.
            fetch_batch_size:
                The maximum allowed number of messages to fetch in a single batch.
            overfetch_factor:
                The factor by which the fetch_batch_size is multiplied.
                Defaults to `1.5`.
            flush_interval:
                The interval at which the state of messages is flushed to the database.
            release_stuck_interval:
                The interval at which the PROCESSING-state messages are marked
                back as PENDING. Defaults to `60`.
            release_stuck_timeout:
                Timeout for releasing stuck messages. Defaults to `600`.
            max_deliveries:
                The maximum number of deliveries allowed for a message. Useful
                for poison message protection.
            ack_policy:
                Acknowledgement policy. Defaults to
                `AckPolicy.REJECT_ON_ERROR`.
            dependencies: Dependencies list to apply to the subscriber.
            parser: Parser to map original message object to FastStream one.
            decoder: Function to decode FastStream msg bytes body to python objects.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            call,
            publishers=publishers,
            queues=queues,
            max_workers=max_workers,
            retry_strategy=retry_strategy,
            max_fetch_interval=max_fetch_interval,
            min_fetch_interval=min_fetch_interval,
            fetch_batch_size=fetch_batch_size,
            overfetch_factor=overfetch_factor,
            flush_interval=flush_interval,
            release_stuck_interval=release_stuck_interval,
            release_stuck_timeout=release_stuck_timeout,
            max_deliveries=max_deliveries,
            ack_policy=ack_policy,
            # basic args
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            # AsyncAPI args
            title=title,
            description=description,
            include_in_schema=include_in_schema,
        )


class SqlBrokerRouter(
    SqlBrokerRegistrator,
    BrokerRouter[SqlBrokerInnerMessage],
):
    """Includable to SqlBroker router."""

    def __init__(
        self,
        prefix: str = "",
        handlers: Iterable[SqlBrokerRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[SqlBrokerRegistrator] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        include_in_schema: bool | None = None,
    ) -> None:
        """Initialize SqlBrokerRouter.

        Args:
            prefix: String prefix to add to all subscribers queues.
            handlers: Route object to include.
            dependencies: Dependencies list to apply to all routers' publishers/subscribers.
            middlewares: Router middlewares to apply to all routers' publishers/subscribers.
            routers: Routers to apply to broker.
            parser: Parser to map original message object to FastStream one.
            decoder: Function to decode FastStream msg bytes body to python objects.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            handlers=handlers,
            config=SqlBrokerConfig(
                broker_middlewares=middlewares,
                broker_dependencies=dependencies,
                broker_parser=parser,
                broker_decoder=decoder,
                include_in_schema=include_in_schema,
                prefix=prefix,
            ),
            routers=routers,
        )
