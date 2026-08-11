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

from faststream_sqlbroker.sqlbroker.broker.registrator import (
    ACK_POLICY_DEFAULT,
    BATCH_DEFAULT,
    BATCH_MAX_ACCUMULATION_TIMEOUT_FACTOR_DEFAULT,
    BATCH_MAX_RECORDS_DEFAULT,
    MAX_DELIVERIES_DEFAULT,
    MAX_NOT_PERSISTED_FACTOR_DEFAULT,
    MAX_NOT_PROCESSED_FACTOR_DEFAULT,
    MAX_WORKERS_DEFAULT,
    RELEASE_STUCK_INTERVAL_DEFAULT,
    RELEASE_STUCK_TIMEOUT_DEFAULT,
    RETAIN_IN_ARCHIVE_ON_ACK_DEFAULT,
    RETAIN_IN_ARCHIVE_ON_REJECT_DEFAULT,
    RETRY_STRATEGY_DEFAULT,
    SqlBrokerRegistrator,
)
from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig
from faststream_sqlbroker.sqlbroker.message import SqlBrokerInnerMessage
from faststream_sqlbroker.sqlbroker.retry import RetryStrategyProto

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
    )


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
        max_workers: int = MAX_WORKERS_DEFAULT,
        retry_strategy: RetryStrategyProto | None = RETRY_STRATEGY_DEFAULT,
        max_fetch_interval: float,
        min_fetch_interval: float | None = None,
        fetch_batch_size: int,
        max_not_processed_factor: float = MAX_NOT_PROCESSED_FACTOR_DEFAULT,
        max_not_persisted_factor: float = MAX_NOT_PERSISTED_FACTOR_DEFAULT,
        flush_interval: float,
        release_stuck_interval: float = RELEASE_STUCK_INTERVAL_DEFAULT,
        release_stuck_timeout: float = RELEASE_STUCK_TIMEOUT_DEFAULT,
        max_deliveries: int | None = MAX_DELIVERIES_DEFAULT,
        batch: bool = BATCH_DEFAULT,
        batch_max_records: int | None = BATCH_MAX_RECORDS_DEFAULT,
        batch_max_accumulation_timeout_factor: float = (
            BATCH_MAX_ACCUMULATION_TIMEOUT_FACTOR_DEFAULT
        ),
        ack_policy: AckPolicy = ACK_POLICY_DEFAULT,
        retain_in_archive_on_ack: bool = RETAIN_IN_ARCHIVE_ON_ACK_DEFAULT,
        retain_in_archive_on_reject: bool = RETAIN_IN_ARCHIVE_ON_REJECT_DEFAULT,
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
            queues:
                List of queue names to consume from.
            publishers: SqlBroker publishers to broadcast the handler result.
            max_workers:
                Number of concurrent handler coroutines.
            retry_strategy:
                Called to determine if and how soon a Nacked message is retried.
            max_fetch_interval:
                Maximum interval between consecutive fetches.
            min_fetch_interval:
                Minimum interval between consecutive fetches. If the last fetch was
                full (returned as many messages as the fetch's limit), the next fetch
                happens after both (i) minimum fetch interval has passed, and (ii)
                capacity equal to the fetch batch size has freed up in both the
                acquired-but-not-yet-processed and acquired-but-not-yet-persisted
                sets.
            fetch_batch_size:
                Maximum number of messages to fetch in a single batch. A fetch's
                actual limit might be lower if either the
                acquired-but-not-yet-processed or acquired-but-not-yet-persisted set
                has less free capacity.
            max_not_processed_factor:
                Multiplier for `fetch_batch_size` to cap the size of the set of
                acquired-but-not-yet-processed messages.
            max_not_persisted_factor:
                Multiplier for `fetch_batch_size` to cap the size of the set of
                acquired messages whose state has not yet been persisted to the
                database. Since this set always contains the
                acquired-but-not-yet-processed one, setting it below
                `max_not_processed_factor` makes the latter have no effect and
                emits a warning.
            flush_interval:
                Interval between flushes of processed message state to the database.
            release_stuck_interval:
                Interval between checks for stuck `PROCESSING` messages in the
                subscriber's queues.
            release_stuck_timeout:
                Interval since `acquired_at` after which a `PROCESSING` message in
                the subscriber's queues is considered stuck and is released back
                to `PENDING`.
            max_deliveries:
                Maximum number of deliveries allowed for a message for poison
                message protection. If set, messages that have reached this limit
                are Rejected without processing. Note that this might violate
                at-least-once processing semantics.
            batch:
                Call the handler once per group of messages rather than once per
                message. Requires `max_workers=1`.
            batch_max_records:
                Maximum number of messages in a single handler batch. Must not
                exceed `fetch_batch_size` multiplied by either
                `max_not_processed_factor` or `max_not_persisted_factor`.
            batch_max_accumulation_timeout_factor:
                Multiplier for `max_fetch_interval` used to determine the batch
                accumulation timeout. The effective timeout is
                `max_fetch_interval * batch_max_accumulation_timeout_factor + 5 ms`.
                The accumulation timer starts when the first message arrives.
                Messages are collected until either `batch_max_records` is reached
                or the timer expires.
            ack_policy:
                `AckPolicy` that controls acknowledgement behavior.
            retain_in_archive_on_ack:
                Acked messages, in addition to being removed from the primary table,
                are also persisted in the archive table. Requires the broker to
                define an archive table (`message_archive_table_name`).
            retain_in_archive_on_reject:
                Rejected messages, in addition to being removed from the primary
                table, are also persisted in the archive table, where they serve as
                a dead-letter queue. Requires the broker to define an archive table
                (`message_archive_table_name`).
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
            max_not_processed_factor=max_not_processed_factor,
            max_not_persisted_factor=max_not_persisted_factor,
            flush_interval=flush_interval,
            release_stuck_interval=release_stuck_interval,
            release_stuck_timeout=release_stuck_timeout,
            max_deliveries=max_deliveries,
            batch=batch,
            batch_max_records=batch_max_records,
            batch_max_accumulation_timeout_factor=batch_max_accumulation_timeout_factor,
            ack_policy=ack_policy,
            retain_in_archive_on_ack=retain_in_archive_on_ack,
            retain_in_archive_on_reject=retain_in_archive_on_reject,
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
