from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Optional, cast

from faststream._internal.broker.registrator import Registrator
from typing_extensions import override

from faststream import AckPolicy
from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig
from faststream_sqlbroker.sqlbroker.message import SqlBrokerInnerMessage
from faststream_sqlbroker.sqlbroker.publisher.factory import create_publisher
from faststream_sqlbroker.sqlbroker.retry import RetryStrategyProto
from faststream_sqlbroker.sqlbroker.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from faststream._internal.types import CustomCallable

    from faststream_sqlbroker.sqlbroker.publisher.usecase import LogicPublisher
    from faststream_sqlbroker.sqlbroker.subscriber.usecase import SqlBrokerSubscriber


class SqlBrokerRegistrator(Registrator[SqlBrokerInnerMessage, SqlBrokerConfig]):
    @override
    def subscriber(  # type: ignore[override]
        self,
        queues: list[str],
        *,
        max_workers: int = 1,
        retry_strategy: RetryStrategyProto | None = None,
        max_fetch_interval: float,
        min_fetch_interval: float,
        fetch_batch_size: int,
        overfetch_factor: float,
        flush_interval: float,
        release_stuck_interval: float,
        release_stuck_timeout: float,
        max_deliveries: int | None = None,
        ack_policy: AckPolicy = AckPolicy.REJECT_ON_ERROR,
        retain_in_archive_on_ack: bool = True,
        retain_in_archive_on_reject: bool = True,
        # broker args
        persistent: bool = True,
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        # AsyncAPI args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "SqlBrokerSubscriber":
        """Args:
        queues:
            List of queue names to consume from.
        max_workers:
            Number of concurrent handler coroutines.
        retry_strategy:
            Called to determine if and how soon a Nack'ed message is retried. If
            None, `AckPolicy.NACK_ON_ERROR` has the same effect as
            `AckPolicy.REJECT_ON_ERROR`.
        min_fetch_interval:
            Minimum interval between consecutive fetches. If the last fetch was
            full (returned as many messages as the fetch's limit), the next fetch
            happens after both (i) minimum fetch interval has passed, and (ii)
            capacity equal to the fetch batch size has freed up in the set of
            acquired-but-not-yet-processed messages.
        max_fetch_interval:
            Maximum interval between consecutive fetches.
        fetch_batch_size:
            Maximum number of messages to fetch in a single batch. A fetch's
            actual limit might be lower if the free capacity of the
            acquired-but-not-yet-processed messages set is smaller.
        overfetch_factor:
            Multiplier for `fetch_batch_size` to size the maximum size of the
            set of acquired-but-not-yet-processed messages.
        flush_interval:
            Interval between flushes of processed message state to the database.
        release_stuck_interval:
            Interval between checks for stuck `PROCESSING` messages.
        release_stuck_timeout:
            Interval since `acquired_at` after which a `PROCESSING` message is
            considered stuck and is released back to `PENDING`.
        max_deliveries:
            Maximum number of deliveries allowed for a message. If set, messages
            that have reached this limit are Reject'ed to `FAILED` without
            processing. Note that this might violate the at-least-once processing
            semantics.
        ack_policy:
            `AckPolicy` that controls acknowledgement behavior.
        """
        subscriber = create_subscriber(
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
            config=cast("SqlBrokerConfig", self.config),
            ack_policy=ack_policy,
            retain_in_archive_on_ack=retain_in_archive_on_ack,
            retain_in_archive_on_reject=retain_in_archive_on_reject,
        )

        super().subscriber(subscriber, persistent=persistent)

        subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
            dependencies_=dependencies,
        )

        return subscriber

    @override
    def publisher(  # type: ignore[override]
        self,
        queue: str = "",
        *,
        headers: dict[str, str] | None = None,
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "LogicPublisher":
        publisher = create_publisher(
            queue=queue,
            headers=headers,
            # Specific
            broker_config=cast("SqlBrokerConfig", self.config),
            # AsyncAPI
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=include_in_schema,
        )

        super().publisher(publisher)

        return publisher
