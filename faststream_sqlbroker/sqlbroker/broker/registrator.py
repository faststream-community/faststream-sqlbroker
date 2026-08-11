from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Optional, cast

from faststream import AckPolicy
from faststream._internal.broker.registrator import Registrator
from typing_extensions import override

from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig
from faststream_sqlbroker.sqlbroker.message import SqlBrokerInnerMessage
from faststream_sqlbroker.sqlbroker.publisher.factory import create_publisher
from faststream_sqlbroker.sqlbroker.retry import NoRetryStrategy, RetryStrategyProto
from faststream_sqlbroker.sqlbroker.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from faststream._internal.types import CustomCallable

    from faststream_sqlbroker.sqlbroker.publisher.usecase import LogicPublisher
    from faststream_sqlbroker.sqlbroker.subscriber.usecase import SqlBrokerSubscriber

MAX_WORKERS_DEFAULT = 1
RETRY_STRATEGY_DEFAULT: RetryStrategyProto = NoRetryStrategy()
MAX_NOT_PROCESSED_FACTOR_DEFAULT = 1.5
MAX_NOT_PERSISTED_FACTOR_DEFAULT = 2.0
RELEASE_STUCK_INTERVAL_DEFAULT = 60
RELEASE_STUCK_TIMEOUT_DEFAULT = 60 * 10
MAX_DELIVERIES_DEFAULT: int | None = None
BATCH_DEFAULT = False
BATCH_MAX_RECORDS_DEFAULT: int | None = None
BATCH_MAX_ACCUMULATION_TIMEOUT_FACTOR_DEFAULT = 0
ACK_POLICY_DEFAULT = AckPolicy.REJECT_ON_ERROR
RETAIN_IN_ARCHIVE_ON_ACK_DEFAULT = True
RETAIN_IN_ARCHIVE_ON_REJECT_DEFAULT = True


class SqlBrokerRegistrator(Registrator[SqlBrokerInnerMessage, SqlBrokerConfig]):
    @override
    def subscriber(  # type: ignore[override]
        self,
        queues: list[str],
        *,
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
            `max_not_processed_factor` makes the latter have no effect and emits
            a warning.
        flush_interval:
            Interval between flushes of processed message state to the database.
        release_stuck_interval:
            Interval between checks for stuck `PROCESSING` messages in the
            subscriber's queues.
        release_stuck_timeout:
            Interval since `acquired_at` after which a `PROCESSING` message in
            the subscriber's queues is considered stuck and is released back to
            `PENDING`.
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
        """
        subscriber = create_subscriber(
            queues=queues,
            max_workers=max_workers,
            retry_strategy=retry_strategy,
            max_fetch_interval=max_fetch_interval,
            min_fetch_interval=(
                max_fetch_interval if min_fetch_interval is None else min_fetch_interval
            ),
            fetch_batch_size=fetch_batch_size,
            max_not_processed_factor=max_not_processed_factor,
            max_not_persisted_factor=max_not_persisted_factor,
            flush_interval=flush_interval,
            release_stuck_interval=release_stuck_interval,
            release_stuck_timeout=release_stuck_timeout,
            max_deliveries=max_deliveries,
            config=cast("SqlBrokerConfig", self.config),
            ack_policy=ack_policy,
            retain_in_archive_on_ack=retain_in_archive_on_ack,
            retain_in_archive_on_reject=retain_in_archive_on_reject,
            batch=batch,
            batch_max_records=batch_max_records,
            batch_max_accumulation_timeout_factor=batch_max_accumulation_timeout_factor,
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
