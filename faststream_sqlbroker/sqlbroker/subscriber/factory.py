from typing import TYPE_CHECKING

from faststream._internal.endpoint.subscriber.call_item import CallsCollection

from faststream_sqlbroker.sqlbroker.configs.subscriber import SqlBrokerSubscriberConfig
from faststream_sqlbroker.sqlbroker.message import SqlBrokerInnerMessage
from faststream_sqlbroker.sqlbroker.subscriber.specification import (
    SqlBrokerSubscriberSpecification,
)
from faststream_sqlbroker.sqlbroker.subscriber.usecase import (
    SqlBrokerBatchSubscriber,
    SqlBrokerSubscriber,
)

if TYPE_CHECKING:
    from faststream import AckPolicy

    from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig
    from faststream_sqlbroker.sqlbroker.retry import RetryStrategyProto


def create_subscriber(
    queues: list[str],
    max_workers: int,
    retry_strategy: "RetryStrategyProto | None",
    max_fetch_interval: float,
    min_fetch_interval: float,
    fetch_batch_size: int,
    max_not_processed_factor: float,
    max_not_persisted_factor: float,
    flush_interval: float,
    release_stuck_interval: float,
    release_stuck_timeout: float,
    max_deliveries: int | None,
    config: "SqlBrokerConfig",
    ack_policy: "AckPolicy",
    retain_in_archive_on_ack: bool,
    retain_in_archive_on_reject: bool,
    batch: bool = False,
    batch_max_records: int | None = None,
    batch_max_accumulation_timeout_factor: float = 0,
) -> SqlBrokerSubscriber:
    subscriber_config = SqlBrokerSubscriberConfig(
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
        batch_max_records=(
            batch_max_records if batch_max_records is not None else fetch_batch_size
        ),
        batch_max_accumulation_timeout_factor=batch_max_accumulation_timeout_factor,
        _batch_max_records_explicit=batch_max_records is not None,
        retain_in_archive_on_ack=retain_in_archive_on_ack,
        retain_in_archive_on_reject=retain_in_archive_on_reject,
        _outer_config=config,
        _ack_policy=ack_policy,
    )
    subscriber_config.validate()

    specification = SqlBrokerSubscriberSpecification()

    if batch:
        batch_calls = CallsCollection[tuple[SqlBrokerInnerMessage, ...]]()
        return SqlBrokerBatchSubscriber(
            subscriber_config,
            specification,  # type: ignore[arg-type]
            batch_calls,
        )

    single_calls = CallsCollection[SqlBrokerInnerMessage]()
    return SqlBrokerSubscriber(
        subscriber_config,
        specification,  # type: ignore[arg-type]
        single_calls,
    )
