import warnings
from typing import TYPE_CHECKING, Any

from faststream import AckPolicy
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.exceptions import SetupError

from faststream_sqlbroker.sqlbroker.configs.subscriber import SqlBrokerSubscriberConfig
from faststream_sqlbroker.sqlbroker.retry import NoRetryStrategy
from faststream_sqlbroker.sqlbroker.subscriber.specification import (
    SqlBrokerSubscriberSpecification,
)
from faststream_sqlbroker.sqlbroker.subscriber.usecase import SqlBrokerSubscriber

if TYPE_CHECKING:
    from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig
    from faststream_sqlbroker.sqlbroker.retry import RetryStrategyProto


def create_subscriber(
    queues: list[str],
    max_workers: int,
    retry_strategy: "RetryStrategyProto | None",
    max_fetch_interval: float,
    min_fetch_interval: float,
    fetch_batch_size: int,
    overfetch_factor: float,
    flush_interval: float,
    release_stuck_interval: float,
    release_stuck_timeout: float,
    max_deliveries: int | None,
    config: "SqlBrokerConfig",
    ack_policy: "AckPolicy",
    retain_in_archive_on_ack: bool,
    retain_in_archive_on_reject: bool,
) -> SqlBrokerSubscriber:
    _validate_input_for_misconfiguration(
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
        retain_in_archive_on_ack=retain_in_archive_on_ack,
        retain_in_archive_on_reject=retain_in_archive_on_reject,
        config=config,
        ack_policy=ack_policy,
    )

    subscriber_config = SqlBrokerSubscriberConfig(
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
        retain_in_archive_on_ack=retain_in_archive_on_ack,
        retain_in_archive_on_reject=retain_in_archive_on_reject,
        _outer_config=config,
        _ack_policy=ack_policy,
    )

    calls = CallsCollection[Any]()

    specification = SqlBrokerSubscriberSpecification()

    return SqlBrokerSubscriber(subscriber_config, specification, calls)  # type: ignore[arg-type]


def _validate_input_for_misconfiguration(
    queues: list[str],
    max_workers: int,
    retry_strategy: "RetryStrategyProto | None",
    max_fetch_interval: float,
    min_fetch_interval: float,
    fetch_batch_size: int,
    overfetch_factor: float,
    flush_interval: float,
    release_stuck_interval: float,
    release_stuck_timeout: float,
    max_deliveries: int | None,
    config: "SqlBrokerConfig",
    ack_policy: AckPolicy,
    retain_in_archive_on_ack: bool,
    retain_in_archive_on_reject: bool,
) -> None:
    if (
        retain_in_archive_on_ack or retain_in_archive_on_reject
    ) and config.message_archive_table_name is None:
        msg = (
            "retain_in_archive_on_ack and retain_in_archive_on_reject require an "
            "archive table, but the broker was configured without one "
            "(message_archive_table_name=None). Either set "
            "message_archive_table_name on the broker or disable both retain options "
            "on the subscriber."
        )
        raise SetupError(msg)

    if max_deliveries is not None:
        warnings.warn(
            "Be aware the setting max_deliveries violates the at-most-once "
            "processing guarantee.",
            UserWarning,
            stacklevel=4,
        )

    if (
        ack_policy is AckPolicy.REJECT_ON_ERROR
        and retry_strategy is not None
        and not isinstance(retry_strategy, NoRetryStrategy)
    ):
        warnings.warn(
            "Be aware that retry_strategy is ignored when AckPolicy.REJECT_ON_ERROR "
            "is used.",
            UserWarning,
            stacklevel=4,
        )

    if retry_strategy is None and ack_policy is AckPolicy.NACK_ON_ERROR:
        warnings.warn(
            "Be aware that if retry_strategy is None, AckPolicy.NACK_ON_ERROR "
            "has the same effect as AckPolicy.REJECT_ON_ERROR for this broker.",
            UserWarning,
            stacklevel=4,
        )

    if ack_policy is AckPolicy.ACK_FIRST:
        warnings.warn(
            "Be aware that AckPolicy.ACK_FIRST has the same effect as AckPolicy.ACK "
            "for this broker.",
            UserWarning,
            stacklevel=4,
        )
