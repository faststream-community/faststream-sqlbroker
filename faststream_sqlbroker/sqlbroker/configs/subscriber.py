import warnings
from dataclasses import dataclass, field

from faststream import AckPolicy
from faststream._internal.configs.endpoint import SubscriberUsecaseConfig
from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError

from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig
from faststream_sqlbroker.sqlbroker.retry import NoRetryStrategy, RetryStrategyProto


@dataclass(kw_only=True)
class SqlBrokerSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "SqlBrokerConfig" = field(default_factory=SqlBrokerConfig)

    queues: list[str]
    max_workers: int
    retry_strategy: RetryStrategyProto | None
    max_fetch_interval: float
    min_fetch_interval: float
    fetch_batch_size: int
    max_not_processed_factor: float
    max_not_persisted_factor: float
    flush_interval: float
    release_stuck_interval: float
    release_stuck_timeout: float
    max_deliveries: int | None
    batch: bool
    batch_max_records: int
    batch_max_accumulation_timeout_factor: float = 0
    _batch_max_records_explicit: bool = False
    retain_in_archive_on_ack: bool = True
    retain_in_archive_on_reject: bool = True

    def validate(self) -> None:
        if (
            self.retain_in_archive_on_ack or self.retain_in_archive_on_reject
        ) and self._outer_config.message_archive_table_name is None:
            msg = (
                "retain_in_archive_on_ack and retain_in_archive_on_reject require an "
                "archive table, but the broker was configured without one "
                "(message_archive_table_name=None). Either set "
                "message_archive_table_name on the broker or disable both retain options "
                "on the subscriber."
            )
            raise SetupError(msg)

        if self.batch and self.max_workers != 1:
            msg = "batch=True requires max_workers=1"
            raise SetupError(msg)

        if self.batch and not (
            self.batch_max_records
            <= self.fetch_batch_size * self.max_not_processed_factor
        ):
            msg = (
                "batch_max_records must be less than or equal to "
                "fetch_batch_size * max_not_processed_factor"
            )
            raise SetupError(msg)

        if self.batch and not (
            self.batch_max_records
            <= self.fetch_batch_size * self.max_not_persisted_factor
        ):
            msg = (
                "batch_max_records must be less than or equal to "
                "fetch_batch_size * max_not_persisted_factor"
            )
            raise SetupError(msg)

        if self.max_not_persisted_factor < self.max_not_processed_factor:
            warnings.warn(
                "Be aware that max_not_persisted_factor is less than "
                "max_not_processed_factor. The acquired-but-not-yet-persisted set "
                "always contains the acquired-but-not-yet-processed one, so "
                "max_not_processed_factor has no effect in this configuration.",
                UserWarning,
                stacklevel=4,
            )

        if not self.batch and (
            self._batch_max_records_explicit
            or self.batch_max_accumulation_timeout_factor != 0
        ):
            warnings.warn(
                "Be aware that batch_max_records and "
                "batch_max_accumulation_timeout_factor are ignored when batch=False.",
                UserWarning,
                stacklevel=4,
            )

        if self.max_deliveries is not None:
            warnings.warn(
                "Be aware the setting max_deliveries violates the at-most-once "
                "processing guarantee.",
                UserWarning,
                stacklevel=4,
            )

        if (
            self.ack_policy is AckPolicy.REJECT_ON_ERROR
            and self.retry_strategy is not None
            and not isinstance(self.retry_strategy, NoRetryStrategy)
        ):
            warnings.warn(
                "Be aware that retry_strategy is ignored when AckPolicy.REJECT_ON_ERROR "
                "is used.",
                UserWarning,
                stacklevel=4,
            )

        if self.retry_strategy is None and self.ack_policy is AckPolicy.NACK_ON_ERROR:
            warnings.warn(
                "Be aware that if retry_strategy is None, AckPolicy.NACK_ON_ERROR "
                "has the same effect as AckPolicy.REJECT_ON_ERROR for this broker.",
                UserWarning,
                stacklevel=4,
            )

        if self.ack_policy is AckPolicy.ACK_FIRST:
            warnings.warn(
                "Be aware that AckPolicy.ACK_FIRST has the same effect as AckPolicy.ACK "
                "for this broker.",
                UserWarning,
                stacklevel=4,
            )

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            return AckPolicy.REJECT_ON_ERROR
        return self._ack_policy
