from dataclasses import dataclass, field

from faststream._internal.configs.endpoint import SubscriberUsecaseConfig
from faststream._internal.constants import EMPTY

from faststream import AckPolicy
from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig
from faststream_sqlbroker.sqlbroker.retry import RetryStrategyProto


@dataclass(kw_only=True)
class SqlBrokerSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "SqlBrokerConfig" = field(default_factory=SqlBrokerConfig)

    queues: list[str]
    max_workers: int
    retry_strategy: RetryStrategyProto | None
    max_fetch_interval: float
    min_fetch_interval: float
    fetch_batch_size: int
    overfetch_factor: float
    flush_interval: float
    release_stuck_interval: float
    release_stuck_timeout: float
    max_deliveries: int | None
    retain_in_archive_on_ack: bool = True
    retain_in_archive_on_reject: bool = True

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            return AckPolicy.NACK_ON_ERROR
        return self._ack_policy
