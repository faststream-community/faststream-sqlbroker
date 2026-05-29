from dataclasses import dataclass, field

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)

from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig


@dataclass(kw_only=True)
class SqlBrokerPublisherSpecificationConfig(PublisherSpecificationConfig):
    queue: str


@dataclass(kw_only=True)
class SqlBrokerPublisherConfig(PublisherUsecaseConfig):
    queue: str = ""
    headers: dict[str, str] | None = None
    _outer_config: "SqlBrokerConfig" = field(default_factory=SqlBrokerConfig)
