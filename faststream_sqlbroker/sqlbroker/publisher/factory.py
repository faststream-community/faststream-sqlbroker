from typing import TYPE_CHECKING, Any

from .config import SqlBrokerPublisherConfig, SqlBrokerPublisherSpecificationConfig
from .specification import SqlBrokerPublisherSpecification
from .usecase import LogicPublisher

if TYPE_CHECKING:
    from faststream_sqlbroker.sqlbroker.configs.broker import SqlBrokerConfig


def create_publisher(
    *,
    queue: str,
    headers: dict[str, str] | None,
    # Publisher args
    broker_config: "SqlBrokerConfig",
    # AsyncAPI args
    schema_: Any | None,
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> LogicPublisher:
    publisher_config = SqlBrokerPublisherConfig(
        queue=queue,
        headers=headers,
        _outer_config=broker_config,
    )

    specification = SqlBrokerPublisherSpecification(
        _outer_config=broker_config,
        specification_config=SqlBrokerPublisherSpecificationConfig(
            queue=queue,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )

    return LogicPublisher(publisher_config, specification)
