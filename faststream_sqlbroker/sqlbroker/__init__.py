from faststream._internal.testing.app import TestApp

try:
    from .annotations import SqlBrokerBatchMessage, SqlBrokerMessage
    from .broker import SqlBroker, SqlBrokerPublisher, SqlBrokerRoute, SqlBrokerRouter
    from .response import (
        SqlBrokerPublishCommand,
        SqlBrokerPublishMessage,
        SqlBrokerResponse,
    )
    from .schema import (
        SqlBrokerCompetingConsumersSchemaVersion,
        SqlBrokerSchemaConfig,
        SqlBrokerSchemaType,
        SqlBrokerSchemaVariant,
    )

except ImportError as e:
    if "'sqlalchemy'" not in e.msg:
        raise

    from faststream_sqlbroker.sqlbroker.exceptions import INSTALL_FASTSTREAM_SqlBroker

    raise ImportError(INSTALL_FASTSTREAM_SqlBroker) from e

__all__ = (
    "SqlBroker",
    "SqlBrokerBatchMessage",
    "SqlBrokerCompetingConsumersSchemaVersion",
    "SqlBrokerMessage",
    "SqlBrokerPublishCommand",
    "SqlBrokerPublishMessage",
    "SqlBrokerPublisher",
    "SqlBrokerResponse",
    "SqlBrokerRoute",
    "SqlBrokerRouter",
    "SqlBrokerSchemaConfig",
    "SqlBrokerSchemaType",
    "SqlBrokerSchemaVariant",
    "TestApp",
)
