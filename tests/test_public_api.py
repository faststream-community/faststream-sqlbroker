from faststream_sqlbroker import (
    ExponentialBackoffRetryStrategy,
    SqlBroker,
    SqlBrokerCompetingConsumersSchemaVersion,
    SqlBrokerSchemaConfig,
    SqlBrokerSchemaType,
    SqlBrokerSchemaVariant,
)


def test_top_level_exports() -> None:
    assert SqlBroker.__name__ == "SqlBroker"
    assert ExponentialBackoffRetryStrategy.__name__ == "ExponentialBackoffRetryStrategy"
    assert SqlBrokerSchemaVariant.COMPETING_CONSUMERS.value == "competing_consumers"
    assert SqlBrokerSchemaType.MESSAGE.value == "message"
    assert SqlBrokerSchemaConfig().version is SqlBrokerCompetingConsumersSchemaVersion.V1
