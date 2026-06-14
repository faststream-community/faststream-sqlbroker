from faststream_sqlbroker import (
    ExponentialBackoffRetryStrategy,
    SqlBroker,
    SqlBrokerSchemaConfig,
    SqlBrokerSchemaType,
    SqlBrokerSchemaVariant,
    SqlBrokerWorkQueueSchemaVersion,
)


def test_top_level_exports() -> None:
    assert SqlBroker.__name__ == "SqlBroker"
    assert ExponentialBackoffRetryStrategy.__name__ == "ExponentialBackoffRetryStrategy"
    assert SqlBrokerSchemaVariant.WORK_QUEUE.value == "work_queue"
    assert SqlBrokerSchemaType.MESSAGE.value == "message"
    assert SqlBrokerSchemaConfig().version is SqlBrokerWorkQueueSchemaVersion.V1
