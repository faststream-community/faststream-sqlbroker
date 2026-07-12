from faststream_sqlbroker import SqlBroker


def test_in_broker_observability() -> None:
    from docs.docs_src.sqlbroker.observability_in_broker import (
        broker,
        engine,
        metrics_app,
    )

    assert isinstance(broker, SqlBroker)
    assert engine.dialect.name == "postgresql"
    assert callable(metrics_app)
