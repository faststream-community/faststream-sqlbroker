from faststream_sqlbroker import SqlBroker


def test_broker() -> None:
    from docs.docs_src.sqlbroker.broker import broker, engine

    assert isinstance(broker, SqlBroker)
    assert engine.dialect.name == "postgresql"
