from faststream_sqlbroker.sqlbroker import SqlBroker


def test_subscribe() -> None:
    from docs.docs_src.sqlbroker.subscribe import broker, handler

    assert isinstance(broker, SqlBroker)
    assert callable(handler)
