from faststream import FastStream

from faststream_sqlbroker import SqlBroker


def test_acknowledgements() -> None:
    from docs.docs_src.sqlbroker.acknowledgements import (
        app,
        automatic_handler,
        broker,
        manual_handler,
    )

    assert isinstance(app, FastStream)
    assert isinstance(broker, SqlBroker)
    assert callable(automatic_handler)
    assert callable(manual_handler)
