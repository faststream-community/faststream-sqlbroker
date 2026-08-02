import typing

from faststream_sqlbroker import SqlBroker


def test_batch() -> None:
    from docs.docs_src.sqlbroker.batch import MyModel, broker, handler

    assert isinstance(broker, SqlBroker)
    assert callable(handler)
    # The first argument is a list of decoded payloads, so the annotation must
    # be the model class itself rather than a Context-carrying annotation.
    hints = typing.get_type_hints(handler._original_call)
    assert hints["bodies"] == list[MyModel]
