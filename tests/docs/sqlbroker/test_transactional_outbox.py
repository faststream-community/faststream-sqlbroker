import pytest

pytest.importorskip("aiokafka")

from faststream_sqlbroker import SqlBroker


def test_transactional_outbox() -> None:
    from docs.docs_src.sqlbroker.transactional_outbox import (
        broker_sqlbroker,
        handle_msg,
    )

    assert isinstance(broker_sqlbroker, SqlBroker)
    assert callable(handle_msg)
