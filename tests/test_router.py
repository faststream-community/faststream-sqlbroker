import pytest

from faststream_sqlbroker.sqlbroker.broker.router import (
    SqlBrokerPublisher,
    SqlBrokerRoute,
)
from tests.brokers.base.router import RouterTestcase

from .basic import SqlBrokerTestcaseConfig


@pytest.mark.connected()
@pytest.mark.slow()
class TestRouter(SqlBrokerTestcaseConfig, RouterTestcase):
    route_class = SqlBrokerRoute
    publisher_class = SqlBrokerPublisher

    async def test_get_one_respect_decoder(self) -> None: ...

    async def test_iterator_respect_decoder(self) -> None: ...
