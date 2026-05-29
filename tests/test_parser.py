import pytest

from tests.brokers.base.parser import CustomParserTestcase

from .basic import SqlBrokerTestcaseConfig


@pytest.mark.connected()
@pytest.mark.slow()
class TestCustomParser(SqlBrokerTestcaseConfig, CustomParserTestcase):
    async def test_iterator_respect_decoder(self): ...

    async def test_get_one_respect_decoder(self): ...
