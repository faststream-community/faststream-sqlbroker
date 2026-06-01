from pathlib import Path as _Path

import pytest

pytest.importorskip("sqlalchemy")

# Merge upstream faststream's tests/ (vendored as a git submodule at
# tests/faststream) into this package's __path__, so e.g.
# `from tests.brokers.base.consume import BrokerRealConsumeTestcase`
# resolves Testcase mixins defined in the upstream repo. Our subpackages
# (tests.base, tests.infra) and upstream's (tests.brokers, ...) don't collide.
_upstream_tests = _Path(__file__).parent / "faststream" / "tests"
if _upstream_tests.is_dir():
    __path__.append(str(_upstream_tests))
