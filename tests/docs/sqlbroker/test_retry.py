from datetime import datetime, timezone

from faststream_sqlbroker.sqlbroker.retry import (
    ConstantRetryStrategy,
    ConstantWithJitterRetryStrategy,
    ExponentialBackoffRetryStrategy,
    ExponentialBackoffWithJitterRetryStrategy,
    LinearRetryStrategy,
    NoRetryStrategy,
)


def test_retry() -> None:
    from docs.docs_src.sqlbroker.retry import (
        constant,
        constant_jitter,
        exponential,
        exponential_jitter,
        linear,
        no_retry,
    )

    assert isinstance(constant, ConstantRetryStrategy)
    assert isinstance(linear, LinearRetryStrategy)
    assert isinstance(exponential, ExponentialBackoffRetryStrategy)
    assert isinstance(exponential_jitter, ExponentialBackoffWithJitterRetryStrategy)
    assert isinstance(constant_jitter, ConstantWithJitterRetryStrategy)
    assert isinstance(no_retry, NoRetryStrategy)

    now = datetime.now(timezone.utc)
    for strategy in [constant, linear, exponential, exponential_jitter, constant_jitter]:
        assert (
            strategy.get_next_attempt_at(
                first_attempt_at=now, last_attempt_at=now, attempts_count=1
            )
            is not None
        )
