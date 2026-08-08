import enum
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from faststream.message.message import StreamMessage

from faststream_sqlbroker.sqlbroker.retry import RetryStrategyProto

if TYPE_CHECKING:
    from faststream._internal.basic_types import LoggerProto


class SqlBrokerMessageState(str, enum.Enum):
    """The message starts out as PENDING. When it is acquired by a worker, it is marked as
    PROCESSING. After being acquired, depending on processing result, AckPolicy, retry
    strategy, and presence of manual acknowledgement, the message can be marked as
    COMPLETED, FAILED, or RETRYABLE prior to or after a processing attempt. A message
    that is COMPLETED or FAILED is archived and will not be processed again. A RETRYABLE
    message might be retried.
    """

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYABLE = "retryable"


class SqlBrokerInnerMessage:
    def __init__(
        self,
        id: int,
        queue: str,
        state: SqlBrokerMessageState,
        headers: dict[str, Any] | None,
        payload: bytes,
        attempts_count: int,
        deliveries_count: int,
        created_at: datetime,
        first_attempt_at: datetime | None,
        next_attempt_at: datetime | None,
        last_attempt_at: datetime | None,
        acquired_at: datetime | None,
        retry_strategy: RetryStrategyProto | None = None,
    ) -> None:
        self.id = id
        self.queue = queue
        self.state = state
        self.headers = headers if headers is not None else {}
        self.payload = payload
        self.attempts_count = attempts_count
        self.deliveries_count = deliveries_count
        self.created_at = created_at
        self.first_attempt_at = first_attempt_at
        self.next_attempt_at = next_attempt_at
        self.last_attempt_at = last_attempt_at
        self.acquired_at = acquired_at

        self.retry_strategy = retry_strategy

        self._state_set = False

    def ack(self) -> None:
        self._update_state_if_not_set(self._ack)

    def nack(self) -> None:
        self._update_state_if_not_set(self._nack)

    def reject(self) -> None:
        self._update_state_if_not_set(self._reject)

    def requeue_from_fetched(self) -> None:
        self._requeue_from_fetched()

    def requeue_from_attempted(self) -> None:
        self._requeue_from_attempted()

    def _update_state_if_not_set(
        self,
        update_method: Callable[[], None],
    ) -> None:
        if self._state_set:
            return

        self._record_attempt()
        update_method()

        self._state_set = True

    def _ack(self) -> None:
        self.state = SqlBrokerMessageState.COMPLETED
        self.acquired_at = None

    def _nack(self) -> None:
        if self.retry_strategy is None or not (
            next_attempt_at := self.retry_strategy.get_next_attempt_at(
                first_attempt_at=cast("datetime", self.first_attempt_at),
                last_attempt_at=cast("datetime", self.last_attempt_at),
                attempts_count=self.attempts_count,
            )
        ):
            self._reject()
        else:
            self.state = SqlBrokerMessageState.RETRYABLE
            self.next_attempt_at = next_attempt_at
            self.acquired_at = None

    def _reject(self) -> None:
        self.state = SqlBrokerMessageState.FAILED
        self.acquired_at = None

    def _requeue_from_fetched(self) -> None:
        self.state = SqlBrokerMessageState.PENDING
        self.deliveries_count -= 1
        self.acquired_at = None

    def _requeue_from_attempted(self) -> None:
        self.state = SqlBrokerMessageState.PENDING
        self.acquired_at = None

    def _record_attempt(self) -> None:
        self.attempts_count += 1
        self.last_attempt_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)
        if self.first_attempt_at is None:
            self.first_attempt_at = self.last_attempt_at

    def _allow_delivery(
        self,
        *,
        max_deliveries: int | None,
        logger: "LoggerProto | None",
    ) -> bool:
        if max_deliveries is not None and self.deliveries_count > max_deliveries:
            self._reject()
            if logger:
                logger.log(
                    logging.ERROR,
                    f"Message delivery limit was exceeded for message {self} "
                    f"and the message was rejected.",
                )
            return False
        return True

    async def _assert_state_updated(self, logger: "LoggerProto | None") -> None:
        if not self._state_set:
            if logger:
                logger.log(
                    logging.ERROR,
                    f"State of message {self} was not updated after processing, "
                    f"perhaps due to the AckPolicy.MANUAL policy and lack of manual "
                    f"acknowledgement in the handler. As a precaution, the message "
                    f"was Rejected.",
                )
            self.reject()

    def __repr__(self) -> str:
        return f"SqlBrokerMessage(id={self.id}, queue={self.queue})"


class SqlBrokerMessage(StreamMessage[SqlBrokerInnerMessage]):
    async def ack(self) -> None:
        self.raw_message.ack()
        await super().ack()

    async def nack(self) -> None:
        self.raw_message.nack()
        await super().nack()

    async def reject(self) -> None:
        self.raw_message.reject()
        await super().reject()


class SqlBrokerBatchMessage(StreamMessage[tuple[SqlBrokerInnerMessage, ...]]):
    messages: list[SqlBrokerMessage]

    def __init__(
        self,
        *args: Any,
        messages: list[SqlBrokerMessage],
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.messages = messages

    async def ack(self) -> None:
        for message in self.messages:
            await message.ack()
        await super().ack()

    async def nack(self) -> None:
        for message in self.messages:
            await message.nack()
        await super().nack()

    async def reject(self) -> None:
        for message in self.messages:
            await message.reject()
        await super().reject()
