import asyncio
import contextlib
import copy
import logging
import time
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Coroutine,
    Iterable,
)
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any, Optional, TypeVar, cast

from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream.exceptions import FeatureNotSupportedException, StopConsume
from typing_extensions import override

from faststream_sqlbroker.sqlbroker.client import SqlBrokerBaseClient
from faststream_sqlbroker.sqlbroker.message import (
    SqlBrokerInnerMessage,
    SqlBrokerMessageState,
)
from faststream_sqlbroker.sqlbroker.parser import SqlBrokerParser

if TYPE_CHECKING:
    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.endpoint.publisher.proto import PublisherProto
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream._internal.endpoint.subscriber.specification import (
        SubscriberSpecification,
    )
    from faststream.message import StreamMessage

    from faststream_sqlbroker.sqlbroker.configs.subscriber import (
        SqlBrokerSubscriberConfig,
    )

_CoroutineReturnType = TypeVar("_CoroutineReturnType")


class SqlBrokerSubscriber(TasksMixin, SubscriberUsecase[SqlBrokerInnerMessage]):
    def __init__(
        self,
        config: "SqlBrokerSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[SqlBrokerInnerMessage]",
    ) -> None:
        self.parser = SqlBrokerParser()
        self.config = config
        config.parser = self.parser.parse_message
        config.decoder = self.parser.decode_message
        super().__init__(config, specification, calls)

        self._worker_count = config.max_workers
        self._retry_strategy = config.retry_strategy

        self._max_fetch_interval = config.max_fetch_interval
        self._min_fetch_interval = config.min_fetch_interval
        self._release_stuck_interval = config.release_stuck_interval
        self._flush_interval = config.flush_interval

        self._fetch_batch_size = config.fetch_batch_size
        self._max_not_processed = int(
            config.fetch_batch_size * config.max_not_processed_factor
        )
        self._max_not_persisted = int(
            config.fetch_batch_size * config.max_not_persisted_factor
        )
        self._not_processed_count = 0
        self._not_persisted_count = 0
        self.graceful_timeout = self._outer_config.graceful_timeout
        self._release_stuck_timeout = config.release_stuck_timeout
        self._max_deliveries = config.max_deliveries

        self._retain_in_archive_on_ack = config.retain_in_archive_on_ack
        self._retain_in_archive_on_reject = config.retain_in_archive_on_reject

        self._pending_consume_queue = asyncio.Queue[SqlBrokerInnerMessage]()
        self._result_buffer: list[SqlBrokerInnerMessage] = []
        self._stop_event = asyncio.Event()
        self._may_fetch_event = asyncio.Event()
        self._retry_on_client_error_delay = 5

        self._tasks: list[asyncio.Task[Any]] = []
        self._referenced_tasks: list[asyncio.Task[Any]] = []

    @property
    def _client(self) -> SqlBrokerBaseClient:
        return cast("SqlBrokerBaseClient", self.config._outer_config.client)

    @property
    def _queues(self) -> list[str]:
        return [f"{self._outer_config.prefix}{q}" for q in self.config.queues]

    @property
    def _logger(self) -> "LoggerProto | None":
        return self._outer_config.logger.logger.logger

    async def start(self) -> None:
        self._stop_event.clear()
        self._may_fetch_event.clear()
        self._not_processed_count = 0
        self._not_persisted_count = 0
        self._result_buffer.clear()
        self._pending_consume_queue = asyncio.Queue[SqlBrokerInnerMessage]()
        self._tasks.clear()

        for _ in range(self._worker_count):
            self.add_task(self._worker_loop)

        self._post_start()

        for loop in [self._fetch_loop, self._flush_loop, self._release_stuck_loop]:
            self.add_task(loop)

        self._stop_task = asyncio.create_task(self._stop_event.wait())

        await super().start()

    async def stop(self) -> None:
        self._stop_event.set()

        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                asyncio.gather(*(self._tasks + self.tasks), return_exceptions=True),
                timeout=self.graceful_timeout,
            )

        try:
            await self._flush_results()
        except Exception as exc:
            self._log(logging.ERROR, "SqlBrokerClient error", exc_info=exc)

        await super().stop()

    @override
    async def consume(self, msg: SqlBrokerInnerMessage) -> Any:
        # copied from parent except
        # `await self.stop()` was changed to `asyncio.create_task(self.stop())`
        if not self.running:
            return None

        try:
            return await self.process_message(msg)

        except StopConsume:
            self._referenced_tasks.append(asyncio.create_task(self.stop()))

        except SystemExit:
            self._referenced_tasks.append(asyncio.create_task(self.stop()))

            if app := self._outer_config.fd_config.context.get("app"):
                app.exit()

        except Exception:  # nosec B110
            pass

    @asynccontextmanager
    async def _task_context(
        self,
        coro: Coroutine[Any, Any, _CoroutineReturnType],
    ) -> AsyncGenerator[asyncio.Task[_CoroutineReturnType], None]:
        task = asyncio.create_task(coro)
        try:
            yield task
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    @property
    def _free_slots(self) -> int:
        return min(
            self._max_not_processed - self._not_processed_count,
            self._max_not_persisted - self._not_persisted_count,
        )

    def _check_if_may_fetch_eagerly(self) -> None:
        if self._free_slots >= self._fetch_batch_size:
            self._may_fetch_event.set()

    async def _fetch_loop(self) -> None:
        try:
            while True:
                if self._stop_event.is_set():
                    break
                self._may_fetch_event.clear()

                if self._free_slots > 0:
                    limit = min(self._fetch_batch_size, self._free_slots)

                    try:
                        batch = await self._client.fetch(self._queues, limit=limit)
                    except Exception as exc:
                        self._log(logging.ERROR, "SqlBrokerClient error", exc_info=exc)
                        await self._sleep_until_stop_event(
                            self._retry_on_client_error_delay
                        )
                        continue

                    for msg in batch:
                        self._not_processed_count += 1
                        self._not_persisted_count += 1
                        await self._pending_consume_queue.put(msg)

                    if not (_last_fetch_was_full := (len(batch) == limit)):
                        await self._sleep_until_stop_event(self._max_fetch_interval)
                        continue

                    self._check_if_may_fetch_eagerly()

                async with self._task_context(
                    asyncio.sleep(self._min_fetch_interval)
                ) as min_fetch_interval_reached_task:
                    match await self._wait_for_first_event_or_timeout(
                        self._may_fetch_event,
                        self._stop_event,
                        timeout=self._max_fetch_interval,
                    ):
                        case self._may_fetch_event:
                            await self._wait_until_stop_event(
                                min_fetch_interval_reached_task
                            )
                            continue
                        case self._stop_event | None:
                            continue
                        case _:
                            raise ValueError

        finally:
            self._requeue_pending_consume_queue()

    async def _worker_loop(self) -> None:
        while True:
            message, _ = await self._wait_until_stop_event(
                self._pending_consume_queue.get()
            )
            if not message:
                break

            try:
                if message._allow_delivery(
                    max_deliveries=self._max_deliveries,
                    logger=self._logger,
                ):
                    message.retry_strategy = self._retry_strategy
                    await self.consume(message)

            except asyncio.CancelledError:
                message.requeue_from_attempted()
                raise

            finally:
                message._reject_if_state_not_set(self._logger)

                self._not_processed_count -= 1
                self._check_if_may_fetch_eagerly()

                self._buffer_results(message)
                self._pending_consume_queue.task_done()

    async def _flush_loop(self) -> None:
        while True:
            await self._sleep_until_stop_event(self._flush_interval)

            if self._stop_event.is_set():
                break

            try:
                await self._flush_results()
            except Exception as exc:
                self._log(logging.ERROR, "SqlBrokerClient error", exc_info=exc)
                await self._sleep_until_stop_event(self._retry_on_client_error_delay)

    async def _release_stuck_loop(self) -> None:
        while True:
            if self._stop_event.is_set():
                break

            try:
                await self._client.release_stuck(
                    self._queues, timeout=self._release_stuck_timeout
                )
            except Exception as exc:
                self._log(logging.ERROR, "SqlBrokerClient error", exc_info=exc)
                await self._sleep_until_stop_event(self._retry_on_client_error_delay)
                continue

            await self._sleep_until_stop_event(self._release_stuck_interval)

    def _buffer_results(
        self, result: SqlBrokerInnerMessage | Iterable[SqlBrokerInnerMessage]
    ) -> None:
        if isinstance(result, Iterable):
            self._result_buffer.extend(result)
        else:
            self._result_buffer.append(result)

    def _pop_from_result_buffer(self) -> list[SqlBrokerInnerMessage]:
        # keep this sync
        messages = copy.copy(self._result_buffer)
        self._result_buffer.clear()
        return messages

    async def _flush_results(self) -> None:
        if not (messages := self._pop_from_result_buffer()):
            return

        to_update_in_primary, to_save_in_archive, to_delete_from_primary = [], [], []
        for message in messages:
            if (
                self._retain_in_archive_on_ack
                and message.state == SqlBrokerMessageState.COMPLETED
            ) or (
                self._retain_in_archive_on_reject
                and message.state == SqlBrokerMessageState.FAILED
            ):
                to_save_in_archive.append(message)
            if message.state in {
                SqlBrokerMessageState.COMPLETED,
                SqlBrokerMessageState.FAILED,
            }:
                to_delete_from_primary.append(message)
            if message.state in {
                SqlBrokerMessageState.PENDING,
                SqlBrokerMessageState.RETRYABLE,
            }:
                to_update_in_primary.append(message)

        try:
            await self._client.retry(to_update_in_primary)
            self._not_persisted_count -= len(to_update_in_primary)
            self._check_if_may_fetch_eagerly()
        except:
            self._buffer_results(messages)
            raise

        try:
            await self._client.archive(to_save_in_archive, to_delete_from_primary)
            self._not_persisted_count -= len(to_delete_from_primary)
            self._check_if_may_fetch_eagerly()
        except:
            self._buffer_results(to_delete_from_primary)
            raise

    def _requeue_pending_consume_queue(self) -> None:
        while True:
            try:
                message = self._pending_consume_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            message.requeue_from_fetched()
            self._not_processed_count -= 1
            self._buffer_results(message)
            self._pending_consume_queue.task_done()

    async def _wait_until_stop_event(
        self,
        awaitable: asyncio.Task[_CoroutineReturnType]
        | Coroutine[Any, Any, _CoroutineReturnType],
        timeout: float | None = None,
    ) -> tuple[_CoroutineReturnType | None, bool]:
        match awaitable:
            case asyncio.Task():
                coro_task: asyncio.Task[_CoroutineReturnType] = awaitable
            case Coroutine():
                coro_task = asyncio.create_task(awaitable)

        done, _ = await asyncio.wait(
            [coro_task, self._stop_task],
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )

        if not done:
            coro_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await coro_task
            return None, self._stop_event.is_set()

        if coro_task in done:
            return (await coro_task, self._stop_event.is_set())

        if self._stop_task in done:
            coro_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await coro_task
            return None, True

        raise ValueError

    async def _sleep_until_stop_event(self, timeout: float) -> None:
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=timeout)

    async def _wait_for_first_event_or_timeout(
        self,
        *events: asyncio.Event,
        timeout: float | None = None,
    ) -> asyncio.Event | None:
        for event in events:
            if event.is_set():
                return event

        task_to_event: dict[asyncio.Task[bool], asyncio.Event] = {
            asyncio.create_task(event.wait()): event for event in events
        }

        try:
            done, _ = await asyncio.wait(
                task_to_event.keys(),
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not done:
                return None

            finished_task = next(iter(done))
            return task_to_event[finished_task]
        finally:
            for task in task_to_event:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*task_to_event.keys(), return_exceptions=True)

    async def get_one(
        self,
        *,
        timeout: float = 5,
    ) -> Optional["StreamMessage[SqlBrokerInnerMessage]"]:
        msg = "SqlBroker doesn't support `get_one`."
        raise FeatureNotSupportedException(msg)

    async def __aiter__(self) -> AsyncIterator["StreamMessage[SqlBrokerInnerMessage]"]:  # type: ignore[override]
        msg = "SqlBroker doesn't support async iteration."
        raise FeatureNotSupportedException(msg)
        yield  # pragma: no cover

    def _make_response_publisher(
        self,
        message: "StreamMessage[SqlBrokerInnerMessage]",
    ) -> Iterable["PublisherProto"]:
        return ()


class SqlBrokerBatchSubscriber(SqlBrokerSubscriber):
    def __init__(
        self,
        config: "SqlBrokerSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        super().__init__(config, specification, calls)
        self._setup_batch_parser(config)
        self._batch_max_records = config.batch_max_records
        self._batch_timeout = (
            config.max_fetch_interval * config.batch_max_accumulation_timeout_factor
            + 0.005
        )

    def _setup_batch_parser(self, config: "SqlBrokerSubscriberConfig") -> None:
        config.parser = self.parser.parse_batch
        config.decoder = self.parser.decode_batch
        self._parser = config.parser
        self._decoder = config.decoder

    async def _gather_batch(self) -> list[SqlBrokerInnerMessage]:
        batch = []
        message, stopped = await self._wait_until_stop_event(
            self._pending_consume_queue.get()
        )
        if message:
            batch.append(message)
        if stopped:
            return batch

        deadline = time.monotonic() + self._batch_timeout

        while (
            len(batch) < self._batch_max_records
            and (remaining := (deadline - time.monotonic())) > 0
        ):
            try:
                batch.append(self._pending_consume_queue.get_nowait())
                continue

            except asyncio.QueueEmpty:
                message, stopped = await self._wait_until_stop_event(
                    self._pending_consume_queue.get(),
                    timeout=remaining,
                )
                if message:
                    batch.append(message)
                if stopped:
                    return batch

        return batch

    def _requeue_batch(self, batch: list[SqlBrokerInnerMessage]) -> None:
        while batch:
            message = batch.pop()
            message.requeue_from_fetched()
            self._not_processed_count -= 1
            self._buffer_results(message)
            self._pending_consume_queue.task_done()

    @override
    async def _worker_loop(self) -> None:
        while True:
            batch = await self._gather_batch()
            if self._stop_event.is_set():
                self._requeue_batch(batch)
                break

            to_process: list[SqlBrokerInnerMessage] = []
            for message in batch:
                if message._allow_delivery(
                    max_deliveries=self._max_deliveries,
                    logger=self._logger,
                ):
                    message.retry_strategy = self._retry_strategy
                    to_process.append(message)

            try:
                if to_process:
                    await self.consume(tuple(to_process))  # type: ignore[arg-type]

            except asyncio.CancelledError:
                for message in batch:
                    message.requeue_from_attempted()
                raise

            finally:
                for message in to_process:
                    message._reject_if_state_not_set(self._logger)

                for message in batch:
                    self._not_processed_count -= 1
                    self._buffer_results(message)
                    self._pending_consume_queue.task_done()

            self._check_if_may_fetch_eagerly()
