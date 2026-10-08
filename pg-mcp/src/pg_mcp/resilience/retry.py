"""Retry with exponential backoff for transient failures.

This module provides a generic retry helper used by the query pipeline to
retry transient LLM and database failures. It consumes the ResilienceConfig
retry settings (max_retries, retry_delay, backoff_factor) that were
previously defined but unused.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def with_retry(
    operation: Callable[[], Awaitable[T]],
    *,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    backoff_factor: float = 2.0,
    retryable: Iterable[type[BaseException]] | Callable[[BaseException], bool] | None = None,
    operation_name: str = "operation",
    on_retry: Callable[[BaseException, int], None] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Execute an async operation with retry and exponential backoff.

    The operation is attempted up to ``max_retries + 1`` times. Between
    attempts the helper waits ``retry_delay * backoff_factor ** (attempt - 1)``
    seconds (e.g. with delay=1.0, factor=2.0: 1s, 2s, 4s, ...).

    Args:
        operation: Zero-argument async callable to execute.
        max_retries: Maximum number of retry attempts after the initial one.
        retry_delay: Initial delay before the first retry in seconds.
        backoff_factor: Multiplier applied to the delay after each attempt.
        retryable: Either an iterable of exception types that should trigger a
            retry, or a predicate receiving the exception and returning True if
            it is retryable. When None, every exception is retryable.
        operation_name: Human-readable name used in log messages.
        on_retry: Optional synchronous callback invoked as ``on_retry(exc, attempt)``
            before each retry (attempt is 1-based, i.e. first retry is 1).
        sleep: Sleep function (injectable for tests).

    Returns:
        T: The result of the successful operation.

    Raises:
        BaseException: The last exception raised by the operation when all
            attempts are exhausted, or immediately for non-retryable exceptions.
    """
    last_error: BaseException | None = None

    for attempt in range(max_retries + 1):
        try:
            return await operation()
        except Exception as exc:
            last_error = exc

            if not _is_retryable(exc, retryable):
                raise

            if attempt >= max_retries:
                logger.error(
                    "%s failed after %d attempt(s), giving up",
                    operation_name,
                    attempt + 1,
                    extra={"attempts": attempt + 1, "error": str(exc)},
                )
                raise

            delay = retry_delay * (backoff_factor**attempt)
            logger.warning(
                "%s failed (attempt %d/%d), retrying in %.2fs: %s",
                operation_name,
                attempt + 1,
                max_retries + 1,
                delay,
                exc,
                extra={"attempt": attempt + 1, "delay_seconds": delay},
            )
            if on_retry is not None:
                on_retry(exc, attempt + 1)
            await sleep(delay)

    # Unreachable: loop either returns or raises.
    raise last_error  # type: ignore[misc]


def _is_retryable(
    exc: BaseException,
    retryable: Iterable[type[BaseException]] | Callable[[BaseException], bool] | None,
) -> bool:
    """Check whether an exception should trigger a retry."""
    if retryable is None:
        return True
    if callable(retryable) and not isinstance(retryable, type):
        return retryable(exc)
    types = tuple(retryable)
    if not types:
        return False
    return isinstance(exc, types)
