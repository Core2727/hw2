"""Unit tests for the retry with backoff helper (resilience/retry.py)."""

import pytest

from pg_mcp.models.errors import DatabaseError, LLMError, LLMTimeoutError, LLMUnavailableError
from pg_mcp.resilience.retry import with_retry


class TestWithRetrySuccess:
    """Test successful execution paths."""

    @pytest.mark.asyncio
    async def test_success_first_attempt_no_delay(self) -> None:
        """A successful first attempt must not sleep at all."""
        sleeps: list[float] = []

        async def op() -> str:
            return "ok"

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        result = await with_retry(
            op,
            max_retries=3,
            retry_delay=1.0,
            backoff_factor=2.0,
            sleep=fake_sleep,
        )

        assert result == "ok"
        assert sleeps == []

    @pytest.mark.asyncio
    async def test_success_after_transient_failures(self) -> None:
        """Failing twice then succeeding must retry and return the result."""
        attempts = 0
        sleeps: list[float] = []

        async def op() -> int:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise LLMTimeoutError("timeout")
            return 42

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        result = await with_retry(
            op,
            max_retries=3,
            retry_delay=1.0,
            backoff_factor=2.0,
            retryable=lambda e: isinstance(e, LLMTimeoutError),
            sleep=fake_sleep,
        )

        assert result == 42
        assert attempts == 3
        # Exponential backoff: 1.0 * 2^0, 1.0 * 2^1
        assert sleeps == [1.0, 2.0]

    @pytest.mark.asyncio
    async def test_on_retry_callback_invoked(self) -> None:
        """The on_retry callback must receive the exception and attempt number."""
        calls: list[tuple[BaseException, int]] = []

        def on_retry(exc: BaseException, attempt: int) -> None:
            calls.append((exc, attempt))

        async def op() -> None:
            raise LLMUnavailableError("rate limited")

        async def fake_sleep(_delay: float) -> None:
            pass

        with pytest.raises(LLMUnavailableError):
            await with_retry(
                op,
                max_retries=1,
                retry_delay=0.5,
                backoff_factor=2.0,
                retryable=(LLMUnavailableError,),
                on_retry=on_retry,
                sleep=fake_sleep,
            )

        assert len(calls) == 1
        assert isinstance(calls[0][0], LLMUnavailableError)
        assert calls[0][1] == 1


class TestWithRetryExhaustion:
    """Test behavior when all attempts fail."""

    @pytest.mark.asyncio
    async def test_raises_last_error_after_exhaustion(self) -> None:
        """When retries are exhausted the last exception must propagate."""
        attempts = 0

        async def op() -> None:
            nonlocal attempts
            attempts += 1
            raise DatabaseError(f"failure {attempts}", details={"transient": True})

        async def fake_sleep(_delay: float) -> None:
            pass

        with pytest.raises(DatabaseError) as exc_info:
            await with_retry(
                op,
                max_retries=2,
                retry_delay=0.5,
                backoff_factor=2.0,
                sleep=fake_sleep,
            )

        # 1 initial + 2 retries = 3 attempts
        assert attempts == 3
        assert "failure 3" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_zero_retries_single_attempt(self) -> None:
        """max_retries=0 must execute exactly once."""
        attempts = 0

        async def op() -> None:
            nonlocal attempts
            attempts += 1
            raise LLMError("down")

        async def fake_sleep(_delay: float) -> None:
            pass

        with pytest.raises(LLMError):
            await with_retry(op, max_retries=0, sleep=fake_sleep)

        assert attempts == 1


class TestWithRetryRetryableFiltering:
    """Test which exceptions trigger a retry."""

    @pytest.mark.asyncio
    async def test_non_retryable_raises_immediately(self) -> None:
        """Non-retryable exceptions must propagate without sleeping."""
        attempts = 0
        sleeps: list[float] = []

        async def op() -> None:
            nonlocal attempts
            attempts += 1
            raise LLMError("auth failed - not transient")

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with pytest.raises(LLMError):
            await with_retry(
                op,
                max_retries=3,
                retryable=lambda e: isinstance(e, LLMTimeoutError),
                sleep=fake_sleep,
            )

        assert attempts == 1
        assert sleeps == []

    @pytest.mark.asyncio
    async def test_retryable_exception_types(self) -> None:
        """An iterable of exception types must be accepted as the filter."""
        attempts = 0

        async def op() -> None:
            nonlocal attempts
            attempts += 1
            raise LLMTimeoutError("timeout")

        async def fake_sleep(_delay: float) -> None:
            pass

        with pytest.raises(LLMTimeoutError):
            await with_retry(
                op,
                max_retries=2,
                retryable=(LLMTimeoutError, LLMUnavailableError),
                sleep=fake_sleep,
            )

        assert attempts == 3

    @pytest.mark.asyncio
    async def test_empty_retryable_tuple_never_retries(self) -> None:
        """An empty retryable collection disables retries entirely."""
        attempts = 0

        async def op() -> None:
            nonlocal attempts
            attempts += 1
            raise LLMError("anything")

        async def fake_sleep(_delay: float) -> None:
            pass

        with pytest.raises(LLMError):
            await with_retry(op, max_retries=5, retryable=(), sleep=fake_sleep)

        assert attempts == 1

    @pytest.mark.asyncio
    async def test_real_sleep_used_by_default(self) -> None:
        """Without an injected sleep the real asyncio.sleep is used (smoke test)."""
        attempts = 0

        async def op() -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise DatabaseError("transient", details={"transient": True})
            return "done"

        result = await with_retry(
            op,
            max_retries=1,
            retry_delay=0.01,
            backoff_factor=1.0,
        )

        assert result == "done"
        assert attempts == 2
