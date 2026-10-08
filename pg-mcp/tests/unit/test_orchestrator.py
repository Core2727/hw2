"""Unit tests for QueryOrchestrator.

This module tests the orchestrator's coordination of the query pipeline,
including multi-database routing, retry logic, rate limiting, metrics
instrumentation, error handling, and integration with all components.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from pg_mcp.config.settings import ResilienceConfig, ValidationConfig
from pg_mcp.models.errors import (
    DatabaseError,
    LLMError,
    LLMTimeoutError,
    SecurityViolationError,
    SQLParseError,
)
from pg_mcp.models.query import (
    QueryRequest,
    ResultValidationResult,
    ReturnType,
    ValidationResult,
)
from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo
from pg_mcp.observability.metrics import MetricsCollector
from pg_mcp.resilience.circuit_breaker import CircuitState
from pg_mcp.resilience.rate_limiter import MultiRateLimiter
from pg_mcp.services.orchestrator import QueryOrchestrator


class TestDatabaseResolution:
    """Test database name resolution logic."""

    @pytest.fixture
    def mock_pools(self) -> dict[str, MagicMock]:
        """Create mock connection pools."""
        return {
            "db1": MagicMock(),
            "db2": MagicMock(),
        }

    @pytest.fixture
    def orchestrator(self, mock_pools: dict[str, MagicMock]) -> QueryOrchestrator:
        """Create orchestrator with mocked components."""
        return QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={name: MagicMock() for name in mock_pools},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools=mock_pools,
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

    def test_resolve_database_specified_valid(self, orchestrator: QueryOrchestrator) -> None:
        """Test resolving a specified valid database."""
        result = orchestrator._resolve_database("db1")
        assert result == "db1"

    def test_resolve_database_specified_invalid(self, orchestrator: QueryOrchestrator) -> None:
        """Test resolving a specified but invalid database."""
        with pytest.raises(DatabaseError) as exc_info:
            orchestrator._resolve_database("nonexistent")

        assert "not found" in str(exc_info.value).lower()
        assert "db1" in exc_info.value.details["available_databases"]
        assert "db2" in exc_info.value.details["available_databases"]

    def test_resolve_database_auto_select_single(self) -> None:
        """Test auto-selecting when only one database available."""
        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"only_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"only_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        result = orchestrator._resolve_database(None)
        assert result == "only_db"

    def test_resolve_database_auto_select_multiple_fails(
        self, orchestrator: QueryOrchestrator
    ) -> None:
        """Test that auto-select fails when multiple databases available."""
        with pytest.raises(DatabaseError) as exc_info:
            orchestrator._resolve_database(None)

        assert "multiple databases" in str(exc_info.value).lower()
        assert "db1" in exc_info.value.details["available_databases"]

    def test_resolve_database_no_databases(self) -> None:
        """Test error when no databases configured."""
        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        with pytest.raises(DatabaseError) as exc_info:
            orchestrator._resolve_database(None)

        assert "no databases configured" in str(exc_info.value).lower()


class TestSQLGenerationWithRetry:
    """Test SQL generation with retry logic."""

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="test_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[
                        ColumnInfo(
                            name="id",
                            data_type="integer",
                            is_nullable=False,
                            is_primary_key=True,
                        ),
                        ColumnInfo(
                            name="name",
                            data_type="varchar(255)",
                            is_nullable=False,
                        ),
                    ],
                )
            ],
            version="15.0",
        )

    @pytest.mark.asyncio
    async def test_generate_sql_success_first_attempt(self, mock_schema: DatabaseSchema) -> None:
        """Test successful SQL generation on first attempt."""
        # Setup mocks
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT * FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )  # No exception = valid

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=3),
            validation_config=ValidationConfig(),
        )

        # Execute
        sql, validation_result, _tokens = await orchestrator._generate_sql_with_retry(
            question="Get all users",
            schema=mock_schema,
            request_id="test-123",
        )

        # Verify
        assert sql == "SELECT * FROM users;"
        assert validation_result is None or validation_result.is_valid is True
        mock_generator.generate.assert_called_once()

    @pytest.mark.asyncio
    async def test_generate_sql_retry_on_validation_failure(
        self, mock_schema: DatabaseSchema
    ) -> None:
        """Test retry logic when validation fails."""
        # Setup mocks - first attempt fails validation, second succeeds
        mock_generator = AsyncMock()
        mock_generator.generate.side_effect = [
            "SELECT * FROM user;",  # First attempt (wrong table name)
            "SELECT * FROM users;",  # Second attempt (correct)
        ]

        mock_validator = MagicMock()
        # First call raises error, second call succeeds
        mock_validator.validate_result_or_raise.side_effect = [
            SQLParseError('relation "user" does not exist'),
            ValidationResult(is_valid=True, is_select=True),  # Success on second attempt
        ]

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=3),
            validation_config=ValidationConfig(),
        )

        # Execute
        sql, _validation, _tokens = await orchestrator._generate_sql_with_retry(
            question="Get all users",
            schema=mock_schema,
            request_id="test-123",
        )

        # Verify
        assert sql == "SELECT * FROM users;"
        assert mock_generator.generate.call_count == 2
        assert mock_validator.validate_result_or_raise.call_count == 2

        # Verify retry included error feedback
        second_call = mock_generator.generate.call_args_list[1]
        assert second_call.kwargs["previous_attempt"] == "SELECT * FROM user;"
        assert 'relation "user" does not exist' in second_call.kwargs["error_feedback"]

    @pytest.mark.asyncio
    async def test_generate_sql_fails_after_max_retries(self, mock_schema: DatabaseSchema) -> None:
        """Test failure after exhausting all retries."""
        # Setup mocks - all attempts fail validation
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "DELETE FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.side_effect = SecurityViolationError(
            "DELETE statements are not allowed"
        )

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=2),
            validation_config=ValidationConfig(),
        )

        # Execute and verify exception
        with pytest.raises(SecurityViolationError) as exc_info:
            await orchestrator._generate_sql_with_retry(
                question="Delete all users",
                schema=mock_schema,
                request_id="test-123",
            )

        assert "DELETE statements are not allowed" in str(exc_info.value)
        # Should attempt max_retries + 1 times (initial + retries)
        assert mock_generator.generate.call_count == 3
        assert orchestrator.circuit_breaker.failure_count == 1

    @pytest.mark.asyncio
    async def test_generate_sql_circuit_breaker_open(self, mock_schema: DatabaseSchema) -> None:
        """Test that open circuit breaker prevents SQL generation."""
        orchestrator = QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(circuit_breaker_threshold=1),
            validation_config=ValidationConfig(),
        )

        # Manually open the circuit breaker
        orchestrator.circuit_breaker._state = CircuitState.OPEN
        orchestrator.circuit_breaker._failure_count = 5

        # Attempt should fail immediately
        with pytest.raises(LLMError) as exc_info:
            await orchestrator._generate_sql_with_retry(
                question="Get all users",
                schema=mock_schema,
                request_id="test-123",
            )

        assert "temporarily unavailable" in str(exc_info.value).lower()
        assert "circuit breaker" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_generate_sql_unexpected_error(self, mock_schema: DatabaseSchema) -> None:
        """Test handling of unexpected errors during generation."""
        mock_generator = AsyncMock()
        mock_generator.generate.side_effect = RuntimeError("Unexpected error")

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=1),
            validation_config=ValidationConfig(),
        )

        with pytest.raises(LLMError) as exc_info:
            await orchestrator._generate_sql_with_retry(
                question="Get all users",
                schema=mock_schema,
                request_id="test-123",
            )

        assert "unexpectedly" in str(exc_info.value).lower()
        assert orchestrator.circuit_breaker.failure_count == 1

    @pytest.mark.asyncio
    async def test_generate_sql_retries_transient_llm_error(
        self, mock_schema: DatabaseSchema
    ) -> None:
        """Test that transient LLM errors (timeout) are retried with backoff."""
        mock_generator = AsyncMock()
        mock_generator.generate.side_effect = [
            LLMTimeoutError("Request timed out"),
            "SELECT * FROM users;",  # Success on retry
        ]

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=2),
            validation_config=ValidationConfig(),
        )

        sql, _validation, _tokens = await orchestrator._generate_sql_with_retry(
            question="Get all users",
            schema=mock_schema,
            request_id="test-123",
        )

        assert sql == "SELECT * FROM users;"
        assert mock_generator.generate.call_count == 2


class TestResultValidation:
    """Test result validation logic."""

    @pytest.mark.asyncio
    async def test_validate_results_success(self) -> None:
        """Test successful result validation."""
        mock_validator = AsyncMock()
        mock_validator.validate.return_value = ResultValidationResult(
            confidence=85,
            explanation="Results match the question well",
            suggestion=None,
            is_acceptable=True,
        )

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=mock_validator,
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=True),
        )

        confidence = await orchestrator._validate_results_safely(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 42}],
            row_count=1,
            request_id="test-123",
        )

        assert confidence == 85
        mock_validator.validate.assert_called_once()

    @pytest.mark.asyncio
    async def test_validate_results_disabled(self) -> None:
        """Test that validation is skipped when disabled."""
        mock_validator = AsyncMock()

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=mock_validator,
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=False),
        )

        confidence = await orchestrator._validate_results_safely(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 42}],
            row_count=1,
            request_id="test-123",
        )

        assert confidence == 100
        mock_validator.validate.assert_not_called()

    @pytest.mark.asyncio
    async def test_validate_results_failure_returns_neutral_confidence(self) -> None:
        """Test that validation failures don't raise and return neutral confidence.

        Validation failures previously defaulted to confidence 100, masking
        correctness issues; now they return a neutral 50.
        """
        mock_validator = AsyncMock()
        mock_validator.validate.side_effect = Exception("Validation failed")

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=mock_validator,
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=True),
        )

        # Should not raise, returns neutral confidence
        confidence = await orchestrator._validate_results_safely(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 42}],
            row_count=1,
            request_id="test-123",
        )

        assert confidence == 50


class TestQuestionLengthGuard:
    """Test the question length guard (input size protection)."""

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="test_db",
            tables=[],
            version="15.0",
        )

    @pytest.mark.asyncio
    async def test_question_too_long_rejected(self, mock_schema: DatabaseSchema) -> None:
        """Test that questions exceeding max_question_length are rejected."""
        mock_generator = AsyncMock()
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(max_question_length=100),
        )

        request = QueryRequest(
            question="x" * 501,  # QueryRequest allows 10000 chars
            database="test_db",
        )
        response = await orchestrator.execute_query(request)

        assert response.success is False
        assert response.error is not None
        assert response.error.code == "question_too_long"
        assert "exceeds" in response.error.message
        # The LLM must never be called with oversized input
        mock_generator.generate.assert_not_called()

    @pytest.mark.asyncio
    async def test_question_within_limit_accepted(self, mock_schema: DatabaseSchema) -> None:
        """Test that questions within the limit proceed to generation."""
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT 1;"
        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(max_question_length=100),
        )

        request = QueryRequest(
            question="x" * 50,
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        assert response.success is True
        mock_generator.generate.assert_called_once()


class TestLowConfidenceFlag:
    """Test the low confidence flag derived from min_confidence_score."""

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="test_db",
            tables=[],
            version="15.0",
        )

    def _build_orchestrator(
        self,
        mock_schema: DatabaseSchema,
        result_confidence: int,
        min_confidence_score: int = 70,
    ) -> QueryOrchestrator:
        """Build an orchestrator whose result validator returns a fixed confidence."""
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT * FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )

        mock_executor = AsyncMock()
        mock_executor.execute.return_value = ([{"count": 1}], 1)

        mock_result_validator = AsyncMock()
        mock_result_validator.validate.return_value = ResultValidationResult(
            confidence=result_confidence,
            explanation="assessment",
            suggestion=None,
            is_acceptable=result_confidence >= min_confidence_score,
        )

        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        return QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": mock_executor},
            result_validator=mock_result_validator,
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(
                enabled=True,
                min_confidence_score=min_confidence_score,
            ),
        )

    @pytest.mark.asyncio
    async def test_low_confidence_flag_set_below_threshold(
        self, mock_schema: DatabaseSchema
    ) -> None:
        """Confidence below min_confidence_score must flag low_confidence."""
        orchestrator = self._build_orchestrator(mock_schema, result_confidence=40)

        response = await orchestrator.execute_query(
            QueryRequest(question="Count users", database="test_db")
        )

        assert response.success is True
        assert response.confidence == 40
        assert response.low_confidence is True

    @pytest.mark.asyncio
    async def test_low_confidence_flag_clear_at_threshold(
        self, mock_schema: DatabaseSchema
    ) -> None:
        """Confidence at/above min_confidence_score must not flag."""
        orchestrator = self._build_orchestrator(mock_schema, result_confidence=90)

        response = await orchestrator.execute_query(
            QueryRequest(question="Count users", database="test_db")
        )

        assert response.success is True
        assert response.confidence == 90
        assert response.low_confidence is False


class TestMultiDatabaseRouting:
    """Test that the executor bound to the resolved database is used."""

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="db1",
            tables=[],
            version="15.0",
        )

    @pytest.mark.asyncio
    async def test_executor_selected_per_database(self, mock_schema: DatabaseSchema) -> None:
        """Request for db2 must execute on db2's executor, not db1's."""
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT 1;"

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )

        executor_db1 = AsyncMock()
        executor_db2 = AsyncMock()
        executor_db2.execute.return_value = ([{"v": 1}], 1)

        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"db1": executor_db1, "db2": executor_db2},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"db1": MagicMock(), "db2": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=False),
        )

        response = await orchestrator.execute_query(
            QueryRequest(question="Test", database="db2", return_type=ReturnType.RESULT)
        )

        assert response.success is True
        executor_db2.execute.assert_awaited_once()
        executor_db1.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_database_rejected(self) -> None:
        """A request naming an unconfigured database must fail without executing."""
        orchestrator = QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validator=MagicMock(),
            sql_executors={"db1": AsyncMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"db1": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        response = await orchestrator.execute_query(
            QueryRequest(question="Test", database="other_db")
        )

        assert response.success is False
        assert response.error is not None
        assert response.error.code == "database_error"
        assert "not found" in response.error.message.lower()


class TestMetricsInstrumentation:
    """Test that the orchestrator emits metrics in the request path.

    MetricsCollector is a process-wide singleton, so all assertions are
    delta-based (value after - value before) to stay independent of other
    tests running in the same session.
    """

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="test_db",
            tables=[],
            version="15.0",
        )

    @pytest.mark.asyncio
    async def test_success_increments_query_request_counter(
        self, mock_schema: DatabaseSchema
    ) -> None:
        """Successful queries must be counted with status=success."""
        metrics = MetricsCollector()
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT 1;"

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )

        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": AsyncMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
            metrics=metrics,
        )

        before = metrics.query_requests.labels(status="success", database="test_db")._value.get()

        await orchestrator.execute_query(
            QueryRequest(
                question="Test",
                database="test_db",
                return_type=ReturnType.SQL,
            )
        )

        after = metrics.query_requests.labels(status="success", database="test_db")._value.get()
        assert after - before == 1

    @pytest.mark.asyncio
    async def test_llm_call_metric_incremented(self, mock_schema: DatabaseSchema) -> None:
        """LLM generation must be counted."""
        metrics = MetricsCollector()
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT 1;"
        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": AsyncMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
            metrics=metrics,
        )

        before = metrics.llm_calls.labels(operation="generate_sql")._value.get()

        await orchestrator.execute_query(
            QueryRequest(question="Test", database="test_db", return_type=ReturnType.SQL)
        )

        after = metrics.llm_calls.labels(operation="generate_sql")._value.get()
        assert after - before == 1

    @pytest.mark.asyncio
    async def test_security_rejection_metric_incremented(self) -> None:
        """Security violations must be counted as rejected SQL."""
        metrics = MetricsCollector()
        mock_cache = MagicMock()
        mock_cache.get.return_value = None
        mock_cache.load = AsyncMock(
            return_value=DatabaseSchema(database_name="test_db", tables=[], version="15.0")
        )
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "DELETE FROM users;"
        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.side_effect = SecurityViolationError(
            "DELETE not allowed"
        )

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": AsyncMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=0),
            validation_config=ValidationConfig(),
            metrics=metrics,
        )

        before = metrics.sql_rejected.labels(reason="security_violation")._value.get()

        response = await orchestrator.execute_query(
            QueryRequest(question="Delete users", database="test_db")
        )

        assert response.success is False
        after = metrics.sql_rejected.labels(reason="security_violation")._value.get()
        assert after - before == 1


class TestRateLimiterIntegration:
    """Test that the rate limiter is applied around LLM and DB operations."""

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="test_db",
            tables=[],
            version="15.0",
        )

    @pytest.mark.asyncio
    async def test_rate_limiter_stats_record_requests(self, mock_schema: DatabaseSchema) -> None:
        """LLM and query limiters must observe the operations."""
        rate_limiter = MultiRateLimiter(query_limit=2, llm_limit=2)
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT 1;"
        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )
        mock_executor = AsyncMock()
        mock_executor.execute.return_value = ([{"v": 1}], 1)
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": mock_executor},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=False),
            rate_limiter=rate_limiter,
        )

        response = await orchestrator.execute_query(
            QueryRequest(question="Test", database="test_db")
        )

        assert response.success is True
        llm_stats = rate_limiter.llm_limiter.get_stats()
        query_stats = rate_limiter.query_limiter.get_stats()
        assert llm_stats["total_requests"] >= 1
        assert query_stats["total_requests"] >= 1
        # Everything released properly (release() decrements the active
        # counter asynchronously, so yield to the event loop first)
        await asyncio.sleep(0.05)
        assert rate_limiter.llm_limiter.get_stats()["active_count"] == 0
        assert rate_limiter.query_limiter.get_stats()["active_count"] == 0

    @pytest.mark.asyncio
    async def test_default_limiter_built_from_config(self) -> None:
        """When no limiter is injected, one is built from ResilienceConfig."""
        config = ResilienceConfig(max_concurrent_queries=7, max_concurrent_llm_calls=3)
        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={},
            resilience_config=config,
            validation_config=ValidationConfig(),
        )

        assert orchestrator.rate_limiter.query_limiter.max_concurrent == 7
        assert orchestrator.rate_limiter.llm_limiter.max_concurrent == 3


class TestDatabaseTransientRetry:
    """Test retry of transient database failures in the execution path."""

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="test_db",
            tables=[],
            version="15.0",
        )

    @pytest.mark.asyncio
    async def test_transient_db_error_retried(self, mock_schema: DatabaseSchema) -> None:
        """A transient connection error must be retried and succeed."""
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT 1;"
        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )

        transient_error = DatabaseError(
            message="connection lost",
            details={"transient": True},
        )
        mock_executor = AsyncMock()
        mock_executor.execute.side_effect = [
            transient_error,
            ([{"v": 1}], 1),  # Success on retry
        ]

        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": mock_executor},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=2),
            validation_config=ValidationConfig(enabled=False),
        )

        response = await orchestrator.execute_query(
            QueryRequest(question="Test", database="test_db")
        )

        assert response.success is True
        assert mock_executor.execute.await_count == 2

    @pytest.mark.asyncio
    async def test_permanent_db_error_not_retried(self, mock_schema: DatabaseSchema) -> None:
        """A non-transient SQL error must surface immediately without retry."""
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT bogus;"
        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )

        permanent_error = DatabaseError(
            message="column does not exist",
            details={"transient": False},
        )
        mock_executor = AsyncMock()
        mock_executor.execute.side_effect = permanent_error

        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": mock_executor},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=3),
            validation_config=ValidationConfig(enabled=False),
        )

        response = await orchestrator.execute_query(
            QueryRequest(question="Test", database="test_db")
        )

        assert response.success is False
        assert mock_executor.execute.await_count == 1
        assert response.error is not None
        assert response.error.code == "database_error"


class TestExecuteQueryFlow:
    """Test complete query execution flow."""

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="test_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[
                        ColumnInfo(
                            name="id",
                            data_type="integer",
                            is_nullable=False,
                            is_primary_key=True,
                        ),
                        ColumnInfo(
                            name="name",
                            data_type="varchar(255)",
                            is_nullable=False,
                        ),
                    ],
                )
            ],
            version="15.0",
        )

    @pytest.mark.asyncio
    async def test_execute_query_sql_only(self, mock_schema: DatabaseSchema) -> None:
        """Test executing query with return_type=SQL."""
        # Setup mocks
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT * FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )

        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Get all users",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify
        assert response.success is True
        assert response.generated_sql == "SELECT * FROM users;"
        assert response.validation is not None
        assert response.validation.is_valid is True
        assert response.data is None  # No execution for SQL-only
        assert response.error is None

    @pytest.mark.asyncio
    async def test_execute_query_with_results(self, mock_schema: DatabaseSchema) -> None:
        """Test executing query with return_type=RESULT."""
        # Setup mocks
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT id, name FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )

        mock_executor = AsyncMock()
        mock_executor.execute.return_value = (
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
            2,  # total count
        )

        mock_result_validator = AsyncMock()
        mock_result_validator.validate.return_value = ResultValidationResult(
            confidence=90,
            explanation="Good results",
            suggestion=None,
            is_acceptable=True,
        )

        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": mock_executor},
            result_validator=mock_result_validator,
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=True),
        )

        # Execute
        request = QueryRequest(
            question="Get all users",
            database="test_db",
            return_type=ReturnType.RESULT,
        )
        response = await orchestrator.execute_query(request)

        # Verify
        assert response.success is True
        assert response.generated_sql == "SELECT id, name FROM users;"
        assert response.data is not None
        assert response.data.row_count == 2
        assert len(response.data.rows) == 2
        assert response.data.columns == ["id", "name"]
        assert response.confidence == 90
        assert response.error is None

    @pytest.mark.asyncio
    async def test_execute_query_schema_not_cached(self) -> None:
        """Test loading schema when not in cache."""
        mock_schema = DatabaseSchema(
            database_name="test_db",
            tables=[],
            version="15.0",
        )

        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = None  # Not in cache
        mock_cache.load = AsyncMock(return_value=mock_schema)

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT 1;"

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )

        mock_pool = MagicMock()

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": mock_pool},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Test query",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify schema was loaded
        mock_cache.load.assert_called_once_with("test_db", mock_pool)
        assert response.success is True

    @pytest.mark.asyncio
    async def test_execute_query_schema_load_fails(self) -> None:
        """Test handling of schema load failure."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = None
        mock_cache.load = AsyncMock(side_effect=Exception("DB connection failed"))

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Test query",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert "schema" in response.error.message.lower()
        assert response.generated_sql is None

    @pytest.mark.asyncio
    async def test_execute_query_validation_error(self) -> None:
        """Test handling of SQL validation errors."""
        mock_schema = DatabaseSchema(
            database_name="test_db",
            tables=[],
            version="15.0",
        )

        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "DELETE FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.side_effect = SecurityViolationError(
            "DELETE not allowed"
        )

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=1),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Delete all users",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert "DELETE not allowed" in response.error.message
        assert response.error.code == "security_violation"

    @pytest.mark.asyncio
    async def test_execute_query_execution_error(self, mock_schema: DatabaseSchema) -> None:
        """Test handling of SQL execution errors."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT * FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )

        mock_executor = AsyncMock()
        mock_executor.execute.side_effect = DatabaseError(
            message="Query execution failed",
            details={"transient": False},
        )

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": mock_executor},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Get all users",
            database="test_db",
            return_type=ReturnType.RESULT,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert "execution failed" in response.error.message.lower()
        assert response.error.code == "database_error"

    @pytest.mark.asyncio
    async def test_execute_query_unexpected_error(self, mock_schema: DatabaseSchema) -> None:
        """Test handling of unexpected errors."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.side_effect = RuntimeError("Unexpected error")

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Test query",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert response.error.code == "internal_error"
        assert "internal server error" in response.error.message.lower()

    @pytest.mark.asyncio
    async def test_execute_query_auto_select_database(self, mock_schema: DatabaseSchema) -> None:
        """Test auto-selecting database when only one available."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT 1;"

        mock_validator = MagicMock()
        mock_validator.validate_result_or_raise.return_value = ValidationResult(
            is_valid=True, is_select=True
        )

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"only_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"only_db": MagicMock()},  # Only one database
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute without specifying database
        request = QueryRequest(
            question="Test query",
            database=None,  # No database specified
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify
        assert response.success is True
        # Verify schema was fetched for auto-selected database
        mock_cache.get.assert_called_once_with("only_db")
