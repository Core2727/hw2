"""Query orchestrator for coordinating the complete query flow.

This module provides the QueryOrchestrator class that coordinates all components
of the query processing pipeline: SQL generation, validation, execution, and result
validation. It implements multi-database routing, retry logic with exponential
backoff, rate limiting, circuit breaker fault tolerance, metrics instrumentation,
and comprehensive error handling.
"""

import logging
import time
from typing import Any

from asyncpg import Pool

from pg_mcp.cache.schema_cache import SchemaCache
from pg_mcp.config.settings import ResilienceConfig, ValidationConfig
from pg_mcp.models.errors import (
    DatabaseError,
    ErrorCode,
    LLMError,
    LLMTimeoutError,
    LLMUnavailableError,
    PgMcpError,
    QuestionTooLongError,
    SchemaLoadError,
    SecurityViolationError,
    SQLParseError,
)
from pg_mcp.models.query import (
    ErrorDetail,
    QueryRequest,
    QueryResponse,
    QueryResult,
    ReturnType,
    ValidationResult,
)
from pg_mcp.observability.metrics import MetricsCollector
from pg_mcp.observability.tracing import generate_request_id, get_request_id
from pg_mcp.resilience.circuit_breaker import CircuitBreaker
from pg_mcp.resilience.rate_limiter import MultiRateLimiter
from pg_mcp.resilience.retry import with_retry
from pg_mcp.services.result_validator import ResultValidator
from pg_mcp.services.sql_executor import SQLExecutor
from pg_mcp.services.sql_generator import SQLGenerator
from pg_mcp.services.sql_validator import SQLValidator

logger = logging.getLogger(__name__)


def _is_transient_db_error(exc: BaseException) -> bool:
    """Check whether a database error is transient and worth retrying.

    The SQLExecutor marks transient PostgreSQL failures (connection issues,
    serialization conflicts, deadlocks) with ``details["transient"] = True``.
    """
    return bool(isinstance(exc, DatabaseError) and exc.details.get("transient") is True)


def _is_transient_llm_error(exc: BaseException) -> bool:
    """Check whether an LLM error is transient and worth retrying.

    Timeouts and availability issues (rate limits, temporary outages) are
    retried; deterministic failures (invalid request, auth) are not.
    """
    return isinstance(exc, (LLMTimeoutError, LLMUnavailableError))


class QueryOrchestrator:
    """Orchestrates the complete query processing pipeline.

    This class coordinates SQL generation, validation, execution, and result
    validation across multiple databases. It implements per-database executor
    routing, retry logic with exponential backoff, rate limiting, circuit
    breaker pattern for fault tolerance, and metrics instrumentation.

    Example:
        >>> orchestrator = QueryOrchestrator(
        ...     sql_generator=generator,
        ...     sql_validator=validator,
        ...     sql_executors={"mydb": executor},
        ...     result_validator=result_validator,
        ...     schema_cache=cache,
        ...     pools={"mydb": pool},
        ...     resilience_config=resilience_config,
        ...     validation_config=validation_config,
        ... )
        >>> response = await orchestrator.execute_query(QueryRequest(
        ...     question="How many users?",
        ...     database="mydb"
        ... ))
    """

    def __init__(
        self,
        sql_generator: SQLGenerator,
        sql_validator: SQLValidator,
        sql_executors: dict[str, SQLExecutor],
        result_validator: ResultValidator,
        schema_cache: SchemaCache,
        pools: dict[str, Pool],
        resilience_config: ResilienceConfig,
        validation_config: ValidationConfig,
        rate_limiter: MultiRateLimiter | None = None,
        metrics: MetricsCollector | None = None,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        """Initialize query orchestrator.

        Args:
            sql_generator: SQL generation service.
            sql_validator: SQL validation service.
            sql_executors: SQL execution services keyed by database name.
            result_validator: Result validation service.
            schema_cache: Schema cache instance.
            pools: Dictionary mapping database names to connection pools.
            resilience_config: Resilience configuration for retries,
                circuit breaker, and concurrency limits.
            validation_config: Validation configuration including thresholds.
            rate_limiter: Optional rate limiter for LLM calls and DB execution.
                When None, a local limiter built from resilience_config is used.
            metrics: Optional metrics collector. When None, a shared collector
                is created (singleton).
            circuit_breaker: Optional shared circuit breaker for LLM calls.
                When None, a local breaker is created from resilience_config.
        """
        self.sql_generator = sql_generator
        self.sql_validator = sql_validator
        self.sql_executors = sql_executors
        self.result_validator = result_validator
        self.schema_cache = schema_cache
        self.pools = pools
        self.resilience_config = resilience_config
        self.validation_config = validation_config

        # Resilience components (share server-level instances when provided)
        self.rate_limiter = rate_limiter or MultiRateLimiter(
            query_limit=resilience_config.max_concurrent_queries,
            llm_limit=resilience_config.max_concurrent_llm_calls,
        )
        self.metrics = metrics or MetricsCollector()
        self.circuit_breaker = circuit_breaker or CircuitBreaker(
            failure_threshold=resilience_config.circuit_breaker_threshold,
            recovery_timeout=resilience_config.circuit_breaker_timeout,
        )

    async def execute_query(self, request: QueryRequest) -> QueryResponse:
        """Execute complete query flow from question to results.

        This method orchestrates the entire pipeline:
        1. Resolve request_id from trace context (or generate one)
        2. Enforce input length guard (max_question_length)
        3. Resolve and validate database name
        4. Load schema from cache
        5. Generate and validate SQL (rate limited, circuit broken)
        6. Execute SQL on the selected database (rate limited, retried)
        7. Validate results (optional)
        8. Return structured response

        Args:
            request: Query request containing question and parameters.

        Returns:
            QueryResponse: Complete response with SQL, results, or error information.

        Example:
            >>> response = await orchestrator.execute_query(
            ...     QueryRequest(question="Count all users", return_type="result")
            ... )
            >>> if response.success:
            ...     print(f"Found {response.data.row_count} rows")
        """
        # Reuse the request_id from the trace context when the server has
        # already opened one, so logs across handler and orchestrator correlate.
        request_id = get_request_id() or generate_request_id()
        logger.info(
            "Starting query execution",
            extra={"request_id": request_id, "question": request.question[:100]},
        )

        start_time = self._get_current_time_ms()
        database_name: str | None = None
        status = "success"

        try:
            # Step 0: Input length guard (prevents unbounded prompt size/cost)
            max_len = self.validation_config.max_question_length
            if len(request.question) > max_len:
                status = "question_too_long"
                self.metrics.increment_query_request(status=status, database="unknown")
                raise QuestionTooLongError(
                    message=(
                        f"Question length {len(request.question)} exceeds the "
                        f"maximum of {max_len} characters"
                    ),
                    details={"length": len(request.question), "max_length": max_len},
                )

            # Step 1: Resolve database name
            database_name = self._resolve_database(request.database)
            logger.debug(
                "Resolved database",
                extra={"request_id": request_id, "database": database_name},
            )

            # Step 2: Get schema from cache
            schema = self.schema_cache.get(database_name)
            if schema is None:
                # Schema not in cache, load it
                pool = self.pools.get(database_name)
                if pool is None:
                    raise DatabaseError(
                        message=f"No connection pool available for database '{database_name}'",
                        details={"database": database_name},
                    )
                try:
                    schema = await self.schema_cache.load(database_name, pool)
                except Exception as e:
                    raise SchemaLoadError(
                        message=f"Failed to load schema for database '{database_name}': {e!s}",
                        details={"database": database_name, "error": str(e)},
                    ) from e

            logger.debug(
                "Schema loaded",
                extra={
                    "request_id": request_id,
                    "database": database_name,
                    "tables": len(schema.tables),
                },
            )

            # Step 3: Generate and validate SQL (rate limited + circuit broken)
            generated_sql, validation_result, tokens_used = await self._generate_sql_with_retry(
                question=request.question,
                schema=schema,
                request_id=request_id,
            )

            # Step 4: If return_type is SQL, return early
            if request.return_type == ReturnType.SQL:
                logger.info(
                    "Returning SQL only",
                    extra={"request_id": request_id, "sql_length": len(generated_sql)},
                )
                self._record_query_metrics(status, database_name, start_time)
                return QueryResponse(
                    success=True,
                    generated_sql=generated_sql,
                    validation=validation_result,
                    data=None,
                    error=None,
                    confidence=100,
                    tokens_used=tokens_used,
                )

            # Step 5: Execute SQL on the executor bound to the resolved database
            # (rate limited + retried on transient failures)
            logger.debug(
                "Executing SQL",
                extra={"request_id": request_id, "database": database_name},
            )
            db_start = self._get_current_time_ms()

            executor = self.sql_executors.get(database_name)
            if executor is None:
                raise DatabaseError(
                    message=f"No SQL executor available for database '{database_name}'",
                    details={"database": database_name},
                )

            results, total_count = await self._execute_with_resilience(
                executor=executor,
                sql=generated_sql,
                request_id=request_id,
            )

            execution_time_ms = self._get_current_time_ms() - db_start
            self.metrics.observe_db_query_duration(execution_time_ms / 1000.0)
            logger.info(
                "SQL executed successfully",
                extra={
                    "request_id": request_id,
                    "database": database_name,
                    "row_count": total_count,
                    "execution_time_ms": execution_time_ms,
                },
            )

            # Step 6: Validate results (non-blocking, failures don't fail the request)
            result_confidence = await self._validate_results_safely(
                question=request.question,
                sql=generated_sql,
                results=results,
                row_count=total_count,
                request_id=request_id,
            )

            # Step 7: Build successful response
            low_confidence = result_confidence < self.validation_config.min_confidence_score
            if low_confidence:
                logger.warning(
                    "Result confidence below configured minimum",
                    extra={
                        "request_id": request_id,
                        "confidence": result_confidence,
                        "min_confidence_score": self.validation_config.min_confidence_score,
                    },
                )

            query_result = QueryResult(
                columns=list(results[0].keys()) if results else [],
                rows=results,
                row_count=len(results),  # Limited row count (after max_rows applied)
                execution_time_ms=execution_time_ms,
            )

            self._record_query_metrics(status, database_name, start_time)
            return QueryResponse(
                success=True,
                generated_sql=generated_sql,
                validation=validation_result,
                data=query_result,
                error=None,
                confidence=result_confidence,
                tokens_used=tokens_used,
                low_confidence=low_confidence,
            )

        except PgMcpError as e:
            # Handle known application errors
            status = e.code.value
            logger.warning(
                "Query execution failed with known error",
                extra={
                    "request_id": request_id,
                    "error_code": e.code,
                    "error_message": str(e),
                },
            )
            self._record_query_metrics(status, database_name or "unknown", start_time)
            if isinstance(e, (SecurityViolationError, SQLParseError)):
                self.metrics.increment_sql_rejected(reason=e.code.value)
            return QueryResponse(
                success=False,
                generated_sql=None,
                validation=None,
                data=None,
                error=ErrorDetail(
                    code=e.code.value,
                    message=e.message,
                    details=e.details,
                ),
                confidence=0,
                tokens_used=None,
            )
        except Exception as e:
            # Handle unexpected errors
            status = ErrorCode.INTERNAL_ERROR.value
            logger.exception(
                "Query execution failed with unexpected error",
                extra={"request_id": request_id},
            )
            self._record_query_metrics(status, database_name or "unknown", start_time)
            return QueryResponse(
                success=False,
                generated_sql=None,
                validation=None,
                data=None,
                error=ErrorDetail(
                    code=ErrorCode.INTERNAL_ERROR.value,
                    message=f"Internal server error: {e!s}",
                    details={"error_type": type(e).__name__},
                ),
                confidence=0,
                tokens_used=None,
            )

    def _record_query_metrics(
        self,
        status: str,
        database: str,
        start_time_ms: float,
    ) -> None:
        """Record query request counter and total duration.

        Args:
            status: Outcome label for the counter (success, error code, ...).
            database: Database name the query targeted (or "unknown").
            start_time_ms: Pipeline start time in milliseconds.
        """
        self.metrics.increment_query_request(status=status, database=database)
        self.metrics.query_duration.observe((self._get_current_time_ms() - start_time_ms) / 1000.0)

    def _resolve_database(self, database: str | None) -> str:
        """Resolve database name from request or auto-select.

        If database is specified, validate it exists.
        If not specified and only one database available, auto-select it.

        Args:
            database: Database name from request (optional).

        Returns:
            str: Resolved database name.

        Raises:
            DatabaseError: If database is invalid or cannot be auto-selected.

        Example:
            >>> name = orchestrator._resolve_database("mydb")  # Validates "mydb" exists
            >>> name = orchestrator._resolve_database(None)  # Auto-selects if only one DB
        """
        if database is not None:
            # Validate specified database exists
            if database not in self.pools:
                raise DatabaseError(
                    message=f"Database '{database}' not found",
                    details={
                        "requested_database": database,
                        "available_databases": list(self.pools.keys()),
                    },
                )
            return database

        # Auto-select if only one database available
        available_dbs = list(self.pools.keys())
        if len(available_dbs) == 0:
            raise DatabaseError(
                message="No databases configured",
                details={},
            )
        if len(available_dbs) == 1:
            return available_dbs[0]

        # Multiple databases, must specify
        raise DatabaseError(
            message="Multiple databases available, please specify which to query",
            details={"available_databases": available_dbs},
        )

    async def _generate_sql_with_retry(
        self,
        question: str,
        schema: Any,
        request_id: str,
    ) -> tuple[str, ValidationResult, int | None]:
        """Generate and validate SQL with retry logic on validation failures.

        This method implements a retry loop that:
        1. Checks circuit breaker state
        2. Generates SQL using LLM (rate limited, retried on transient errors)
        3. Validates the generated SQL
        4. On validation failure, retries with error feedback
        5. Records success/failure to circuit breaker
        6. Emits LLM metrics (calls, latency, tokens)

        Args:
            question: User's natural language question.
            schema: Database schema for context.
            request_id: Request ID for tracking.

        Returns:
            tuple: (generated_sql, validation_result, tokens_used)

        Raises:
            LLMError: If circuit breaker is open or generation fails.
            SecurityViolationError: If SQL fails validation after all retries.
            SQLParseError: If SQL cannot be parsed.

        Example:
            >>> sql, validation, tokens = await orchestrator._generate_sql_with_retry(
            ...     question="Count users",
            ...     schema=db_schema,
            ...     request_id="123",
            ... )
        """
        # Check circuit breaker
        if not self.circuit_breaker.allow_request():
            raise LLMError(
                message="SQL generation service is temporarily unavailable (circuit breaker open)",
                details={
                    "circuit_state": self.circuit_breaker.state,
                    "failure_count": self.circuit_breaker.failure_count,
                },
            )

        previous_sql: str | None = None
        error_feedback: str | None = None
        max_retries = self.resilience_config.max_retries
        tokens_used: int | None = None

        for attempt in range(max_retries + 1):
            try:
                logger.debug(
                    "Generating SQL",
                    extra={
                        "request_id": request_id,
                        "attempt": attempt + 1,
                        "max_retries": max_retries + 1,
                    },
                )

                # Generate SQL (rate limited, retried on transient LLM errors)
                generated_sql = await self._generate_sql_rate_limited(
                    question=question,
                    schema=schema,
                    previous_attempt=previous_sql,
                    error_feedback=error_feedback,
                    request_id=request_id,
                )

                # Note: tokens_used would come from OpenAI response metadata if available
                # For now, we don't extract it, but it can be added later

                logger.debug(
                    "SQL generated",
                    extra={
                        "request_id": request_id,
                        "sql_length": len(generated_sql),
                    },
                )

                # Validate SQL, propagating the real validation outcome
                try:
                    validation_result = self.sql_validator.validate_result_or_raise(generated_sql)
                except (SecurityViolationError, SQLParseError) as validation_error:
                    if attempt < max_retries:
                        # Retry with feedback so the LLM can correct the query
                        logger.warning(
                            "SQL validation failed, retrying with feedback",
                            extra={
                                "request_id": request_id,
                                "attempt": attempt + 1,
                                "error": str(validation_error),
                            },
                        )
                        previous_sql = generated_sql
                        error_feedback = str(validation_error)
                        continue

                    # Out of retries, record failure and raise
                    self.circuit_breaker.record_failure()
                    logger.error(
                        "SQL validation failed after all retries",
                        extra={
                            "request_id": request_id,
                            "attempts": attempt + 1,
                            "error": str(validation_error),
                        },
                    )
                    raise

                # Validation successful
                self.circuit_breaker.record_success()
                logger.info(
                    "SQL generated and validated successfully",
                    extra={
                        "request_id": request_id,
                        "attempts": attempt + 1,
                    },
                )

                return generated_sql, validation_result, tokens_used

            except (LLMError, SecurityViolationError, SQLParseError):
                # Re-raise known errors
                raise
            except Exception as e:
                # Unexpected error during generation
                self.circuit_breaker.record_failure()
                logger.exception(
                    "Unexpected error during SQL generation",
                    extra={"request_id": request_id},
                )
                raise LLMError(
                    message=f"SQL generation failed unexpectedly: {e!s}",
                    details={"error_type": type(e).__name__},
                ) from e

        # Should not reach here, but just in case
        self.circuit_breaker.record_failure()
        raise LLMError(
            message="SQL generation failed after all retry attempts",
            details={"max_retries": max_retries},
        )

    async def _generate_sql_rate_limited(
        self,
        question: str,
        schema: Any,
        previous_attempt: str | None,
        error_feedback: str | None,
        request_id: str,
    ) -> str:
        """Call the SQL generator under the LLM rate limiter with retry/backoff.

        Wraps the raw LLM call with:
        - Metrics (calls, latency, tokens)
        - MultiRateLimiter.for_llm() concurrency control
        - with_retry() exponential backoff for transient errors

        Args:
            question: User's natural language question.
            schema: Database schema for context.
            previous_attempt: Previously failed SQL (for retry with feedback).
            error_feedback: Error message from previous attempt.
            request_id: Request ID for tracking.

        Returns:
            str: Generated SQL query.
        """
        operation = self.sql_generator.generate

        async def _call() -> str:
            self.metrics.increment_llm_call(operation="generate_sql")
            start = time.perf_counter()
            try:
                async with self.rate_limiter.for_llm():
                    return await operation(
                        question=question,
                        schema=schema,
                        previous_attempt=previous_attempt,
                        error_feedback=error_feedback,
                    )
            finally:
                self.metrics.observe_llm_latency(
                    operation="generate_sql", duration=time.perf_counter() - start
                )

        return await with_retry(
            _call,
            max_retries=self.resilience_config.max_retries,
            retry_delay=self.resilience_config.retry_delay,
            backoff_factor=self.resilience_config.backoff_factor,
            retryable=_is_transient_llm_error,
            operation_name="SQL generation (LLM)",
        )

    async def _execute_with_resilience(
        self,
        executor: SQLExecutor,
        sql: str,
        request_id: str,
    ) -> tuple[list[dict[str, Any]], int]:
        """Execute SQL under the query rate limiter with retry/backoff.

        Only transient database failures (connection issues, serialization
        conflicts, deadlocks) are retried; deterministic SQL errors surface
        immediately.

        Args:
            executor: The SQL executor bound to the resolved database.
            sql: Validated SQL query to execute.
            request_id: Request ID for tracking.

        Returns:
            tuple: (results, total_row_count)
        """

        async def _run() -> tuple[list[dict[str, Any]], int]:
            async with self.rate_limiter.for_queries():
                return await executor.execute(sql)

        return await with_retry(
            _run,
            max_retries=self.resilience_config.max_retries,
            retry_delay=self.resilience_config.retry_delay,
            backoff_factor=self.resilience_config.backoff_factor,
            retryable=_is_transient_db_error,
            operation_name="SQL execution",
        )

    async def _validate_results_safely(
        self,
        question: str,
        sql: str,
        results: list[dict[str, Any]],
        row_count: int,
        request_id: str,
    ) -> int:
        """Validate query results with error handling (non-blocking).

        This method attempts to validate results using LLM, but failures
        don't cause the overall query to fail. Returns a confidence score.

        When validation itself fails, a neutral confidence of 50 is returned
        instead of 100 so that unverifiable results are not presented as
        fully trustworthy.

        Args:
            question: User's original question.
            sql: Generated SQL query.
            results: Query results.
            row_count: Total row count.
            request_id: Request ID for tracking.

        Returns:
            int: Confidence score (0-100).

        Example:
            >>> confidence = await orchestrator._validate_results_safely(
            ...     question="Count users",
            ...     sql="SELECT COUNT(*) FROM users",
            ...     results=[{"count": 42}],
            ...     row_count=1,
            ...     request_id="123",
            ... )
        """
        if not self.validation_config.enabled:
            return 100

        try:
            logger.debug(
                "Validating results",
                extra={"request_id": request_id},
            )

            self.metrics.increment_llm_call(operation="validate_result")
            validation_start = time.perf_counter()

            async with self.rate_limiter.for_llm():
                validation_result = await self.result_validator.validate(
                    question=question,
                    sql=sql,
                    results=results,
                    row_count=row_count,
                )

            self.metrics.observe_llm_latency(
                operation="validate_result", duration=time.perf_counter() - validation_start
            )

            logger.info(
                "Result validation completed",
                extra={
                    "request_id": request_id,
                    "confidence": validation_result.confidence,
                    "is_acceptable": validation_result.is_acceptable,
                },
            )

            return validation_result.confidence

        except Exception as e:
            # Log but don't fail the query. Return a neutral confidence instead
            # of 100 so unverifiable results are not masked as perfect.
            logger.warning(
                "Result validation failed, continuing with neutral confidence",
                extra={
                    "request_id": request_id,
                    "error": str(e),
                },
            )
            return 50

    @staticmethod
    def _get_current_time_ms() -> float:
        """Get current time in milliseconds.

        Returns:
            float: Current time in milliseconds since epoch.
        """
        return time.time() * 1000
