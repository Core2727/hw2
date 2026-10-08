"""Unit tests for the MCP server module (health tool and query tool guards)."""

import asyncio
import time
from unittest.mock import MagicMock

import pytest

import pg_mcp.server as server_module
from pg_mcp.config.settings import ResilienceConfig, ValidationConfig
from pg_mcp.services.orchestrator import QueryOrchestrator


@pytest.fixture(autouse=True)
def restore_server_globals():
    """Save and restore server module globals around each test."""
    saved = {
        name: getattr(server_module, name)
        for name in (
            "_settings",
            "_pools",
            "_schema_cache",
            "_orchestrator",
            "_metrics",
            "_circuit_breaker",
            "_rate_limiter",
            "_started_at",
        )
    }
    yield
    for name, value in saved.items():
        setattr(server_module, name, value)


def run(coro_fn, *args, **kwargs) -> dict:
    """Run a server tool coroutine synchronously."""
    return asyncio.run(coro_fn(*args, **kwargs))


class TestHealthTool:
    """Test the health MCP tool (codex review: missing health check)."""

    def test_health_uninitialized(self) -> None:
        """Before lifespan runs, health must report uninitialized."""
        server_module._orchestrator = None
        result = run(server_module.health)
        assert result["status"] == "uninitialized"
        assert result["databases"] == []

    def test_health_reports_runtime_state(self) -> None:
        """With components initialized, health exposes databases and controls."""
        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"db1": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"db1": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        server_module._orchestrator = orchestrator
        server_module._pools = {"db1": MagicMock()}
        server_module._settings = MagicMock()
        server_module._settings.security.allow_write_operations = False
        server_module._settings.security.blocked_functions = ["pg_sleep"]
        server_module._settings.security.blocked_tables = ["salaries"]
        server_module._settings.security.blocked_columns = []
        server_module._settings.security.allow_explain = False
        server_module._settings.observability.metrics_enabled = False
        server_module._started_at = time.time() - 10.0

        result = run(server_module.health)

        assert result["status"] == "healthy"
        assert result["databases"] == ["db1"]
        assert result["uptime_seconds"] >= 10.0
        assert result["circuit_breaker"]["state"] == "closed"
        assert "queries" in result["rate_limiter"]
        assert result["security"]["blocked_tables"] == ["salaries"]
        assert result["security"]["allow_explain"] is False


class TestQueryToolGuards:
    """Test the query tool's guard paths that do not need a live pipeline."""

    def test_query_returns_error_when_not_initialized(self) -> None:
        """Without an orchestrator the tool must fail with a clear error."""
        server_module._orchestrator = None
        result = run(server_module.query, question="test")
        assert result["success"] is False
        assert result["error"]["code"] == "SERVER_NOT_INITIALIZED"
        assert result["tokens_used"] == 0

    def test_query_rejects_invalid_return_type(self) -> None:
        """An unknown return_type must be rejected before any processing."""
        server_module._orchestrator = MagicMock()
        result = run(server_module.query, question="test", return_type="bogus")
        assert result["success"] is False
        assert result["error"]["code"] == "INVALID_PARAMETER"

    def test_query_rejects_invalid_request(self) -> None:
        """An invalid request payload must produce INVALID_REQUEST."""
        server_module._orchestrator = MagicMock()
        result = run(server_module.query, question="")  # empty question -> pydantic error
        assert result["success"] is False
        assert result["error"]["code"] == "INVALID_REQUEST"
