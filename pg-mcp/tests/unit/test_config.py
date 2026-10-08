"""Unit tests for configuration management.

Tests for all configuration classes to ensure proper validation,
defaults, and environment variable parsing.
"""

import os

import pytest
from pydantic import ValidationError
from pydantic_settings.exceptions import SettingsError

from pg_mcp.config.settings import (
    CacheConfig,
    DatabaseConfig,
    ObservabilityConfig,
    OpenAIConfig,
    ResilienceConfig,
    SecurityConfig,
    Settings,
    ValidationConfig,
    get_settings,
    reset_settings,
)


class TestDatabaseConfig:
    """Tests for DatabaseConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = DatabaseConfig()
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.name == "postgres"
        assert config.user == "postgres"
        assert config.min_pool_size == 5
        assert config.max_pool_size == 20

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = DatabaseConfig(
            host="db.example.com",
            port=5433,
            name="mydb",
            user="myuser",
            password="secret",
        )
        assert config.host == "db.example.com"
        assert config.port == 5433
        assert config.name == "mydb"
        assert config.user == "myuser"
        assert config.password == "secret"

    def test_dsn_generation(self) -> None:
        """Test DSN string generation."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            name="testdb",
            user="testuser",
            password="testpass",
        )
        dsn = config.dsn
        assert dsn == "postgresql://testuser:testpass@localhost:5432/testdb"

    def test_safe_dsn_masks_password(self) -> None:
        """Test safe DSN masks password."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            name="testdb",
            user="testuser",
            password="secret123",
        )
        safe_dsn = config.safe_dsn
        assert "secret123" not in safe_dsn
        assert "***" in safe_dsn
        assert "testuser" in safe_dsn

    def test_invalid_port(self) -> None:
        """Test invalid port number is rejected."""
        with pytest.raises(ValidationError):
            DatabaseConfig(port=0)

        with pytest.raises(ValidationError):
            DatabaseConfig(port=99999)

    def test_invalid_pool_size(self) -> None:
        """Test invalid pool size is rejected."""
        with pytest.raises(ValidationError):
            DatabaseConfig(min_pool_size=0)

        with pytest.raises(ValidationError):
            DatabaseConfig(max_pool_size=101)


class TestOpenAIConfig:
    """Tests for OpenAIConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = OpenAIConfig(api_key="sk-test123")
        assert config.model == "gpt-4o-mini"
        assert config.max_tokens == 2000
        assert config.temperature == 0.0
        assert config.timeout == 30.0

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = OpenAIConfig(
            api_key="sk-custom",
            model="gpt-4",
            max_tokens=4000,
            temperature=0.7,
            timeout=60.0,
        )
        assert config.model == "gpt-4"
        assert config.max_tokens == 4000
        assert config.temperature == 0.7
        assert config.timeout == 60.0

    def test_empty_api_key_rejected(self) -> None:
        """Test empty API key is rejected."""
        with pytest.raises(ValidationError, match="must not be empty"):
            OpenAIConfig(api_key="")

    def test_whitespace_api_key_rejected(self) -> None:
        """Test whitespace-only API key is rejected."""
        with pytest.raises(ValidationError, match="must not be empty"):
            OpenAIConfig(api_key="   ")

    def test_invalid_api_key_format(self) -> None:
        """Test API key must start with sk-."""
        with pytest.raises(ValidationError, match="must start with 'sk-'"):
            OpenAIConfig(api_key="invalid-key")

    def test_invalid_max_tokens(self) -> None:
        """Test invalid max_tokens is rejected."""
        with pytest.raises(ValidationError):
            OpenAIConfig(api_key="sk-test", max_tokens=50)

        with pytest.raises(ValidationError):
            OpenAIConfig(api_key="sk-test", max_tokens=5000)

    def test_invalid_temperature(self) -> None:
        """Test invalid temperature is rejected."""
        with pytest.raises(ValidationError):
            OpenAIConfig(api_key="sk-test", temperature=-0.1)

        with pytest.raises(ValidationError):
            OpenAIConfig(api_key="sk-test", temperature=2.1)


class TestSecurityConfig:
    """Tests for SecurityConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = SecurityConfig()
        assert config.allow_write_operations is False
        assert config.max_rows == 10000
        assert config.max_execution_time == 30.0
        assert "pg_sleep" in config.blocked_functions
        assert "pg_read_file" in config.blocked_functions

    def test_custom_blocked_functions(self) -> None:
        """Test custom blocked functions."""
        config = SecurityConfig(
            blocked_functions=["func1", "func2"],
        )
        assert config.blocked_functions == ["func1", "func2"]

    def test_parse_blocked_functions_from_string(self) -> None:
        """Test parsing blocked functions from comma-separated string."""
        config = SecurityConfig(
            blocked_functions="func1, func2, func3",  # type: ignore
        )
        assert "func1" in config.blocked_functions
        assert "func2" in config.blocked_functions
        assert "func3" in config.blocked_functions

    def test_allow_write_operations(self) -> None:
        """Test enabling write operations."""
        config = SecurityConfig(allow_write_operations=True)
        assert config.allow_write_operations is True

    def test_invalid_max_rows(self) -> None:
        """Test invalid max_rows is rejected."""
        with pytest.raises(ValidationError):
            SecurityConfig(max_rows=0)

        with pytest.raises(ValidationError):
            SecurityConfig(max_rows=100001)


class TestValidationConfig:
    """Tests for ValidationConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = ValidationConfig()
        assert config.max_question_length == 10000
        assert config.min_confidence_score == 70

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = ValidationConfig(
            max_question_length=5000,
            min_confidence_score=80,
        )
        assert config.max_question_length == 5000
        assert config.min_confidence_score == 80

    def test_invalid_confidence_score(self) -> None:
        """Test invalid confidence score is rejected."""
        with pytest.raises(ValidationError):
            ValidationConfig(min_confidence_score=-1)

        with pytest.raises(ValidationError):
            ValidationConfig(min_confidence_score=101)


class TestCacheConfig:
    """Tests for CacheConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = CacheConfig()
        assert config.schema_ttl == 3600
        assert config.max_size == 100
        assert config.enabled is True

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = CacheConfig(
            schema_ttl=7200,
            max_size=200,
            enabled=False,
        )
        assert config.schema_ttl == 7200
        assert config.max_size == 200
        assert config.enabled is False

    def test_invalid_ttl(self) -> None:
        """Test invalid TTL is rejected."""
        with pytest.raises(ValidationError):
            CacheConfig(schema_ttl=30)

        with pytest.raises(ValidationError):
            CacheConfig(schema_ttl=90000)


class TestResilienceConfig:
    """Tests for ResilienceConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = ResilienceConfig()
        assert config.max_retries == 3
        assert config.retry_delay == 1.0
        assert config.backoff_factor == 2.0
        assert config.circuit_breaker_threshold == 5
        assert config.circuit_breaker_timeout == 60.0

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = ResilienceConfig(
            max_retries=5,
            retry_delay=2.0,
            backoff_factor=3.0,
        )
        assert config.max_retries == 5
        assert config.retry_delay == 2.0
        assert config.backoff_factor == 3.0

    def test_invalid_values(self) -> None:
        """Test invalid values are rejected."""
        with pytest.raises(ValidationError):
            ResilienceConfig(max_retries=-1)

        with pytest.raises(ValidationError):
            ResilienceConfig(backoff_factor=0.5)


class TestObservabilityConfig:
    """Tests for ObservabilityConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = ObservabilityConfig()
        # metrics_enabled 在测试环境可能被禁用以避免启动 HTTP 服务器
        # 生产环境应该通过环境变量显式设置
        assert config.metrics_port == 9090
        assert config.log_level == "INFO"
        assert config.log_format == "json"

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = ObservabilityConfig(
            metrics_enabled=False,
            metrics_port=8080,
            log_level="DEBUG",
            log_format="text",
        )
        assert config.metrics_enabled is False
        assert config.metrics_port == 8080
        assert config.log_level == "DEBUG"
        assert config.log_format == "text"

    def test_invalid_log_level(self) -> None:
        """Test invalid log level is rejected."""
        with pytest.raises(ValidationError):
            ObservabilityConfig(log_level="INVALID")  # type: ignore

    def test_invalid_log_format(self) -> None:
        """Test invalid log format is rejected."""
        with pytest.raises(ValidationError):
            ObservabilityConfig(log_format="xml")  # type: ignore


class TestSettings:
    """Tests for main Settings class."""

    def test_default_settings(self) -> None:
        """Test default settings initialization."""
        settings = Settings(openai=OpenAIConfig(api_key="sk-test"))
        assert settings.environment == "development"
        assert settings.database is not None
        assert settings.openai is not None
        assert settings.security is not None
        assert settings.validation is not None
        assert settings.cache is not None
        assert settings.resilience is not None
        assert settings.observability is not None

    def test_is_production(self) -> None:
        """Test production environment check."""
        settings = Settings(
            environment="production",
            openai=OpenAIConfig(api_key="sk-test"),
        )
        assert settings.is_production
        assert not settings.is_development

    def test_is_development(self) -> None:
        """Test development environment check."""
        settings = Settings(
            environment="development",
            openai=OpenAIConfig(api_key="sk-test"),
        )
        assert settings.is_development
        assert not settings.is_production

    def test_nested_config_override(self) -> None:
        """Test overriding nested configurations."""
        settings = Settings(
            openai=OpenAIConfig(api_key="sk-test"),
            databases=[
                DatabaseConfig(
                    name="custom_db",
                    host="custom.host",
                    port=5433,
                )
            ],
            security=SecurityConfig(
                allow_write_operations=True,
            ),
        )
        assert settings.databases[0].host == "custom.host"
        assert settings.databases[0].port == 5433
        # Backward-compatible property returns the primary database
        assert settings.database.host == "custom.host"
        assert settings.security.allow_write_operations is True


class TestSettingsGlobalInstance:
    """Tests for global settings instance management."""

    def teardown_method(self) -> None:
        """Clean up after each test."""
        reset_settings()
        # Clean up environment variables
        for key in list(os.environ.keys()):
            if key.startswith(("DATABASE_", "OPENAI_", "SECURITY_")):
                del os.environ[key]

    def test_get_settings_creates_instance(self) -> None:
        """Test get_settings creates instance."""
        # Set required env var
        os.environ["OPENAI_API_KEY"] = "sk-test123"

        settings = get_settings()
        assert settings is not None
        assert isinstance(settings, Settings)

    def test_get_settings_returns_same_instance(self) -> None:
        """Test get_settings returns singleton."""
        os.environ["OPENAI_API_KEY"] = "sk-test123"

        settings1 = get_settings()
        settings2 = get_settings()
        assert settings1 is settings2

    def test_reset_settings(self) -> None:
        """Test reset_settings clears instance."""
        os.environ["OPENAI_API_KEY"] = "sk-test123"

        settings1 = get_settings()
        reset_settings()
        settings2 = get_settings()
        assert settings1 is not settings2

    def test_settings_from_environment(self) -> None:
        """Test loading settings from environment variables."""
        os.environ["OPENAI_API_KEY"] = "sk-env-key"
        os.environ["OPENAI_MODEL"] = "gpt-4"
        os.environ["DATABASE_HOST"] = "env.host.com"
        os.environ["SECURITY_MAX_ROWS"] = "5000"

        reset_settings()
        settings = get_settings()

        # Use get_secret_value() to access SecretStr content
        assert settings.openai.api_key.get_secret_value() == "sk-env-key"
        assert settings.openai.model == "gpt-4"
        assert settings.database.host == "env.host.com"
        assert settings.security.max_rows == 5000


class TestMultiDatabaseConfig:
    """Tests for the multi-database Settings.databases list (codex review fix)."""

    def teardown_method(self) -> None:
        """Clean up environment variables after each test."""
        reset_settings()
        for key in list(os.environ.keys()):
            if key.startswith(("DATABASES", "DATABASE_", "OPENAI_", "SECURITY_")):
                del os.environ[key]

    def test_single_database_fallback(self) -> None:
        """Without DATABASES, one database is built from DATABASE_* vars."""
        os.environ["OPENAI_API_KEY"] = "sk-test"
        os.environ["DATABASE_NAME"] = "primary_db"

        settings = Settings()
        assert len(settings.databases) == 1
        assert settings.databases[0].name == "primary_db"
        assert settings.database.name == "primary_db"  # backward-compat property

    def test_multiple_databases_from_json_env(self) -> None:
        """DATABASES env var accepts a JSON array of database configs."""
        os.environ["OPENAI_API_KEY"] = "sk-test"
        os.environ["DATABASES"] = (
            '[{"name":"db1","host":"h1"},{"name":"db2","host":"h2","port":5433}]'
        )

        settings = Settings()
        assert len(settings.databases) == 2
        assert settings.databases[0].name == "db1"
        assert settings.databases[0].host == "h1"
        assert settings.databases[1].name == "db2"
        assert settings.databases[1].port == 5433

    def test_databases_invalid_json_rejected(self) -> None:
        """A malformed DATABASES value must fail fast instead of being ignored."""
        os.environ["OPENAI_API_KEY"] = "sk-test"
        os.environ["DATABASES"] = "not-json"

        # pydantic-settings rejects undecodable env values before validators run
        with pytest.raises(SettingsError):
            Settings()

    def test_databases_duplicate_names_rejected(self) -> None:
        """Duplicate database names must be rejected."""
        os.environ["OPENAI_API_KEY"] = "sk-test"
        os.environ["DATABASES"] = '[{"name":"dup"},{"name":"dup"}]'

        with pytest.raises(Exception, match="unique"):
            Settings()

    def test_databases_empty_rejected(self) -> None:
        """An empty database list must be rejected."""
        os.environ["OPENAI_API_KEY"] = "sk-test"

        with pytest.raises(Exception, match="At least one database"):
            Settings(databases=[])

    def test_resilience_concurrency_limits(self) -> None:
        """ResilienceConfig exposes concurrency limits for the rate limiter."""
        config = ResilienceConfig(max_concurrent_queries=42, max_concurrent_llm_calls=7)
        assert config.max_concurrent_queries == 42
        assert config.max_concurrent_llm_calls == 7


class TestSecurityConfigControls:
    """Tests for configurable security controls (codex review fix)."""

    def test_default_security_controls(self) -> None:
        """By default no tables/columns are blocked and EXPLAIN is disallowed."""
        config = SecurityConfig()
        assert config.blocked_tables == []
        assert config.blocked_columns == []
        assert config.allow_explain is False

    def test_blocked_tables_from_comma_separated_env(self) -> None:
        """SECURITY_BLOCKED_TABLES accepts comma-separated values."""
        os.environ["SECURITY_BLOCKED_TABLES"] = "salaries, audit_log ,"
        config = SecurityConfig()
        assert config.blocked_tables == ["salaries", "audit_log"]

    def test_blocked_columns_from_env(self) -> None:
        """SECURITY_BLOCKED_COLUMNS accepts table-qualified columns."""
        os.environ["SECURITY_BLOCKED_COLUMNS"] = "users.ssn,password"
        config = SecurityConfig()
        assert config.blocked_columns == ["users.ssn", "password"]

    def test_allow_explain_from_env(self) -> None:
        """SECURITY_ALLOW_EXPLAIN toggles the EXPLAIN policy."""
        os.environ["SECURITY_ALLOW_EXPLAIN"] = "true"
        config = SecurityConfig()
        assert config.allow_explain is True

    def test_blocked_functions_from_comma_separated_env(self) -> None:
        """SECURITY_BLOCKED_FUNCTIONS accepts comma-separated values (was broken)."""
        os.environ["SECURITY_BLOCKED_FUNCTIONS"] = "pg_sleep,lo_import"
        config = SecurityConfig()
        assert config.blocked_functions == ["pg_sleep", "lo_import"]

    def test_observability_log_format_default_is_json(self) -> None:
        """log_format default matches the design spec (json)."""
        config = ObservabilityConfig()
        assert config.log_format == "json"
