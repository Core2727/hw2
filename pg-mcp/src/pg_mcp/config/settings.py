"""Configuration management for PostgreSQL MCP Server.

This module defines all configuration settings using Pydantic for validation
and type safety. Configuration is loaded from environment variables with
sensible defaults.
"""

import json
from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def _parse_str_list(v: str | list[str]) -> list[str]:
    """Parse a JSON array or a comma-separated string into a list.

    Environment variables carrying list values arrive as raw strings here
    (the NoDecode annotation disables pydantic-settings' automatic JSON
    decoding), so both formats must be handled.

    Args:
        v: JSON array string (e.g. '["a","b"]'), comma-separated string
            (e.g. 'a, b'), or an already-parsed list.

    Returns:
        list[str]: Parsed and stripped list of strings.
    """
    if isinstance(v, str):
        stripped = v.strip()
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    return [str(item) for item in parsed]
            except json.JSONDecodeError:
                pass
        return [item.strip() for item in v.split(",") if item.strip()]
    return v


# Annotated type for list-of-string settings loaded from environment variables:
# NoDecode disables automatic JSON decoding so the raw string reaches our parser.
CommaSeparatedList = Annotated[list[str], BeforeValidator(_parse_str_list), NoDecode]


class DatabaseConfig(BaseSettings):
    """PostgreSQL database connection configuration."""

    # Every nested config must declare env_file itself: pydantic-settings only
    # applies env_file to the class whose model_config declares it — a parent
    # Settings env_file does NOT propagate to child BaseSettings classes.
    model_config = SettingsConfigDict(
        env_prefix="DATABASE_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, ge=1, le=65535, description="Database port")
    name: str = Field(default="postgres", description="Database name")
    user: str = Field(default="postgres", description="Database user")
    password: str = Field(default="", description="Database password")

    # Connection pool settings
    min_pool_size: int = Field(default=5, ge=1, le=100, description="Minimum pool size")
    max_pool_size: int = Field(default=20, ge=1, le=100, description="Maximum pool size")
    pool_timeout: float = Field(
        default=30.0, ge=1.0, le=300.0, description="Pool acquire timeout in seconds"
    )
    command_timeout: float = Field(
        default=30.0, ge=1.0, le=300.0, description="Command execution timeout in seconds"
    )

    @property
    def dsn(self) -> str:
        """Build PostgreSQL DSN connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def safe_dsn(self) -> str:
        """Build DSN with masked password for logging."""
        return f"postgresql://{self.user}:***@{self.host}:{self.port}/{self.name}"


class OpenAIConfig(BaseSettings):
    """OpenAI API configuration."""

    model_config = SettingsConfigDict(
        env_prefix="OPENAI_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    api_key: SecretStr = Field(default=SecretStr(""), description="OpenAI API key")
    model: str = Field(default="gpt-4o-mini", description="Model to use for SQL generation")
    max_tokens: int = Field(default=2000, ge=100, le=4096, description="Maximum tokens in response")
    temperature: float = Field(
        default=0.0, ge=0.0, le=2.0, description="Temperature for response randomness"
    )
    timeout: float = Field(
        default=30.0, ge=5.0, le=120.0, description="API request timeout in seconds"
    )

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, v: SecretStr) -> SecretStr:
        """Validate API key is not empty and has correct format."""
        api_key_str = v.get_secret_value()
        if not api_key_str or not api_key_str.strip():
            raise ValueError("OpenAI API key must not be empty")
        if not api_key_str.startswith("sk-"):
            raise ValueError("OpenAI API key must start with 'sk-'")
        return v


class SecurityConfig(BaseSettings):
    """Security and access control configuration."""

    model_config = SettingsConfigDict(
        env_prefix="SECURITY_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    allow_write_operations: bool = Field(
        default=False, description="Allow write operations (INSERT, UPDATE, DELETE)"
    )
    blocked_functions: CommaSeparatedList = Field(
        default_factory=lambda: [
            "pg_sleep",
            "pg_read_file",
            "pg_write_file",
            "lo_import",
            "lo_export",
        ],
        description="List of blocked PostgreSQL functions",
    )
    max_rows: int = Field(default=10000, ge=1, le=100000, description="Maximum rows to return")
    max_execution_time: float = Field(
        default=30.0, ge=1.0, le=300.0, description="Maximum query execution time in seconds"
    )
    readonly_role: str | None = Field(
        default=None, description="PostgreSQL role to switch to for read-only access"
    )
    safe_search_path: str = Field(
        default="public", description="Safe search_path to set during query execution"
    )
    blocked_tables: CommaSeparatedList = Field(
        default_factory=list,
        description="List of tables that queries must not access",
    )
    blocked_columns: CommaSeparatedList = Field(
        default_factory=list,
        description="List of columns (optionally 'table.column') that queries must not access",
    )
    allow_explain: bool = Field(
        default=False,
        description="Whether EXPLAIN statements are allowed",
    )


class ValidationConfig(BaseSettings):
    """Query validation configuration."""

    model_config = SettingsConfigDict(
        env_prefix="VALIDATION_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    max_question_length: int = Field(
        default=10000, ge=1, le=50000, description="Maximum question length in characters"
    )
    min_confidence_score: int = Field(
        default=70, ge=0, le=100, description="Minimum confidence score (0-100)"
    )

    # Result validation settings
    enabled: bool = Field(default=True, description="Enable result validation using LLM")
    sample_rows: int = Field(
        default=5, ge=1, le=100, description="Number of sample rows to include in validation"
    )
    timeout_seconds: float = Field(
        default=10.0, ge=1.0, le=60.0, description="Result validation timeout in seconds"
    )
    confidence_threshold: int = Field(
        default=70, ge=0, le=100, description="Minimum confidence for acceptable results"
    )


class CacheConfig(BaseSettings):
    """Schema cache configuration."""

    model_config = SettingsConfigDict(
        env_prefix="CACHE_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    schema_ttl: int = Field(
        default=3600, ge=60, le=86400, description="Schema cache TTL in seconds"
    )
    max_size: int = Field(default=100, ge=1, le=1000, description="Maximum cache entries")
    enabled: bool = Field(default=True, description="Enable schema caching")


class ResilienceConfig(BaseSettings):
    """Resilience and fault tolerance configuration."""

    model_config = SettingsConfigDict(
        env_prefix="RESILIENCE_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum retry attempts")
    retry_delay: float = Field(
        default=1.0, ge=0.1, le=10.0, description="Initial retry delay in seconds"
    )
    backoff_factor: float = Field(
        default=2.0, ge=1.0, le=10.0, description="Exponential backoff factor"
    )
    circuit_breaker_threshold: int = Field(
        default=5, ge=1, le=100, description="Failures before circuit opens"
    )
    circuit_breaker_timeout: float = Field(
        default=60.0, ge=10.0, le=300.0, description="Circuit breaker timeout in seconds"
    )
    max_concurrent_queries: int = Field(
        default=10, ge=1, le=1000, description="Maximum concurrent database queries"
    )
    max_concurrent_llm_calls: int = Field(
        default=5, ge=1, le=1000, description="Maximum concurrent LLM API calls"
    )


class ObservabilityConfig(BaseSettings):
    """Observability and monitoring configuration."""

    model_config = SettingsConfigDict(
        env_prefix="OBSERVABILITY_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    metrics_enabled: bool = Field(default=True, description="Enable Prometheus metrics")
    metrics_port: int = Field(
        default=9090, ge=1024, le=65535, description="Metrics HTTP server port"
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", description="Logging level"
    )
    log_format: Literal["json", "text"] = Field(
        default="json", description="Log format (json for production/log aggregators)"
    )


class Settings(BaseSettings):
    """Main application settings aggregating all config sections."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    environment: Literal["development", "staging", "production"] = Field(
        default="development", description="Application environment"
    )

    # Nested configurations
    databases: list[DatabaseConfig] = Field(
        default_factory=lambda: [DatabaseConfig()],
        description=(
            "Database configurations. Set DATABASES env var as a JSON array for "
            "multiple databases; otherwise a single database is built from DATABASE_* vars."
        ),
    )
    openai: OpenAIConfig = Field(default_factory=OpenAIConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    resilience: ResilienceConfig = Field(default_factory=ResilienceConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    @field_validator("databases", mode="before")
    @classmethod
    def parse_databases(
        cls, v: str | list[DatabaseConfig | dict[str, Any]] | None
    ) -> list[DatabaseConfig | dict[str, Any]]:
        """Parse databases from a JSON string (env var) or pass through a list.

        When the DATABASES environment variable is set to a JSON array, it is
        decoded here. When nothing is configured, a single database is built
        from the DATABASE_* environment variables (DatabaseConfig defaults).
        """
        if v is None:
            return [DatabaseConfig()]
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
            except json.JSONDecodeError as e:
                raise ValueError(f"DATABASES must be a valid JSON array: {e}") from e
            if not isinstance(parsed, list):
                raise ValueError("DATABASES must be a JSON array of database objects")
            return parsed
        return v

    @model_validator(mode="after")
    def validate_databases(self) -> "Settings":
        """Ensure at least one database is configured with unique names."""
        if not self.databases:
            raise ValueError("At least one database must be configured")
        names = [db.name for db in self.databases]
        if len(names) != len(set(names)):
            duplicates = sorted({n for n in names if names.count(n) > 1})
            raise ValueError(f"Database names must be unique, duplicates: {duplicates}")
        return self

    @property
    def database(self) -> DatabaseConfig:
        """Get the primary (first) database configuration.

        Kept for backward compatibility with single-database deployments.
        """
        return self.databases[0]

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "production"

    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == "development"


# Global settings instance
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get or create global settings instance.

    Returns:
        Settings: The global settings instance.
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Reset global settings instance. Useful for testing."""
    global _settings
    _settings = None
