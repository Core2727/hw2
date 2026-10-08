# Codex Review 缺陷修复说明

本文档对照 `specs/w5/0006-pg-mcp-code-review.md`（第 5 章第 4 节）中列出的缺失功能，
逐项说明修复内容、涉及文件与验证方式。

**修复后状态：306 个单元测试全部通过（修复前基线 247 通过 / 1 失败），ruff 检查零告警。**

---

## 修复总览

| Review 建议 | 缺陷 | 修复 | 关键文件 |
|---|---|---|---|
| 1.1 多数据库合规 | 单一执行器，请求可能访问错误数据库 | `Settings.databases` 列表 + 每库独立连接池/执行器 + 按请求路由 | `config/settings.py`、`db/pool.py`、`server.py`、`services/orchestrator.py` |
| 1.2 恢复安全控制 | 表/列封锁、EXPLAIN 策略未接线；问题长度不设防 | 三个安全配置落地到 SQLValidator；超长问题在调用 LLM 前拒绝 | `config/settings.py`、`services/sql_validator.py`、`models/errors.py`、`services/orchestrator.py` |
| 2.1 速率限制整合 | RateLimiter 只存在于设计中 | LLM 调用与 DB 执行全部分别套限流闸，上限来自配置 | `resilience/rate_limiter.py`、`services/orchestrator.py`、`server.py` |
| 2.2 重试/退避 | `resilience/retry.py` 未实现 | 新增 `with_retry` 通用重试（指数退避+可重试分类），应用于 LLM 与 DB 两条路径 | `resilience/retry.py`（新增）、`services/sql_executor.py`、`services/orchestrator.py` |
| 2.3 指标与追踪 | 指标从不发射；无健康检查 | 查询计数/LLM 调用/延迟直方图/SQL 拒绝计数全部接入；request_id 贯穿日志；新增 `health` 工具 | `observability/metrics.py`、`services/orchestrator.py`、`server.py` |
| 3.1 模型缺陷 | 重复 `to_dict`；`tokens_used` 可能缺失；`ErrorDetail` 双定义 | 删除重复定义；`to_dict` 保证 `tokens_used` 恒在；ErrorDetail 收敛为唯一定义 | `models/query.py`、`models/errors.py` |
| 3.2 未用配置字段 | retry_delay/backoff_factor 等字段从未被读取 | 全部接线（重试参数、并发上限、封锁清单）；移除冗余熔断器实例 | `services/orchestrator.py`、`server.py` |
| 4.1/4.2 测试补充 | 重试/限流/多库路由/健康检查均无测试 | 新增 59 个单元测试（8 个测试文件），全 mock、确定性 | `tests/unit/` |

---

## Priority 1：多数据库与安全控制

### 1.1 多数据库合规

**改动：**

- `Settings.databases` 改为 `list[DatabaseConfig]`：
  - `DATABASES` 环境变量接受 JSON 数组配置多个库；
  - 未设置时回退为单库（由 `DATABASE_*` 变量构造），老配置不受影响；
  - 校验库名唯一、列表非空；保留 `settings.database` property（返回第一库）向后兼容。
- `db/pool.py` 新增 `create_pools(configs) -> dict[str, Pool]` / `close_pools`，
  每个库一个连接池。
- `server.py` lifespan 为每个库创建独立 `SQLExecutor`（绑定各自的 pool 与 db_config）。
- `QueryOrchestrator` 构造签名从单 `sql_executor` 改为 `sql_executors: dict[str, SQLExecutor]`；
  执行阶段按 `_resolve_database()` 解析出的库名选择执行器；请求指定不存在的库时
  返回 `database_error` 并附带可用库列表。

**验证：** `DATABASES` 配两个库，query 工具指定 `database="db2"`，
日志显示仅 db2 的执行器被调用；指定不存在的库名返回错误并列出可用库。
单测：`tests/unit/test_orchestrator.py::TestMultiDatabaseRouting`（3 例）、
`tests/unit/test_config.py::TestMultiDatabaseConfig`（6 例）。

### 1.2 恢复安全控制

**改动：**

- `SecurityConfig` 新增 `blocked_tables`、`blocked_columns`、`allow_explain`
  三个字段（支持逗号分隔或 JSON 数组两种环境变量格式）。
- `server.py` 将三者传入 `SQLValidator`（此前构造时完全未传，等于永远不封锁）。
- `SQLValidator`：
  - 新增 `validate_result_or_raise()` —— 校验失败抛出原始异常
    （`SecurityViolationError` / `SQLParseError`），不再把安全违规误报成解析错误；
  - 封锁列采用保守策略：限定名 `table.column` 命中时，该裸列名在所有表上也拒绝。
- 问题长度守卫：`orchestrator.execute_query` 第 0 步检查
  `len(question) > validation.max_question_length`，超长直接抛
  `QuestionTooLongError`（新增异常，错误码 `question_too_long`），**不消耗 LLM 调用**。
- 顺带修复一个潜伏 bug：list 类型的环境变量（如 `SECURITY_BLOCKED_FUNCTIONS=a,b`）
  此前会被 pydantic-settings 按 JSON 解码直接抛异常——
  `.env.example` 里文档化的写法从来不可用。现以
  `CommaSeparatedList`（`NoDecode` + `BeforeValidator`）类型修复，
  逗号与 JSON 数组两种格式均可用。

**验证：** `SECURITY_BLOCKED_TABLES=salaries` 启动后提问涉及 salaries 表，
返回 `security_violation`；`VALIDATION_MAX_QUESTION_LENGTH=100` 下提交 101 字符问题
立即返回 `question_too_long` 且不产生 LLM token 消耗。
单测：`test_sql_validator.py::TestConfiguredSecurityControls`（7 例）、
`test_orchestrator.py::TestQuestionLengthGuard`（3 例）、
`test_config.py::TestSecurityConfigControls`（5 例）。

---

## Priority 2：弹性与可观测性整合

### 2.1 速率限制

**改动：**

- `MultiRateLimiter` 的上限改为来自配置
  `RESILIENCE_MAX_CONCURRENT_QUERIES` / `RESILIENCE_MAX_CONCURRENT_LLM_CALLS`
  （原为硬编码，配置字段属于"从未使用的字段"缺陷）。
- orchestrator 中：
  - LLM 生成路径套 `async with self.rate_limiter.for_llm()`；
  - DB 执行路径套 `async with self.rate_limiter.for_queries()`；
  - 结果校验（LLM 辅助调用）同样纳入限流。

**验证：** 把 `RESILIENCE_MAX_CONCURRENT_QUERIES=1` 调小后并发发多个请求，
`health` 工具的 `rate_limiter.queries` 里可观察到 `active` 不超过上限。
单测：`test_orchestrator.py::TestRateLimiterIntegration`（3 例）。

### 2.2 重试/退避

**改动：**

- 新增 `src/pg_mcp/resilience/retry.py`：`with_retry()` 通用异步重试，
  指数退避（延迟 = `retry_delay * backoff_factor ** attempt`），
  支持 retryable 类型列表 / 谓词过滤、`on_retry` 回调、可注入 sleep（测试确定性）。
- 瞬态判定：
  - **DB**：`sql_executor.py` 为 `DatabaseError` 附加 `details["transient"]`，
    依据 SQLSTATE 分类（`08xxx` 连接类、`40001` 序列化冲突、`40P01` 死锁）——
    语法错误等永久性失败不重试；
  - **LLM**：仅超时（`LLMTimeoutError`）与服务不可用（`LLMUnavailableError`）重试。
- 参数全部来自 `ResilienceConfig`（max_retries / retry_delay / backoff_factor）。

**验证：** 单测 `test_retry.py`（8 例，覆盖退避序列、耗尽抛最后错、
非瞬态立即抛、空元组永不重试等）；
`test_orchestrator.py::TestDatabaseTransientRetry`（2 例：瞬态错误重试后成功 / 非瞬态只调一次）。

### 2.3 指标与追踪

**改动：**

- orchestrator 全链路埋点（此前 MetricsCollector 从未被调用）：
  - `pg_mcp_queries_total{status,database}` 查询计数；
  - `pg_mcp_query_duration_seconds` 查询总时长直方图；
  - `pg_mcp_llm_calls_total{operation}` / `pg_mcp_llm_latency_seconds` LLM 调用与延迟；
  - `pg_mcp_sql_rejected_total{reason}` 被拒绝 SQL 计数。
- request_id 贯穿：`server.py` 的 query 工具用 `request_context()` 包裹，
  orchestrator 内所有日志行带同一 request_id。
- 新增 `health` MCP 工具：返回 status / databases / uptime_seconds /
  circuit_breaker 状态 / rate_limiter 并发 / 安全配置摘要 / metrics 开关。

**验证：** `OBSERVABILITY_METRICS_ENABLED=true` 启动后 `curl http://localhost:9090/metrics`
可见上述指标随查询次数增长；调用 health 工具见完整运行状态。
单测：`test_orchestrator.py::TestMetricsInstrumentation`（5 例，delta 断言）、
`test_server.py::TestHealthTool`（2 例）。

---

## Priority 3：代码质量

**改动：**

- `models/errors.py` 的 `ErrorDetail` 为唯一定义（含 `to_dict()`），
  `models/query.py` 删除本地重复类、改为 import（重复定义曾导致 pydantic 校验混乱）。
- `QueryResponse.to_dict` 删除文件末尾的重复版本，保留确保
  `tokens_used` 恒存在（None→0）的正确版本；新增单测锁定"序列化恒含 tokens_used"与"全局仅一份 to_dict 定义"。
- 冗余熔断器实例移除：熔断器由 orchestrator 统一创建并持有，
  server 侧只读引用（`_orchestrator.circuit_breaker`）供 health 使用。
- 未用配置字段全部接线或落实：`max_concurrent_*` → 限流上限；
  `retry_delay` / `backoff_factor` → 重试参数；`blocked_*` / `allow_explain` → 校验器；
  `low_confidence` 响应字段按 `validation.min_confidence_score` 真实计算
  （置信度低于阈值时为 true，客户端可据此提示）。

### 追加修复：嵌套配置读取 .env 失效

**缺陷：** `DatabaseConfig`/`OpenAIConfig`/`SecurityConfig` 等嵌套配置类各自持有
`env_prefix`，但 pydantic-settings 的 `env_file` 只作用于声明它的那个类——顶层
`Settings` 的 `env_file=".env"` **不会**传播给子配置类。结果是 `.env` 里写的
`DATABASE_NAME`、`OPENAI_API_KEY` 等全部静默失效（只能靠真实环境变量），且无任何提示。

**修复：** 每个 `SettingsConfigDict` 显式声明 `env_file=".env", extra="ignore"`
（`extra="ignore"` 使各段忽略 .env 中属于其他段的键）。环境变量优先级仍高于 .env，
原有用法不受影响。

---

## Priority 4：测试

**新增/重写（净增 59 例，247→306）：**

| 文件 | 内容 |
|---|---|
| `tests/unit/test_retry.py` | 新增，8 例：成功路径零延迟 / 退避序列 / 回调 / 耗尽 / 零重试 / 非瞬态直抛 / 空元组 |
| `tests/unit/test_server.py` | 新增，5 例：health 未初始化与运行态、query 工具三守卫路径 |
| `tests/unit/test_orchestrator.py` | 重写，35 例：问题长度守卫、low_confidence、多库路由、指标埋点（delta 断言）、限流集成、瞬态重试 |
| `tests/unit/test_config.py` | 追加 11 例：多库配置（JSON/回退/重名拒绝）、安全控制解析（逗号 env、allow_explain） |
| `tests/unit/test_models.py` | 追加 5 例：序列化恒含 tokens_used、to_dict 唯一、ErrorDetail 一致性 |
| `tests/unit/test_sql_validator.py` | 追加 7 例：封锁表/列、EXPLAIN 双向、validate_result_or_raise 异常类型保留 |

全部基于 mock（AsyncMock/MagicMock），不依赖真实 OpenAI 或数据库，可离线确定性运行。

---

## 验证指南

### 环境准备（Windows）

```powershell
# 方式一（推荐）：uv 自动按 uv.lock 精确还原依赖（含 mcp==1.25.0 锁定）
winget install astral-sh.uv
cd w5\pg-mcp
uv sync
uv run pytest tests/unit/ -q          # 预期：306 passed
```

```powershell
# 方式二：pip（需 Python 3.14+，python.org 下载）
py -3.14 -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
pip install "mcp==1.25.0" "fastmcp==2.14.1"   # uv.lock 锁定版本，防止装到不兼容的 mcp 2.x
pytest tests/unit/ -q
```

### 端到端演示（可选，需 Docker Desktop + OpenAI key）

```powershell
copy .env.example .env    # 填入 OPENAI_API_KEY；按需加 SECURITY_BLOCKED_TABLES 等
docker compose up -d postgres
# 多库演示：在 .env 加 DATABASES JSON 数组（见 .env.example 尾部说明）
uv run python main.py     # stdio 方式启动，或在 Claude Desktop 里配置（见 CLAUDE_DESKTOP_SETUP.md）
```

### 关键演示点（对应 review 缺陷）

1. **安全封锁**：`.env` 设 `SECURITY_BLOCKED_TABLES=salaries` → 提问涉及该表 → 返回 `security_violation`。
2. **超长问题守卫**：`VALIDATION_MAX_QUESTION_LENGTH=100` → 提交超 100 字符问题 → 立即返回 `question_too_long`。
3. **多库路由**：`DATABASES` 配两库 → 指定 `database` 参数 → 日志显示命中的库；指定错误库名 → 返回可用库列表。
4. **指标**：`curl http://localhost:9090/metrics` → `pg_mcp_queries_total` 等指标随查询增长。
5. **健康检查**：调用 `health` 工具 → 返回熔断器/限流器/安全配置运行状态。
