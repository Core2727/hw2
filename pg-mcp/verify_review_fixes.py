"""Codex Review 修复验证脚本（作业演示用）。

在一台装有 Python 3.14+、已按 README 安装依赖的机器上运行：

    # 演示 1+2+3（无需真实 LLM，OPENAI_API_KEY 填 sk-test 即可）：
    #   前置：.env 里配好以下 4 项（嵌套配置已支持读取 .env）：
    #     OPENAI_API_KEY=sk-test
    #     DATABASE_NAME=mydb
    #     DATABASE_PASSWORD=<你的postgres密码>
    #     VALIDATION_MAX_QUESTION_LENGTH=100
    #   （不想改 .env 的话，也可以在 PowerShell 同一窗口用
    #     $env:OPENAI_API_KEY="sk-test" 等设置这 4 个环境变量）
    uv run python verify_review_fixes.py --basic

    # 演示 4（需要真实 OPENAI_API_KEY，且 .env 或环境变量设
    # SECURITY_BLOCKED_TABLES=salaries）：
    uv run python verify_review_fixes.py --blocked-table

前置：Postgres 已就绪、目标库已建好。

本脚本不依赖 mcp 客户端库（mcp 的 stdio_client 在 Windows + Python 3.14
组合下存在挂起问题），而是用裸 subprocess 直接以换行分隔的 JSON-RPC 与
stdio MCP 服务器通信。服务器的日志走 stderr 并直接透传到终端显示。
"""

import argparse
import json
import queue
import subprocess
import sys
import threading


class StdioMcpClient:
    """Minimal line-delimited JSON-RPC client for a stdio MCP server."""

    def __init__(self) -> None:
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "pg_mcp"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            # stderr 直通终端：服务器日志(JSON)照常显示，不污染协议通道
            stderr=None,
        )
        self._lines: queue.Queue[str] = queue.Queue()
        self._next_id = 0
        threading.Thread(target=self._pump_stdout, daemon=True).start()

    def _pump_stdout(self) -> None:
        assert self.proc.stdout is not None
        for raw in self.proc.stdout:
            line = raw.decode("utf-8", errors="replace").strip()
            if line:
                self._lines.put(line)

    def request(self, method: str, params: dict, timeout: float = 90.0) -> dict:
        """Send a JSON-RPC request and return the matching result."""
        self._next_id += 1
        rid = self._next_id
        payload = {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}
        self._send(payload)
        while True:
            try:
                line = self._lines.get(timeout=timeout)
            except queue.Empty:
                raise RuntimeError(
                    f"等待 {method} 响应超时({timeout}s)——服务器无回应"
                ) from None
            msg = json.loads(line)
            if msg.get("id") != rid:
                continue  # 通知或其它请求的响应，跳过
            if "error" in msg:
                raise RuntimeError(f"{method} 返回错误: {msg['error']}")
            return msg.get("result", {})

    def notify(self, method: str, params: dict | None = None) -> None:
        payload: dict = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        self._send(payload)

    def _send(self, payload: dict) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
        self.proc.stdin.flush()

    def initialize(self) -> None:
        self.request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "verify-script", "version": "1.0"},
            },
        )
        self.notify("notifications/initialized")

    def list_tools(self) -> list[str]:
        result = self.request("tools/list", {})
        return [t["name"] for t in result.get("tools", [])]

    def call_tool(self, name: str, arguments: dict) -> dict:
        result = self.request("tools/call", {"name": name, "arguments": arguments})
        text = result["content"][0]["text"]
        return json.loads(text)

    def close(self) -> None:
        try:
            self.proc.kill()
        except OSError:
            pass


def banner(title: str) -> None:
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def check(label: str, ok: bool) -> None:
    print(f"\n>>> {'PASS [OK]' if ok else 'FAIL [X]'} -- {label}")


def run_scenarios(scenarios: list[str]) -> None:
    print("正在启动 pg-mcp 服务器（冷启动 + 建连接池约需几秒）...")
    client = StdioMcpClient()
    try:
        client.initialize()
        print("MCP 会话已建立。")

        if "tools" in scenarios:
            banner("演示 1 修复确认：health 工具已注册（review 2.3）")
            names = client.list_tools()
            print("已注册工具:", names)
            check("health 工具存在", "health" in names)

        if "health" in scenarios:
            banner("演示 2 health 工具：熔断器/限流器/安全配置运行状态（review 2.3）")
            payload = client.call_tool("health", {})
            print(json.dumps(payload, ensure_ascii=True, indent=2))
            check(
                "status=healthy 且含 circuit_breaker / rate_limiter / security",
                payload.get("status") == "healthy"
                and "circuit_breaker" in payload
                and "rate_limiter" in payload
                and "security" in payload,
            )

        if "long_question" in scenarios:
            banner(
                "演示 3 超长问题守卫：提交超长问题应在调用 LLM 前被拒（review 1.2）"
            )
            result = client.call_tool(
                "query",
                {"question": "请告诉我所有用户的详细信息 " * 10},
            )
            payload = result
            print(json.dumps(payload, ensure_ascii=True, indent=2))
            check(
                "error.code=question_too_long 且 tokens_used=0（未消耗 LLM）",
                payload.get("error", {}).get("code") == "question_too_long"
                and payload.get("tokens_used") == 0,
            )

        if "blocked_table" in scenarios:
            banner("演示 4 封锁表：提问涉及被封禁的表应返回 security_violation（review 1.2）")
            import os

            blocked = os.environ.get("SECURITY_BLOCKED_TABLES", "")
            print(f"当前封锁表: {blocked!r}")
            result = client.call_tool(
                "query",
                {
                    "question": (
                        f"查询 {blocked.split(',')[0].strip() or 'salaries'} 表的全部数据"
                    )
                },
            )
            payload = result
            print(json.dumps(payload, ensure_ascii=True, indent=2))
            check(
                "error.code=security_violation",
                payload.get("error", {}).get("code") == "security_violation",
            )
    finally:
        client.close()

    banner("全部演示结束")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--basic",
        action="store_true",
        help="演示 1-3：工具注册 + health + 超长问题守卫（无需真实 LLM key）",
    )
    group.add_argument(
        "--blocked-table",
        action="store_true",
        help="演示 4：封锁表拒绝（需真实 OPENAI_API_KEY 与 SECURITY_BLOCKED_TABLES）",
    )
    args = parser.parse_args()

    scenarios = (
        ["tools", "health", "long_question"] if args.basic else ["blocked_table"]
    )
    run_scenarios(scenarios)


if __name__ == "__main__":
    main()
