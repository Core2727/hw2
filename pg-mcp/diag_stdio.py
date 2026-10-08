import subprocess, sys, time, json

msg = json.dumps({
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2024-11-05", "capabilities": {},
               "clientInfo": {"name": "diag", "version": "0"}},
}) + "\n"

p = subprocess.Popen(
    [sys.executable, "-m", "pg_mcp"],
    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
)
try:
    p.stdin.write(msg.encode()); p.stdin.flush()
    print("initialize 已发送")
except Exception as e:
    print("stdin 写入失败:", e)

time.sleep(4)
print("子进程退出码:", p.poll())
p.kill()
out, err = p.communicate()
print("=== STDOUT 前1000字节（协议通道，应只有一条JSON响应）===")
print(out[:1000].decode(errors="replace"))
print("=== STDERR 前2000字节（日志/报错）===")
print(err[:2000].decode(errors="replace"))
