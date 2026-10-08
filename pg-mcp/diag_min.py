import subprocess, sys, time, json

msg = json.dumps({
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2024-11-05", "capabilities": {},
               "clientInfo": {"name": "diag", "version": "0"}},
}) + "\n"

p = subprocess.Popen(
    [sys.executable, "echo_srv.py"],
    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
)
time.sleep(3)
p.stdin.write(msg.encode()); p.stdin.flush()
time.sleep(6)
print("最小服务器退出码:", p.poll())
p.kill()
out, err = p.communicate()
print("=== STDOUT ===")
print(out[:600].decode(errors="replace"))
print("=== STDERR ===")
print(err[:800].decode(errors="replace"))
