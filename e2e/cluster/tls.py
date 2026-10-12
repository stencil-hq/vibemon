import os, time, httpx
from e2b import Sandbox, Template
assert "E2B_SANDBOX_URL" not in os.environ and "E2B_API_URL" not in os.environ
s = Sandbox.create("alp", timeout=300)
print("created", s.sandbox_id, "envd host:", s.get_host(49983))
r = s.commands.run("echo hello-over-tls; hostname")
print("exec:", r.stdout.split())
s.files.write("/tmp/tls.txt", "tls-file"); print("file:", s.files.read("/tmp/tls.txt"))

s.commands.run("sh -c 'while true; do printf \"HTTP/1.1 200 OK\\r\\nContent-Length: 2\\r\\n\\r\\nok\" | nc -l -p 8080; done' >/dev/null 2>&1 &")
time.sleep(0.5)
with httpx.Client(http2=True) as c:
    r = c.get(f"https://{s.get_host(49983)}/health")
    print("envd /health:", r.status_code, r.http_version, "server-cert ok")
    r = c.get(f"https://{s.get_host(8080)}/")
    print("port 8080:", r.status_code, r.http_version, r.text)
    r = c.get("https://api.vmon.test/health")
    print("api /health:", r.status_code, r.http_version)
s.pause(); print("paused"); s2 = Sandbox.connect(s.sandbox_id); print("resumed exec:", s2.commands.run("cat /tmp/tls.txt").stdout)
s2.kill(); print("killed")
