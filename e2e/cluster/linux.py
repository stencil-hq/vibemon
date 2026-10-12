import json, os, sys, time
from e2b import Sandbox, Volume
T = lambda: time.strftime("%H:%M:%S")
def sh(s, cmd, timeout=60):
    r = s.commands.run(cmd, timeout=timeout)
    return r.stdout.strip()
def probe(s, url):
    r = s.commands.run(f"curl -s -o /dev/null -m 6 -w '%{{http_code}}' {url} || true", timeout=30)
    return r.stdout.strip() or "fail"

# lifecycle: create, commands, files, pause, resume, kill
t = time.time(); s = Sandbox.create("alp", timeout=600)
print(T(), "create", s.sandbox_id, f"{time.time()-t:.1f}s")
print(T(), "uname:", sh(s, "uname -sr"), "| ip:", sh(s, "ip -4 -o addr show eth0 | awk '{print $4}'"), "| route:", sh(s, "ip route | head -1"))
s.files.write("/home/user/a.txt", "linux-file"); print(T(), "file:", s.files.read("/home/user/a.txt"))
print(T(), "open internet:", "example.com", probe(s, "https://example.com"), "| 1.1.1.1", probe(s, "https://1.1.1.1"))
t = time.time(); s.pause(); print(T(), f"paused in {time.time()-t:.1f}s")
t = time.time(); s = Sandbox.connect(s.sandbox_id); print(T(), f"resumed in {time.time()-t:.1f}s; file:", sh(s, "cat /home/user/a.txt"), "| net after resume:", probe(s, "https://example.com"))
s.kill(); print(T(), "killed")

# egress: allowOut domain + deny-all
e = Sandbox.create("alp", timeout=300, network={"allow_out": ["example.com"], "deny_out": ["0.0.0.0/0"]})
print(T(), "allowOut=[example.com] denyOut=[0/0]: example.com", probe(e, "https://example.com"), "http", probe(e, "http://example.com"), "| google.com", probe(e, "https://www.google.com"), "| 1.1.1.1", probe(e, "https://1.1.1.1"))
e.kill()
# egress: deny one CIDR
d = Sandbox.create("alp", timeout=300, network={"deny_out": ["1.1.1.1/32"]})
print(T(), "denyOut=[1.1.1.1/32]: 1.1.1.1", probe(d, "https://1.1.1.1"), "| 1.0.0.1", probe(d, "https://1.0.0.1"), "| example.com", probe(d, "https://example.com"))
d.kill()
# allow_internet_access=False
n = Sandbox.create("alp", timeout=300, allow_internet_access=False)
print(T(), "allow_internet_access=False: example.com", probe(n, "https://example.com"), "| 1.1.1.1", probe(n, "https://1.1.1.1"), "| ping", sh(n, "ping -c1 -W2 1.1.1.1 >/dev/null 2>&1 && echo ok || echo blocked"))
n.kill()

# volumes
name = f"vol{int(time.time())}"
v = Volume.create(name); print(T(), "volume", v.volume_id, v.name)
a = Sandbox.create("alp", timeout=300, volume_mounts={"/data": v})
print(T(), "mount:", sh(a, "mount | grep /data | cut -d' ' -f1-5"))
a.commands.run("echo from-a > /data/shared.txt")
a.kill()
b = Sandbox.create("alp", timeout=300, volume_mounts={"/data": name})
print(T(), "second sandbox reads:", sh(b, "cat /data/shared.txt"))

b.kill(); Volume.destroy(v.volume_id); print(T(), "volume destroyed")
