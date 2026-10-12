# Test-only CONNECT proxy acting as a resolver: `*.vmon.test:443` -> 127.0.0.1:$TLS_PORT.
# TLS (SNI, certificate, ALPN) stays end-to-end between the SDK and vmon.
import asyncio, os, sys
TLS_PORT = int(os.environ.get("VMON_TLS_PORT", "3463"))
async def pipe(r, w):
    try:
        while data := await r.read(65536):
            w.write(data); await w.drain()
    finally:
        w.close()
async def handle(r, w):
    line = (await r.readline()).decode()
    while (await r.readline()) not in (b"\r\n", b""): pass
    method, target, _ = line.split()
    host, port = target.rsplit(":", 1)
    if method != "CONNECT" or not (host == "vmon.test" or host.endswith(".vmon.test")) or port != "443":
        w.write(b"HTTP/1.1 403 Forbidden\r\n\r\n"); await w.drain(); w.close(); return
    ur, uw = await asyncio.open_connection("127.0.0.1", TLS_PORT)
    print(f"CONNECT {target} -> 127.0.0.1:{TLS_PORT}", flush=True)
    w.write(b"HTTP/1.1 200 Connection Established\r\n\r\n"); await w.drain()
    await asyncio.gather(pipe(r, uw), pipe(ur, w), return_exceptions=True)
async def main():
    srv = await asyncio.start_server(handle, "127.0.0.1", int(sys.argv[1]))
    print("resolver proxy ready", flush=True)
    async with srv: await srv.serve_forever()
asyncio.run(main())
