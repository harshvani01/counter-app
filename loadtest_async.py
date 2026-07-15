"""Async load tester for the counter app — one thread, thousands of sockets.

Usage:
    python loadtest_async.py [BASE_URL] [TOTAL_REQUESTS] [CONCURRENCY]

Example:
    python loadtest_async.py http://tpproject.duckdns.org 20000 1000

Unlike loadtest.py (which uses OS threads), this uses a single-threaded asyncio
event loop. Concurrency is bounded by a semaphore, NOT by threads, so you can
push it into the thousands without exhausting memory. The network and the
server become the bottleneck long before the client does.
"""

import asyncio
import json
import sys
import time
import urllib.request
from urllib.parse import urlparse

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://tpproject.duckdns.org"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 20000  # total requests
C = int(sys.argv[3]) if len(sys.argv) > 3 else 1000  # max in-flight at once

_u = urlparse(BASE)
HOST = _u.hostname
PORT = _u.port or 80

# A pre-built raw HTTP/1.1 request. Connection: close => server closes the
# socket after replying, so we can read to EOF simply.
REQUEST = (
    f"POST /api/counter/increment HTTP/1.1\r\n"
    f"Host: {HOST}\r\n"
    f"Content-Length: 0\r\n"
    f"Connection: close\r\n"
    f"\r\n"
).encode()


async def one_request(sem: asyncio.Semaphore) -> bool:
    """Open a socket, send one increment, return True if the server said 200."""
    async with sem:  # gate: at most C of these run concurrently
        writer = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(HOST, PORT), timeout=15
            )
            writer.write(REQUEST)
            await asyncio.wait_for(writer.drain(), timeout=15)
            status = await asyncio.wait_for(reader.readline(), timeout=15)
            await asyncio.wait_for(reader.read(), timeout=15)  # drain the body
            return b"200" in status
        except Exception:
            return False
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass


def counter() -> int:
    with urllib.request.urlopen(BASE + "/api/counter", timeout=15) as r:
        return json.load(r)["value"]


async def flood() -> int:
    sem = asyncio.Semaphore(C)
    tasks = [asyncio.create_task(one_request(sem)) for _ in range(N)]
    return sum(await asyncio.gather(*tasks))


def main() -> None:
    print(f"target={BASE}  requests={N}  concurrency={C}  (async, single thread)")
    urllib.request.urlopen(
        urllib.request.Request(BASE + "/api/counter/reset", method="POST"), timeout=15
    )
    start = counter()

    t0 = time.time()
    succ = asyncio.run(flood())
    dt = time.time() - t0

    end = counter()
    delta = end - start
    print(f"requests sent:       {N}")
    print(f"  succeeded (2xx):   {succ}")
    print(f"  failed:            {N - succ}")
    print(f"counter delta:       {delta}")
    print(f"lost updates:        {succ - delta}   <-- 0 for atomic INCR")
    print(f"time: {dt:.2f}s   throughput: {N / dt:.0f} req/s")


if __name__ == "__main__":
    main()
