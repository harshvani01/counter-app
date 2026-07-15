"""Tiny stdlib load tester for the counter app.

Usage:
    python loadtest.py [BASE_URL] [TOTAL_REQUESTS] [CONCURRENCY]

Example:
    python loadtest.py http://tpproject.duckdns.org 2000 100

It resets the counter, fires TOTAL_REQUESTS concurrent increments, then reads
the counter back and reports how many increments were *lost* (succeeded HTTP
requests whose increment did not make it into the counter). For an atomic
Redis INCR this must always be 0, no matter how hard you flood it.
"""

import json
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://tpproject.duckdns.org"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 2000  # total requests
C = int(sys.argv[3]) if len(sys.argv) > 3 else 100  # concurrency


def post(url: str) -> bool:
    req = urllib.request.Request(url, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status == 200
    except Exception:
        return False


def counter() -> int:
    with urllib.request.urlopen(BASE + "/api/counter", timeout=15) as r:
        return json.load(r)["value"]


def main() -> None:
    print(f"target={BASE}  requests={N}  concurrency={C}")
    post(BASE + "/api/counter/reset")
    start = counter()

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=C) as ex:
        succ = sum(ex.map(post, [BASE + "/api/counter/increment"] * N))
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
