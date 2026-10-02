"""Tạo tải cho API để test HPA (tự mở rộng số pod).
  python scripts/loadtest.py --url http://localhost:8000 --concurrency 50 --seconds 120
Chạy trong cluster: xem scripts/test-scalability.sh (bước api).
"""
import argparse
import asyncio
import random
import statistics
import time

import httpx

PROVINCES = ["ha-noi", "ho-chi-minh", "da-nang", "can-tho", "hai-phong", "nghe-an", "thanh-hoa", "vinh-long",
             "an-giang", "lam-dong", "khanh-hoa", "quang-ninh", "hue", "dak-lak", "gia-lai", "bac-ninh"]


async def worker(client, base, until, lat, errors):
    while time.time() < until:
        p = random.choice(PROVINCES)
        path = random.choice([f"/api/trending?province={p}&rt_minutes={random.randint(30, 90)}",
                              "/api/provinces?minutes=15", "/api/national?limit=30", f"/api/mentions?province={p}"])
        t = time.perf_counter()
        try:
            r = await client.get(base + path)
            if r.status_code >= 500:
                errors.append(r.status_code)
        except Exception as e:  # noqa: BLE001
            errors.append(type(e).__name__)
        lat.append((time.perf_counter() - t) * 1000)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://music-api.music.svc.cluster.local")
    ap.add_argument("--concurrency", type=int, default=40)
    ap.add_argument("--seconds", type=int, default=180)
    a = ap.parse_args()
    lat, errors = [], []
    start = time.time()
    async with httpx.AsyncClient(timeout=15, limits=httpx.Limits(max_connections=a.concurrency)) as client:
        tasks = [asyncio.create_task(worker(client, a.url, start + a.seconds, lat, errors)) for _ in range(a.concurrency)]
        last = 0
        while any(not t.done() for t in tasks):
            await asyncio.sleep(10)
            n = len(lat)
            print(f"t={time.time() - start:5.0f}s  req/s={(n - last) / 10:7.1f}  "
                  f"p50={statistics.median(lat[last:] or [0]):6.0f}ms  errors={len(errors)}", flush=True)
            last = n
    q = statistics.quantiles(lat, n=20) if len(lat) > 20 else [0] * 19
    print(f"TOTAL {len(lat)} req in {a.seconds}s = {len(lat) / a.seconds:.1f} req/s | p50={statistics.median(lat):.0f}ms "
          f"p95={q[18]:.0f}ms | errors={len(errors)}")


if __name__ == "__main__":
    asyncio.run(main())
