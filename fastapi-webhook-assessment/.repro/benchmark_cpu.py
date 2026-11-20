"""Benchmark unchanged preprocessing and real HTTP health latency.

From the backend directory:
    uv run --extra dev python .repro/benchmark_cpu.py

The load driver and HTTP server run in separate processes. This benchmark
isolates preprocessing: no SQLite or downstream traffic is included.
"""

import argparse
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import asynccontextmanager
import json
import multiprocessing
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import httpx
from fastapi import FastAPI
import uvicorn

from app.processor import scrub_and_fingerprint

SAMPLES = [
    f"Customer user{i}@example.com SSN 123-45-6789 submitted ticket #{i}"
    for i in range(30)
]


def cpu_job(content):
    started = time.perf_counter()
    cleaned, fingerprint = scrub_and_fingerprint(content)
    return {"cleaned": cleaned, "fingerprint": fingerprint,
            "work_ms": (time.perf_counter() - started) * 1000}


def create_app(mode, workers):
    @asynccontextmanager
    async def lifespan(app):
        pool = None
        if mode == "thread":
            pool = ThreadPoolExecutor(max_workers=workers)
        elif mode == "process":
            pool = ProcessPoolExecutor(max_workers=workers,
                                       mp_context=multiprocessing.get_context("spawn"))
        app.state.pool = pool
        if pool:
            loop = asyncio.get_running_loop()
            await asyncio.gather(*(loop.run_in_executor(pool, cpu_job, SAMPLES[i])
                                   for i in range(workers)))
        yield
        if pool:
            pool.shutdown(wait=True, cancel_futures=True)

    app = FastAPI(lifespan=lifespan)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/run")
    async def run(jobs: int):
        if jobs not in (30, 60):
            return {"error": "jobs must be 30 or 60"}
        queue = asyncio.Queue(maxsize=jobs)
        for index in range(jobs):
            queue.put_nowait(index)
        results = [None] * jobs
        loop = asyncio.get_running_loop()

        async def consume():
            while not queue.empty():
                index = queue.get_nowait()
                if app.state.pool:
                    result = await loop.run_in_executor(app.state.pool, cpu_job,
                                                        SAMPLES[index % 30])
                else:
                    result = cpu_job(SAMPLES[index % 30])
                results[index] = result

        started = time.perf_counter()
        # At most `workers` submitted futures; executor backlog cannot grow.
        await asyncio.gather(*(consume() for _ in range(workers)))
        return {"elapsed_ms": (time.perf_counter() - started) * 1000,
                "results": results}

    return app


async def wait_ready(client, process):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Benchmark server exited; inspect .repro/cpu-*.log")
        try:
            if (await client.get("/health")).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.05)
    raise RuntimeError("Benchmark server did not become ready")


async def measure(client, jobs, expected):
    stop = asyncio.Event()
    health = []

    async def probe():
        while not stop.is_set():
            started = time.perf_counter()
            response = await client.get("/health")
            response.raise_for_status()
            health.append((time.perf_counter() - started) * 1000)
            await asyncio.sleep(0.05)

    task = asyncio.create_task(probe())
    try:
        response = await client.post("/run", params={"jobs": jobs})
        response.raise_for_status()
        result = response.json()
    finally:
        stop.set()
        await task
    assert len(result["results"]) == jobs
    for index, item in enumerate(result["results"]):
        assert (item["cleaned"], item["fingerprint"]) == expected[index % 30]
    return {"jobs": jobs, "elapsed_ms": round(result["elapsed_ms"], 3),
            "jobs_per_second": round(jobs / (result["elapsed_ms"] / 1000), 3),
            "health_ms": [round(value, 3) for value in health],
            "health_max_ms": round(max(health), 3),
            "health_over_50_ms": sum(value >= 50 for value in health),
            "work_median_ms": round(statistics.median(item["work_ms"] for item in result["results"]), 3)}


async def benchmark(rounds):
    expected = [scrub_and_fingerprint(sample) for sample in SAMPLES]
    durations = []
    for _ in range(9):
        started = time.perf_counter()
        assert scrub_and_fingerprint(SAMPLES[0]) == expected[0]
        durations.append((time.perf_counter() - started) * 1000)
    report = {
        "environment": {"python": sys.version, "platform": platform.platform(),
                        "logical_cpus": os.cpu_count(), "gil_enabled": getattr(sys, "_is_gil_enabled", lambda: True)()},
        "method": {"rounds_per_size": rounds, "batch_sizes": [30, 60],
                   "probe_interval_ms": 50, "hash_iterations": 120000,
                   "pool_state": "warmed before readiness", "multiprocessing_start": "spawn",
                   "note": "Independent HTTP load driver; original function; no database or downstream; every output checked."},
        "serial_call_median_ms": round(statistics.median(durations), 3),
        "serial_call_max_ms": round(max(durations), 3),
        "configurations": [],
    }
    configurations = [("inline", 1)] + [(mode, workers) for workers in (1, 2, 4)
                                          for mode in ("thread", "process")]
    for mode, workers in configurations:
        started = time.perf_counter()
        with (ROOT / f".repro/cpu-{mode}-{workers}.log").open("w") as log:
            process = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "--serve", "--mode", mode,
                 "--workers", str(workers)], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
            try:
                async with httpx.AsyncClient(base_url="http://127.0.0.1:8915", timeout=30,
                                              trust_env=False) as client:
                    await wait_ready(client, process)
                    startup_ms = (time.perf_counter() - started) * 1000
                    baseline = []
                    for _ in range(5):
                        tick = time.perf_counter()
                        (await client.get("/health")).raise_for_status()
                        baseline.append((time.perf_counter() - tick) * 1000)
                    runs = []
                    for _ in range(rounds):
                        for jobs in (30, 60):
                            runs.append(await measure(client, jobs, expected))
                config = {"mode": mode, "workers": workers,
                          "startup_including_pool_warmup_ms": round(startup_ms, 3),
                          "idle_health_max_ms": round(max(baseline), 3), "runs": runs,
                          "health_max_ms": max(run["health_max_ms"] for run in runs),
                          "health_over_50_ms": sum(run["health_over_50_ms"] for run in runs),
                          "total_probes": sum(len(run["health_ms"]) for run in runs)}
                for jobs in (30, 60):
                    config[f"batch_{jobs}_median_ms"] = round(statistics.median(
                        run["elapsed_ms"] for run in runs if run["jobs"] == jobs), 3)
                report["configurations"].append(config)
                (ROOT / ".repro/cpu-benchmark-results.json").write_text(json.dumps(report, indent=2) + "\n")
                print(json.dumps({key: value for key, value in config.items() if key != "runs"}), flush=True)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--mode", choices=("inline", "thread", "process"))
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()
    if args.serve:
        uvicorn.run(create_app(args.mode, args.workers), host="127.0.0.1", port=8915,
                    log_level="warning")
    else:
        asyncio.run(benchmark(args.rounds))
