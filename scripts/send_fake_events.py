import argparse
import asyncio
import os
import random
import time
import uuid
from typing import Any, Dict, List
import httpx

EVENT_TYPES = [
    "order.created",
    "user.signup",
    "payment.succeeded",
    "inventory.updated",
    "account.verified",
]


def generate_payload(event_type: str) -> Dict[str, Any]:
    if event_type == "order.created":
        return {
            "order_id": f"ord_{random.randint(10000, 99999)}",
            "amount": round(random.uniform(10.0, 500.0), 2),
            "currency": "USD",
            "customer_id": f"cust_{random.randint(100, 999)}",
            "items_count": random.randint(1, 5),
        }
    elif event_type == "user.signup":
        return {
            "user_id": f"usr_{random.randint(1000, 9999)}",
            "email": f"user{random.randint(1000, 9999)}@example.com",
            "plan": random.choice(["free", "pro", "enterprise"]),
        }
    elif event_type == "payment.succeeded":
        return {
            "charge_id": f"ch_{uuid.uuid4().hex[:12]}",
            "amount": round(random.uniform(20.0, 1000.0), 2),
            "payment_method": random.choice(["card", "bank_transfer", "apple_pay"]),
        }
    elif event_type == "inventory.updated":
        return {
            "sku": f"SKU-{random.randint(100, 999)}",
            "stock_change": random.randint(-10, 50),
            "warehouse": random.choice(["US-EAST", "EU-CENTRAL", "AP-SOUTH"]),
        }
    elif event_type == "error.simulate":
        return {
            "simulate_failure": True,
            "reason": "Test fault tolerance and DLQ",
            "timestamp": time.time(),
        }
    return {"message": "generic event"}


async def send_single_event(
    client: httpx.AsyncClient,
    url: str,
    api_key: str,
    delivery_id: str,
    event_type: str,
    data: Dict[str, Any],
    stats: Dict[str, Any]
) -> None:
    payload = {
        "delivery_id": delivery_id,
        "source": "fake-generator",
        "event_type": event_type,
        "data": data,
    }
    t0 = time.perf_counter()
    headers = {"X-API-Key": api_key}
    try:
        resp = await client.post(url, json=payload, headers=headers, timeout=10.0)
        elapsed = time.perf_counter() - t0
        stats["latencies"].append(elapsed)

        if resp.status_code == 202:
            stats["accepted"] += 1
        elif resp.status_code == 200 and resp.json().get("status") == "duplicate":
            stats["duplicate"] += 1
        else:
            stats["other"] += 1
    except Exception as exc:
        stats["failed"] += 1


async def run_load_generator(
    url: str,
    api_key: str,
    total_events: int,
    concurrency: int,
    error_rate: float,
    duplicate_rate: float
) -> None:
    print(f"\n==========================================")
    print(f"  Fake Webhook Event Generator")
    print(f"  Target URL: {url}")
    print(f"  Total Events: {total_events}")
    print(f"  Concurrency: {concurrency}")
    print(f"  Error Simulation Rate: {int(error_rate * 100)}%")
    print(f"  Duplicate Rate: {int(duplicate_rate * 100)}%")
    print(f"==========================================\n")

    stats = {
        "accepted": 0,
        "duplicate": 0,
        "failed": 0,
        "other": 0,
        "latencies": []
    }

    # Track already-sent delivery_ids so duplicates genuinely repeat an existing ID
    sent_deliveries: List[str] = []

    semaphore = asyncio.Semaphore(concurrency)

    async def worker(delivery_id: str, event_type: str, data: Dict[str, Any], client: httpx.AsyncClient):
        async with semaphore:
            await send_single_event(client, url, api_key, delivery_id, event_type, data, stats)

    tasks = []
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(limits=limits) as client:
        start_time = time.perf_counter()

        for i in range(total_events):
            # Check if this should be a duplicate
            if sent_deliveries and random.random() < duplicate_rate:
                deliv_id = random.choice(sent_deliveries)
            else:
                deliv_id = str(uuid.uuid4())
                sent_deliveries.append(deliv_id)

            # Check if this should be an error simulation event
            if random.random() < error_rate:
                ev_type = "error.simulate"
            else:
                ev_type = random.choice(EVENT_TYPES)

            data = generate_payload(ev_type)
            tasks.append(worker(deliv_id, ev_type, data, client))

        await asyncio.gather(*tasks)
        total_time = time.perf_counter() - start_time

    latencies = stats["latencies"]
    avg_latency = (sum(latencies) / len(latencies) * 1000) if latencies else 0.0
    latencies.sort()
    p50_latency = (latencies[int(len(latencies) * 0.50)] * 1000) if latencies else 0.0
    p95_latency = (latencies[int(len(latencies) * 0.95)] * 1000) if latencies else 0.0
    p99_latency = (latencies[int(len(latencies) * 0.99)] * 1000) if latencies else 0.0
    throughput = total_events / total_time if total_time > 0 else 0

    print("\n--- Benchmark / Ingestion Results ---")
    print(f"Total Events Sent   : {total_events}")
    print(f"Accepted (202)      : {stats['accepted']}")
    print(f"Duplicates (200)    : {stats['duplicate']}")
    print(f"Network Failures    : {stats['failed']}")
    print(f"Other Statuses      : {stats['other']}")
    print(f"Total Time Taken    : {total_time:.2f} seconds")
    print(f"Throughput          : {throughput:.1f} events/sec")
    print(f"Avg Latency         : {avg_latency:.2f} ms")
    print(f"p50 Latency         : {p50_latency:.2f} ms")
    print(f"p95 Latency         : {p95_latency:.2f} ms")
    print(f"p99 Latency         : {p99_latency:.2f} ms")
    print("-------------------------------------\n")


def main():
    parser = argparse.ArgumentParser(description="Send synthetic events to the ingestion service.")
    parser.add_argument("--url", default="http://localhost:8000/api/v1/events", help="Ingestion endpoint URL")
    parser.add_argument("--api-key", default=os.environ.get("INGEST_API_KEY", ""), help="Ingestion API key")
    parser.add_argument("--count", type=int, default=100, help="Number of events to generate")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent HTTP requests")
    parser.add_argument("--error-rate", type=float, default=0.10, help="Fraction of events simulating errors (0.0 - 1.0)")
    parser.add_argument("--dup-rate", type=float, default=0.15, help="Fraction of events simulating duplicate delivery IDs (0.0 - 1.0)")

    args = parser.parse_args()
    asyncio.run(
        run_load_generator(
            url=args.url,
            api_key=args.api_key,
            total_events=args.count,
            concurrency=args.concurrency,
            error_rate=args.error_rate,
            duplicate_rate=args.dup_rate,
        )
    )


if __name__ == "__main__":
    main()

