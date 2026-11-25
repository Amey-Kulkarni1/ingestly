#!/usr/bin/env python3
"""
Realistic Multi-Source Security Telemetry & Webhook Traffic Simulator

Reads real enterprise security logs from sample_logs/ (Cloudflare, CrowdStrike,
Cortex XDR, Netskope, SentinelOne, Wallarm, Mimecast, Huntress, Prisma Cloud)
and streams them to the ingestion service at realistic, randomized intervals
with variable bursts and natural duplicate rates.
"""

import argparse
import asyncio
import glob
import hashlib
import hmac
import json
import os
from pathlib import Path
import random
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple
import httpx

# Color formatting for terminal
class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"
    RED = "\033[31m"
    DIM = "\033[2m"

SOURCE_COLORS = {
    "cloudflare": Colors.YELLOW,
    "crowdstrike": Colors.RED,
    "cortexxdr": Colors.BLUE,
    "netskope": Colors.CYAN,
    "sentinelone": Colors.MAGENTA,
    "wallarm": Colors.GREEN,
    "mimecast": Colors.BLUE,
    "huntress": Colors.YELLOW,
    "prismacloud": Colors.CYAN,
    "generic": Colors.RESET,
}


def compute_hmac_sha256(secret: str, body: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


from datetime import datetime, timezone


def prepare_unique_event(source: str, raw_data: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
    """Mutate real log record into an infinitely unique event with current timestamps."""
    data = dict(raw_data)  # shallow copy to avoid mutating cache
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    now_epoch_ms = int(now.timestamp() * 1000)
    now_epoch_s = int(now.timestamp())
    uid_suffix = uuid.uuid4().hex[:10]

    # Update common timestamp fields across vendors to keep time-series graphs fresh
    for k in ("datetime", "timestamp", "created_at", "updated_at", "crawled_timestamp", "created_timestamp", "context_timestamp", "first_time", "last_time"):
        if k in data:
            if isinstance(data[k], (int, float)):
                data[k] = now_epoch_ms if data[k] > 1e11 else now_epoch_s
            else:
                data[k] = now_iso

    ev_type = f"{source}.event"

    if source == "cloudflare":
        lt = data.get("logtype") or data.get("securityAction") or data.get("securitySource") or "waf"
        ev_type = f"cloudflare.{lt}"
        deliv_id = f"cf_{now_epoch_s}_{uid_suffix}"

    elif source == "crowdstrike":
        name = data.get("name") or data.get("display_name") or data.get("category") or data.get("type") or "detection"
        ev_type = f"crowdstrike.{str(name).replace(' ', '_').lower()}"
        deliv_id = f"cs_{uid_suffix}"

    elif source == "cortexxdr":
        inc_name = data.get("incident_name") or data.get("status") or "incident"
        clean_name = str(inc_name).split()[0].strip("'").lower()
        ev_type = f"cortexxdr.{clean_name}"
        deliv_id = f"xdr_{uid_suffix}"

    elif source == "netskope":
        cat = data.get("_category_name") or data.get("app") or "web_traffic"
        ev_type = f"netskope.{str(cat).replace(' ', '_').lower()}"
        deliv_id = f"nsk_{uid_suffix}"
        data["_correlation_id"] = str(uuid.uuid4())

    elif source == "sentinelone":
        act = data.get("activityType") or data.get("threatId") or "threat_alert"
        ev_type = f"sentinelone.activity_{act}"
        deliv_id = f"s1_{uid_suffix}"
        data["activityUuid"] = str(uuid.uuid4())

    elif source == "wallarm":
        atk = data.get("type") or data.get("attack_grouping") or "waf_attack"
        ev_type = f"wallarm.{atk}"
        deliv_id = f"wal_{uid_suffix}"

    elif source == "mimecast":
        sub = data.get("eventType") or data.get("rejectionType") or data.get("action") or "mail"
        ev_type = f"mimecast.{str(sub).replace(' ', '_').lower()}"
        deliv_id = f"mim_{uid_suffix}"

    elif source == "huntress":
        sev = data.get("severity") or data.get("platform") or "ransomware_alert"
        ev_type = f"huntress.{sev}"
        deliv_id = f"hnt_{uid_suffix}"

    elif source == "prismacloud":
        act = data.get("action") or "policy_audit"
        clean_act = str(act).split("@")[0].strip("'")[:20]
        ev_type = f"prismacloud.{clean_act.lower()}"
        deliv_id = f"prisma_{now_epoch_ms}_{uid_suffix}"

    else:
        deliv_id = f"evt_{uid_suffix}"

    return ev_type, deliv_id, data


class LogPool:
    """Loads and samples real log files into memory pools."""

    def __init__(self, log_dir: str, max_samples_per_file: int = 500):
        # Resolve log directory relative to current directory, repository root, or script dir
        target_path = Path(log_dir)
        if not target_path.is_absolute() and not target_path.exists():
            repo_root = Path(__file__).resolve().parent.parent / log_dir
            if repo_root.exists():
                target_path = repo_root
            else:
                script_rel = Path(__file__).resolve().parent / log_dir
                if script_rel.exists():
                    target_path = script_rel

        self.log_dir = str(target_path)
        self.max_samples = max_samples_per_file
        self.pools: Dict[str, List[Dict[str, Any]]] = {}
        self.load_all()

    def identify_source(self, filename: str) -> str:
        base = os.path.basename(filename).lower()
        if "cloudflare" in base:
            return "cloudflare"
        elif "crowdstrike" in base:
            return "crowdstrike"
        elif "cortex" in base:
            return "cortexxdr"
        elif "netskope" in base:
            return "netskope"
        elif "sentinel" in base:
            return "sentinelone"
        elif "wallarm" in base:
            return "wallarm"
        elif "mimecast" in base:
            return "mimecast"
        elif "huntress" in base:
            return "huntress"
        elif "prisma" in base:
            return "prismacloud"
        return "generic"

    def load_all(self):
        files = glob.glob(os.path.join(self.log_dir, "*"))
        print(f"{Colors.DIM}Discovering log files in '{self.log_dir}'...{Colors.RESET}")

        decoder = json.JSONDecoder()

        for fpath in files:
            if not os.path.isfile(fpath) or os.path.getsize(fpath) == 0:
                continue

            source = self.identify_source(fpath)
            items = []

            try:
                with open(fpath, "r", encoding="utf-8", errors="ignore") as fp:
                    # First try standard line-by-line JSON
                    for i, line in enumerate(fp):
                        if i >= self.max_samples * 2 or len(items) >= self.max_samples:
                            break
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                            if isinstance(obj, dict):
                                items.append(obj)
                        except Exception:
                            pass

                # If line-by-line gave few items, parse raw JSON blocks (like Mimecast / Huntress)
                if len(items) < 10:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as fp:
                        content = fp.read(1024 * 1024)  # Read up to 1MB
                        idx = 0
                        while idx < len(content) and len(items) < self.max_samples:
                            content = content[idx:].lstrip()
                            if not content:
                                break
                            try:
                                obj, end_idx = decoder.raw_decode(content)
                                if isinstance(obj, dict):
                                    items.append(obj)
                                idx = end_idx
                            except Exception:
                                break

            except Exception as err:
                print(f"{Colors.RED}Failed reading {fpath}: {err}{Colors.RESET}")

            if items:
                self.pools[source] = items
                color = SOURCE_COLORS.get(source, Colors.RESET)
                print(f"  Loaded {color}{len(items):>4} records{Colors.RESET} from {Colors.BOLD}{os.path.basename(fpath)}{Colors.RESET} [{source}]")

        total = sum(len(v) for v in self.pools.values())
        print(f"{Colors.GREEN}{Colors.BOLD}Ready: {total} genuine telemetry payloads cached across {len(self.pools)} sources.{Colors.RESET}\n")

    def sample(self, source_filter: Optional[List[str]] = None) -> Tuple[str, Dict[str, Any]]:
        available = [s for s in self.pools if not source_filter or s in source_filter]
        if not available:
            available = list(self.pools.keys())
        if not available:
            raise RuntimeError(
                f"No log samples found in '{self.log_dir}'. Ensure the directory exists and contains valid JSON logs."
            )
        source = random.choice(available)
        payload = random.choice(self.pools[source])
        return source, payload


async def send_event(
    client: httpx.AsyncClient,
    url: str,
    delivery_id: str,
    source: str,
    event_type: str,
    data: Dict[str, Any],
    secret: Optional[str],
    stats: Dict[str, Any],
    is_duplicate: bool = False
) -> None:
    correlation_id = f"sim-{source[:4]}-{uuid.uuid4().hex[:8]}"

    envelope = {
        "delivery_id": delivery_id,
        "source": source,
        "event_type": event_type,
        "data": data,
    }
    raw_body = json.dumps(envelope).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "X-Correlation-ID": correlation_id,
        "X-API-Key": "dev-ingest-key",
    }
    if secret:
        headers["X-Hub-Signature-256"] = compute_hmac_sha256(secret, raw_body)

    t0 = time.perf_counter()
    status_str = "ERR"
    code = 0
    try:
        resp = await client.post(url, content=raw_body, headers=headers, timeout=5.0)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        code = resp.status_code

        if code == 202:
            stats["accepted"] += 1
            status_str = f"{Colors.GREEN}202 Accepted{Colors.RESET}"
        elif code == 200 and resp.json().get("status") == "duplicate":
            stats["duplicate"] += 1
            status_str = f"{Colors.YELLOW}200 Duplicate{Colors.RESET}"
        else:
            stats["other"] += 1
            status_str = f"{Colors.RED}{code} Other{Colors.RESET}"

        stats["latencies"].append(elapsed_ms)

    except Exception as exc:
        stats["failed"] += 1
        elapsed_ms = (time.perf_counter() - t0) * 1000
        status_str = f"{Colors.RED}ERR: {exc}{Colors.RESET}"

    color = SOURCE_COLORS.get(source, Colors.RESET)
    dup_tag = f" {Colors.YELLOW}[RETRY-DUP]{Colors.RESET}" if is_duplicate else ""
    ts = time.strftime("%H:%M:%S")

    print(f" {Colors.DIM}{ts}{Colors.RESET} | {color}{source:<13}{Colors.RESET} | {event_type:<32} | {delivery_id[:18]:<18} | {status_str:<21} | {elapsed_ms:>6.1f}ms{dup_tag}")


async def run_traffic_simulator(
    url: str,
    pool: LogPool,
    profile: str,
    sources: Optional[List[str]],
    count: Optional[int],
    dup_rate: float,
    secret: Optional[str]
):
    print("=" * 85)
    print(f" {Colors.BOLD}Enterprise Webhook & Log Stream Simulator{Colors.RESET}")
    print(f" Target: {Colors.CYAN}{url}{Colors.RESET}")
    print(f" Profile: {Colors.BOLD}{profile.upper()}{Colors.RESET} | Duplicate Rate: {int(dup_rate * 100)}% | HMAC Auth: {'Enabled' if secret else 'Disabled'}")
    print("=" * 85)
    print(f" {'TIME':<8} | {'SOURCE':<13} | {'EVENT TYPE':<32} | {'DELIVERY ID':<18} | {'STATUS':<12} | {'LATENCY'}")
    print("-" * 85)

    stats: Dict[str, Any] = {
        "accepted": 0,
        "duplicate": 0,
        "failed": 0,
        "other": 0,
        "latencies": [],
    }

    recent_deliveries: List[Tuple[str, str, str, Dict[str, Any]]] = []
    sent_total = 0

    limits = httpx.Limits(max_connections=50, max_keepalive_connections=25)
    async with httpx.AsyncClient(limits=limits) as client:
        start_time = time.perf_counter()

        while True:
            if count is not None and sent_total >= count:
                break

            # Profile cadence definition
            if profile == "human":
                # Natural human/server burstiness: mix of trickle and sudden spikes
                burst_roll = random.random()
                if burst_roll < 0.10:
                    batch_size = random.randint(8, 20)      # Sudden alert storm
                    sleep_time = random.uniform(0.1, 0.4)
                elif burst_roll < 0.35:
                    batch_size = random.randint(3, 7)       # Moderate activity
                    sleep_time = random.uniform(0.4, 0.9)
                else:
                    batch_size = random.randint(1, 2)       # Quiet baseline trickle
                    sleep_time = random.uniform(0.5, 1.8)

            elif profile == "constant":
                batch_size = 2
                sleep_time = 0.2

            elif profile == "storm":
                batch_size = random.randint(15, 30)
                sleep_time = random.uniform(0.05, 0.15)
            else:
                batch_size = 1
                sleep_time = 0.5

            if count is not None and (sent_total + batch_size) > count:
                batch_size = count - sent_total

            tasks = []
            for _ in range(batch_size):
                # Check for realistic duplicate delivery
                if recent_deliveries and random.random() < dup_rate:
                    old_deliv_id, old_src, old_ev_type, old_data = random.choice(recent_deliveries)
                    tasks.append(
                        send_event(client, url, old_deliv_id, old_src, old_ev_type, old_data, secret, stats, is_duplicate=True)
                    )
                else:
                    src, raw_log = pool.sample(sources)
                    ev_type, deliv_id, mutated_log = prepare_unique_event(src, raw_log)
                    recent_deliveries.append((deliv_id, src, ev_type, mutated_log))
                    if len(recent_deliveries) > 100:
                        recent_deliveries.pop(0)

                    tasks.append(
                        send_event(client, url, deliv_id, src, ev_type, mutated_log, secret, stats, is_duplicate=False)
                    )

            await asyncio.gather(*tasks)
            sent_total += batch_size

            try:
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                break

    elapsed = time.perf_counter() - start_time
    latencies = stats["latencies"]
    avg_lat = (sum(latencies) / len(latencies)) if latencies else 0.0
    latencies.sort()
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0.0

    print("-" * 85)
    print(f"\n{Colors.BOLD}Simulation Run Summary:{Colors.RESET}")
    print(f" Total Events Streamed : {Colors.BOLD}{sent_total}{Colors.RESET}")
    print(f" Accepted (202)        : {Colors.GREEN}{stats['accepted']}{Colors.RESET}")
    print(f" Duplicates Caught (200): {Colors.YELLOW}{stats['duplicate']}{Colors.RESET}")
    print(f" Failures / Errors     : {Colors.RED}{stats['failed'] + stats['other']}{Colors.RESET}")
    print(f" Total Duration        : {elapsed:.2f}s ({sent_total / elapsed:.1f} events/sec)")
    print(f" Latency               : Avg {avg_lat:.1f}ms | p95 {p95:.1f}ms\n")


def main():
    parser = argparse.ArgumentParser(description="Realistic Enterprise Security Log & Webhook Simulator")
    parser.add_argument("--url", default="http://localhost:8000/api/v1/events", help="Ingestion endpoint URL")
    parser.add_argument("--logs-dir", default="sample_logs", help="Directory containing raw log files")
    parser.add_argument("--profile", choices=["human", "constant", "storm"], default="human", help="Traffic behavior profile")
    parser.add_argument("--sources", help="Comma-separated sources to stream (e.g. cloudflare,crowdstrike)")
    parser.add_argument("--count", type=int, default=None, help="Total events to send (omit for continuous stream until Ctrl+C)")
    parser.add_argument("--dup-rate", type=float, default=0.05, help="Fraction of duplicate delivery IDs to simulate (default: 0.05)")
    parser.add_argument("--secret", default="dev-webhook-secret", help="HMAC secret for signature generation (empty to disable)")
    parser.add_argument("--no-sign", action="store_true", help="Disable HMAC header signing")

    args = parser.parse_args()

    sources_list = [s.strip().lower() for s in args.sources.split(",")] if args.sources else None
    secret = None if args.no_sign else args.secret

    pool = LogPool(log_dir=args.logs_dir)

    try:
        asyncio.run(
            run_traffic_simulator(
                url=args.url,
                pool=pool,
                profile=args.profile,
                sources=sources_list,
                count=args.count,
                dup_rate=args.dup_rate,
                secret=secret
            )
        )
    except KeyboardInterrupt:
        print(f"\n{Colors.DIM}Stream stopped by user.{Colors.RESET}")


if __name__ == "__main__":
    main()

