"""Synthetic producer for Brazos Sensors (fictional).

Emits machine telemetry and work-order CDC events with deliberate defects:
- duplicates   : ~2% of telemetry re-sent with the same event_id
- late events  : ~3% of telemetry delivered with an event time 5-30 minutes old
- out of order : CDC events for a work order are sometimes delivered newest-first
- malformed    : ~0.5% broken JSON, missing ids, impossible temperatures, bad ops

Usage:
    python scripts/generate_events.py --events 50000 --bootstrap localhost:19092
    python scripts/generate_events.py --events 2000 --out-dir data/sample   # no Kafka
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from collections.abc import Iterable, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

Record = tuple[str, str, str]  # (topic, key, value)

MACHINES = [f"NW-PRESS-{i:03d}" for i in range(1, 41)]
STATES = ["OPEN", "IN_PROGRESS", "CLOSED"]
PRIORITIES = ["LOW", "MEDIUM", "HIGH"]
START = datetime(2026, 3, 2, 6, 0, tzinfo=timezone.utc)


def iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def telemetry_reading(rng: random.Random, ts: datetime) -> dict:
    """One plausible reading; hot machines occasionally report WARN/ERROR."""
    machine = rng.choice(MACHINES)
    temp = round(rng.gauss(70, 9), 2)
    status = "ERROR" if temp > 95 else "WARN" if temp > 85 else "OK"
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "machine_id": machine,
        "event_ts": iso(ts),
        "temperature_c": temp,
        "vibration_mm_s": round(abs(rng.gauss(2.5, 1.2)), 3),
        "status": status,
    }


def telemetry_events(rng: random.Random, n: int) -> Iterator[Record]:
    """Telemetry stream with duplicates and late arrivals mixed in."""
    for i in range(n):
        ts = START + timedelta(seconds=i * 2)
        if rng.random() < 0.03:
            ts -= timedelta(minutes=rng.randint(5, 30))
        reading = telemetry_reading(rng, ts)
        yield ("telemetry", reading["machine_id"], json.dumps(reading))
        if rng.random() < 0.02:
            yield ("telemetry", reading["machine_id"], json.dumps(reading))


def work_order_history(rng: random.Random, wo_id: str, opened: datetime) -> list[dict]:
    """create -> 0..2 updates -> maybe close -> maybe delete, with increasing op_ts."""
    base = {
        "work_order_id": wo_id,
        "machine_id": rng.choice(MACHINES),
        "priority": rng.choice(PRIORITIES),
        "opened_at": iso(opened),
    }
    events = [{**base, "op": "c", "op_ts": iso(opened), "state": "OPEN"}]
    ts = opened
    for state in STATES[1 : rng.randint(1, 3)]:
        ts += timedelta(minutes=rng.randint(5, 240))
        events.append({**base, "op": "u", "op_ts": iso(ts), "state": state})
    if rng.random() < 0.08:
        ts += timedelta(minutes=rng.randint(1, 60))
        events.append({**base, "op": "d", "op_ts": iso(ts), "state": None})
    return events


def workorder_events(rng: random.Random, n_orders: int) -> Iterator[Record]:
    """CDC events; ~10% of histories are delivered out of order, ~3% replay an event."""
    for i in range(n_orders):
        wo_id = f"WO-{100000 + i}"
        history = work_order_history(rng, wo_id, START + timedelta(minutes=rng.randint(0, 2880)))
        if rng.random() < 0.10:
            history.reverse()
        if rng.random() < 0.03:
            history.append(rng.choice(history))
        for event in history:
            yield ("workorder_cdc", wo_id, json.dumps(event))


def malformed_events(rng: random.Random, n: int) -> Iterator[Record]:
    """Records that validation must quarantine, never drop."""
    for _ in range(n):
        kind = rng.randrange(5)
        if kind == 0:
            yield ("telemetry", "bad", '{"event_id": "x", "temperature_c": ')
        elif kind == 1:
            reading = telemetry_reading(rng, START)
            reading["event_id"] = None
            yield ("telemetry", "bad", json.dumps(reading))
        elif kind == 2:
            reading = telemetry_reading(rng, START)
            reading["temperature_c"] = 999.0
            yield ("telemetry", "bad", json.dumps(reading))
        elif kind == 3:
            event = {"op": "x", "op_ts": iso(START), "work_order_id": "WO-1", "state": "OPEN"}
            yield ("workorder_cdc", "bad", json.dumps(event))
        else:
            event = {"op": "u", "op_ts": iso(START), "work_order_id": "WO-2", "state": "LOST"}
            yield ("workorder_cdc", "bad", json.dumps(event))


def generate(total: int, seed: int = 42) -> list[Record]:
    """Roughly ``total`` records: ~85% telemetry, ~14.5% CDC, ~0.5% malformed, shuffled."""
    rng = random.Random(seed)
    records = list(telemetry_events(rng, int(total * 0.85)))
    records += list(workorder_events(rng, max(1, int(total * 0.145 / 2.6))))
    records += list(malformed_events(rng, max(1, int(total * 0.005))))
    rng.shuffle(records)
    return records


def write_jsonl(records: Iterable[Record], out_dir: Path) -> None:
    """Offline mode: one JSONL file per topic, each line is the raw message value."""
    out_dir.mkdir(parents=True, exist_ok=True)
    handles = {}
    try:
        for topic, _key, value in records:
            if topic not in handles:
                handles[topic] = (out_dir / f"{topic}.jsonl").open("w", encoding="utf-8")
            handles[topic].write(value + "\n")
    finally:
        for handle in handles.values():
            handle.close()


def produce(records: Iterable[Record], bootstrap: str) -> int:
    """Send to Kafka/Redpanda keyed by machine or work order (keeps per-key ordering)."""
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=bootstrap, linger_ms=20, acks="all")
    sent = 0
    for topic, key, value in records:
        producer.send(topic, key=key.encode(), value=value.encode())
        sent += 1
    producer.flush()
    producer.close()
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--events", type=int, default=50_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--out-dir", type=Path, default=None, help="write JSONL instead")
    args = parser.parse_args()

    records = generate(args.events, args.seed)
    if args.out_dir:
        write_jsonl(records, args.out_dir)
        print(f"wrote {len(records)} records to {args.out_dir}")
    else:
        bootstrap = args.bootstrap or "localhost:19092"
        print(f"sent {produce(records, bootstrap)} records to {bootstrap}")


if __name__ == "__main__":
    main()
