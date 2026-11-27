"""Deterministic replay clock with duplicate, late, poison and schema-version events."""
import argparse
import heapq
import itertools
import json
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, Optional


@dataclass
class Source:
    name: str
    topic: str
    reader: Callable
    source_system: str
    kind: str = "json"
    late_p: float = 0.01
    late_minutes: tuple = (5, 45)
    dup_p: float = 0.003
    dup_seconds: tuple = (1, 300)
    poison_p: float = 0.0004
    version: Optional[Callable] = None
    amount_field: Optional[str] = None
    ts_field: Optional[str] = None
    key_fields: tuple = ()
    sorted_input: bool = True


@dataclass
class Context:
    data: str
    d_from: date
    d_to: date
    rnd: random.Random = field(default_factory=random.Random)

    def days(self):
        current = self.d_from
        while current <= self.d_to:
            yield current
            current += timedelta(days=1)

    def in_range(self, value):
        return self.d_from <= value.date() <= self.d_to


def parse_ts(value):
    value = str(value).replace("T", " ")
    return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")


class DryRunSink:
    def __init__(self):
        os.makedirs("_dryrun", exist_ok=True)
        self.files = {}

    def send(self, topic, key, value, headers):
        handle = self.files.get(topic)
        if handle is None:
            handle = self.files[topic] = open(
                f"_dryrun/{topic}.jsonl", "w", encoding="utf-8"
            )
        handle.write(json.dumps({
            "key": key,
            "headers": {name: value.decode() for name, value in headers},
            "value": value.decode(errors="replace"),
        }) + "\n")

    def poll(self):
        return None

    def close(self):
        for handle in self.files.values():
            handle.close()
        return 0


class KafkaSink:
    def __init__(self, bootstrap, client_id):
        from confluent_kafka import Producer
        self.errors = 0
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": client_id,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "batch.size": 131072,
            "compression.type": "lz4",
        })

    def _callback(self, error, message):
        if error is not None:
            self.errors += 1
            if self.errors <= 5:
                print(f"delivery failed: {error}", file=sys.stderr)

    def send(self, topic, key, value, headers):
        while True:
            try:
                self.producer.produce(
                    topic,
                    key=key.encode() if key else None,
                    value=value,
                    headers=headers,
                    on_delivery=self._callback,
                )
                break
            except BufferError:
                self.producer.poll(0.5)
        self.producer.poll(0)

    def poll(self):
        self.producer.poll(0)

    def close(self):
        remaining = self.producer.flush(30)
        return self.errors + remaining


def main(spec):
    sources_by_name = {source.name: source for source in spec.SOURCES}
    parser = argparse.ArgumentParser(
        description=spec.DOC, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--data", default=os.environ.get("RAW_DATA", "data/raw"))
    parser.add_argument("--from", dest="date_from", default=spec.DEFAULT_FROM)
    parser.add_argument("--to", dest="date_to", default=spec.DEFAULT_TO)
    parser.add_argument("--sources", default=",".join(sources_by_name))
    parser.add_argument(
        "--speed", type=float, default=1440,
        help="simulated seconds per real second (1440 = one simulated day per minute; 0 = instant)",
    )
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get(
            "KAFKA_BOOTSTRAP",
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        ),
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--no-chaos", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    selected_names = [name.strip() for name in args.sources.split(",") if name.strip()]
    for name in selected_names:
        if name not in sources_by_name:
            parser.error(f"Unknown source {name!r}; choose from {', '.join(sources_by_name)}")
    if args.speed < 0:
        parser.error("--speed must be zero or positive")
    sources = [sources_by_name[name] for name in selected_names]
    context = Context(
        args.data,
        date.fromisoformat(args.date_from),
        date.fromisoformat(args.date_to),
        random.Random(args.seed),
    )
    if not os.path.isdir(args.data):
        parser.error(f"Dataset folder {args.data!r} not found; copy the dataset's data/raw folder here")

    rng = random.Random(args.seed)
    chaos = not args.no_chaos
    sink = DryRunSink() if args.dry_run else KafkaSink(args.bootstrap, f"{spec.PROJECT}-replay")

    def tagged(source):
        rows = source.reader(context)
        if not source.sorted_input:
            rows = sorted(rows, key=lambda row: row[0])
        for row in rows:
            event_time, key, value = row[:3]
            extra = row[3] if len(row) > 3 else None
            yield event_time, source.name, key, value, extra

    def poison(source, value):
        if source.kind == "text":
            if rng.choice(["truncated", "garbage"]) == "truncated":
                return value[: rng.randint(5, max(6, len(value) // 2))].encode()
            return f"\x00\x00ERR|{source.name}|CONNECTION_RESET".encode()
        kinds = ["truncated", "not_json"]
        if source.amount_field:
            kinds.append("bad_amount")
        if source.ts_field:
            kinds.append("bad_ts")
        if source.key_fields:
            kinds.append("missing_key")
        kind = rng.choice(kinds)
        row = dict(value)
        if kind == "truncated":
            serialized = json.dumps(row)
            return serialized[: rng.randint(10, max(11, len(serialized) - 5))].encode()
        if kind == "not_json":
            return f"ERR|{source.name}|{rng.randint(1, 10**9)}|RETRY_LATER".encode()
        if kind == "bad_amount":
            try:
                row[source.amount_field] = f"{float(row[source.amount_field]):,.2f}"
            except (TypeError, ValueError, KeyError):
                row[source.amount_field] = "NaN"
        elif kind == "bad_ts" and row.get(source.ts_field):
            row[source.ts_field] = str(row[source.ts_field]).replace("-", "/", 2)
        elif kind == "missing_key":
            for field_name in source.key_fields:
                row.pop(field_name, None)
        return json.dumps(row).encode()

    stats = {
        source.name: {"sent": 0, "duplicates": 0, "late": 0, "poison": 0, "v2": 0}
        for source in sources
    }
    by_name = {source.name: source for source in sources}

    def emit(item, simulated_now):
        event_time, name, key, value, extra, flag = item
        source = by_name[name]
        version = "1"
        if source.version:
            value, version = source.version(event_time, value)
        if flag == "poison":
            body = poison(source, value)
        elif source.kind == "text":
            body = value.encode()
        else:
            body = json.dumps(value).encode()
        headers = [
            ("source_system", source.source_system.encode()),
            ("schema_version", version.encode()),
            ("sent_at", simulated_now.strftime("%Y-%m-%dT%H:%M:%S").encode()),
        ]
        for header_name, header_value in (extra or {}).items():
            headers.append((header_name, str(header_value).encode()))
        sink.send(source.topic, key, body, headers)
        stats[name]["sent"] += 1
        stats[name]["v2"] += version == "2"

    sim_start = datetime.combine(context.d_from, datetime.min.time())
    wall_start = time.monotonic()
    last_report = [time.monotonic()]

    def wait_until(simulated_time):
        if args.speed <= 0:
            return
        target = wall_start + (simulated_time - sim_start).total_seconds() / args.speed
        delay = target - time.monotonic()
        while delay > 0:
            time.sleep(min(delay, 0.5))
            sink.poll()
            delay = target - time.monotonic()

    def report(simulated_now, force=False):
        if force or time.monotonic() - last_report[0] >= 10:
            last_report[0] = time.monotonic()
            summary = ", ".join(f"{name}={value['sent']}" for name, value in stats.items())
            print(f"[sim {simulated_now:%Y-%m-%d %H:%M}] sent: {summary}", flush=True)

    print(
        f"Replaying {', '.join(selected_names)} from {context.d_from} to {context.d_to} "
        f"at speed {args.speed:g} -> {'_dryrun/' if args.dry_run else args.bootstrap} "
        f"(chaos {'on' if chaos else 'off'})",
        flush=True,
    )
    stream = heapq.merge(*(tagged(source) for source in sources), key=lambda item: item[0])
    delayed = []
    sequence = itertools.count()
    simulated_now = sim_start
    try:
        for event_time, name, key, value, extra in stream:
            while delayed and delayed[0][0] <= event_time:
                release_time, _, item = heapq.heappop(delayed)
                wait_until(release_time)
                emit(item, release_time)
            wait_until(event_time)
            simulated_now = event_time
            source = by_name[name]
            flag = None
            if chaos:
                if rng.random() < source.poison_p:
                    flag = "poison"
                    stats[name]["poison"] += 1
                    delay = rng.randint(10, 90)
                    heapq.heappush(
                        delayed,
                        (event_time + timedelta(minutes=delay), next(sequence),
                         (event_time, name, key, value, extra, None)),
                    )
                elif rng.random() < source.late_p:
                    delay = rng.randint(*source.late_minutes)
                    heapq.heappush(
                        delayed,
                        (event_time + timedelta(minutes=delay), next(sequence),
                         (event_time, name, key, value, extra, None)),
                    )
                    stats[name]["late"] += 1
                    continue
            emit((event_time, name, key, value, extra, flag), event_time)
            if chaos and flag is None and rng.random() < source.dup_p:
                delay = rng.randint(*source.dup_seconds)
                heapq.heappush(
                    delayed,
                    (event_time + timedelta(seconds=delay), next(sequence),
                     (event_time, name, key, value, extra, None)),
                )
                stats[name]["duplicates"] += 1
            report(simulated_now)
        while delayed:
            release_time, _, item = heapq.heappop(delayed)
            wait_until(release_time)
            emit(item, release_time)
    except KeyboardInterrupt:
        print("Stopping replay (Ctrl+C)")
    failures = sink.close()
    report(simulated_now, force=True)
    print("Replay summary:")
    for source in sources:
        values = stats[source.name]
        print(
            f"  {source.topic:<32} sent={values['sent']:>7} "
            f"duplicates={values['duplicates']:>4} late={values['late']:>4} "
            f"broken={values['poison']:>3} schema_v2={values['v2']}"
        )
    if failures:
        print(f"WARNING: {failures} Kafka messages were not delivered", file=sys.stderr)
        sys.exit(1)
