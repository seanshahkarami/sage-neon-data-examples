#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "confluent-kafka>=2.3",
#     "fastavro>=1.9",
#     "pyarrow>=15",
# ]
# ///
"""Dump Avro messages from a Kafka topic (last N per partition, since a time, or all) to Parquet.

Reads the raw message bytes (no deserializer), strips the Confluent 5-byte
header, decodes each record with fastavro using the writer schema from the
registry, and writes Parquet with pyarrow. The Arrow schema is built from the
Avro schema, so ["null", T] unions become plain nullable T columns.

    uv run kafka_to_parquet.py reading.sensor.cmp22 -n 10 -o cmp22.parquet
    uv run kafka_to_parquet.py reading.sensor.cmp22 -s 3600 -o cmp22.parquet  # last hour
    uv run kafka_to_parquet.py reading.sensor.cmp22 -s 2h -o cmp22.parquet
    uv run kafka_to_parquet.py reading.sensor.cmp22 --all -o cmp22.parquet
    duckdb -c "select * from 'cmp22.parquet'"

Records are written one row group at a time, so --all works on topics larger than memory.

Or make it executable (chmod +x) and run ./kafka_to_parquet.py directly.
"""
import argparse
import io
import json
import os
import struct
import sys
import time
from dataclasses import dataclass, field

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
import urllib.request

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition


# --- Kafka: read raw bytes (like kcat without -s) ----------------------------

def read_raw(broker: str, topic: str, last_n: int | None, since_ms: int | None = None):
    """Yield raw value bytes, then stop once every partition is caught up.

    since_ms reads each partition from its first message at or after that
    time (Unix ms). Otherwise last_n=None reads each partition from its
    earliest retained offset, and an int reads only the last N messages.
    """
    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": "kafka-to-parquet",  # required, unused: we assign manually
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    try:
        md = consumer.list_topics(topic, timeout=10).topics[topic]
        if md.error:
            raise KafkaException(md.error)
        assignments, expected = [], 0
        for p in md.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if since_ms is not None:
                # Offset of the first message at or after since_ms; -1 if none.
                found = consumer.offsets_for_times([TopicPartition(topic, p, since_ms)], timeout=10)[0]
                start = high if found.offset < 0 else max(low, found.offset)
            elif last_n is None:
                start = low
            else:
                start = max(low, high - last_n)
            if high > start:
                assignments.append(TopicPartition(topic, p, start))
                expected += high - start
        print(f"reading up to {expected:,} messages from {len(assignments)} partition(s)",
              file=sys.stderr)
        consumer.assign(assignments)
        remaining = {tp.partition for tp in assignments}
        while remaining:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    remaining.discard(msg.partition())
                    continue
                raise KafkaException(msg.error())
            if msg.value() is not None:  # skip tombstones
                yield msg.value()
    finally:
        consumer.close()


# --- Avro: strip the Confluent header and decode -----------------------------

def registry_fetcher(base_url: str):
    """Return a function that fetches a schema string by ID from the registry REST API."""
    def fetch(schema_id: int) -> str:
        with urllib.request.urlopen(f"{base_url.rstrip('/')}/schemas/ids/{schema_id}", timeout=10) as r:
            body = json.load(r)
        if body.get("references"):
            raise NotImplementedError(f"schema {schema_id} uses schema references")
        return body["schema"]
    return fetch


def split_header(raw: bytes) -> tuple[int, bytes]:
    """Confluent wire format: magic byte 0, 4-byte big-endian schema ID, Avro body."""
    if len(raw) < 5 or raw[0] != 0:
        raise ValueError("message is not in Confluent schema-registry format")
    return struct.unpack(">I", raw[1:5])[0], raw[5:]


# --- Avro schema -> Arrow schema ---------------------------------------------

PRIMITIVES = {
    "null": pa.null(), "boolean": pa.bool_(), "int": pa.int32(), "long": pa.int64(),
    "float": pa.float32(), "double": pa.float64(), "bytes": pa.binary(), "string": pa.string(),
}
LOGICAL = {
    "date": pa.date32(), "time-millis": pa.time32("ms"), "time-micros": pa.time64("us"),
    "timestamp-millis": pa.timestamp("ms", tz="UTC"), "timestamp-micros": pa.timestamp("us", tz="UTC"),
    "local-timestamp-millis": pa.timestamp("ms"), "local-timestamp-micros": pa.timestamp("us"),
    "uuid": pa.string(),
}


def avro_to_arrow(t, named=None, namespace=""):
    """Convert an Avro type to (arrow_type, nullable)."""
    named = {} if named is None else named
    if isinstance(t, str):
        if t in PRIMITIVES:
            return PRIMITIVES[t], t == "null"
        full = t if "." in t or not namespace else f"{namespace}.{t}"
        return named.get(full, named.get(t)), False  # reference to a named type

    if isinstance(t, list):  # union
        branches = [b for b in t if b != "null"]
        if len(branches) != 1:
            raise NotImplementedError(f"union with several non-null types: {t}")
        arrow_type, _ = avro_to_arrow(branches[0], named, namespace)
        return arrow_type, "null" in t

    kind, logical = t["type"], t.get("logicalType")
    if logical == "decimal":
        return pa.decimal128(t["precision"], t.get("scale", 0)), False
    if logical in LOGICAL:
        return LOGICAL[logical], False

    if kind == "record":
        ns = t.get("namespace", namespace)
        full = t["name"] if "." in t["name"] or not ns else f"{ns}.{t['name']}"
        fields = []
        for f in t["fields"]:
            ft, nullable = avro_to_arrow(f["type"], named, ns)
            fields.append(pa.field(f["name"], ft, nullable=nullable))
        named[full] = named[t["name"]] = pa.struct(fields)
        return named[full], False
    if kind == "array":
        item, nullable = avro_to_arrow(t["items"], named, namespace)
        return pa.list_(pa.field("item", item, nullable=nullable)), False
    if kind == "map":
        val, _ = avro_to_arrow(t["values"], named, namespace)
        return pa.map_(pa.string(), val), False
    if kind in ("enum", "fixed"):
        arrow_type = pa.string() if kind == "enum" else pa.binary(t["size"])
        named[t["name"]] = arrow_type
        if "namespace" in t or namespace:
            named[f"{t.get('namespace', namespace)}.{t['name']}"] = arrow_type
        return arrow_type, False
    return avro_to_arrow(kind, named, namespace)  # e.g. {"type": "string"}


def arrow_schema(avro_schema) -> pa.Schema:
    arrow_struct, _ = avro_to_arrow(avro_schema)
    return pa.schema(list(arrow_struct))


# --- Parquet: stream records out one row group at a time ------------------------

@dataclass
class _SchemaOutput:
    parsed: dict
    arrow: pa.Schema
    path: str
    writer: pq.ParquetWriter | None = None
    buf: list = field(default_factory=list)
    rows: int = 0


class ParquetSink:
    """Decodes raw messages and streams them into one Parquet file per schema ID."""

    def __init__(self, out_path: str, fetch_schema, row_group_size: int = 122_880):
        self.out_path = out_path
        self.stem, self.ext = os.path.splitext(out_path)
        self.fetch_schema = fetch_schema
        self.row_group_size = row_group_size
        self.outputs: dict[int, _SchemaOutput] = {}

    def add(self, raw: bytes):
        schema_id, body = split_header(raw)
        out = self.outputs.get(schema_id)
        if out is None:
            avro_schema = json.loads(self.fetch_schema(schema_id))
            out = self.outputs[schema_id] = _SchemaOutput(
                parsed=fastavro.parse_schema(avro_schema),
                arrow=arrow_schema(avro_schema),
                path=f"{self.stem}.schema{schema_id}{self.ext}",
            )
        # fastavro returns plain Python values: unions are already unwrapped.
        out.buf.append(fastavro.schemaless_reader(io.BytesIO(body), out.parsed))
        if len(out.buf) >= self.row_group_size:
            self._flush(out)

    def _flush(self, out: _SchemaOutput):
        if not out.buf:
            return
        if out.writer is None:
            out.writer = pq.ParquetWriter(out.path, out.arrow)
        out.writer.write_table(pa.Table.from_pylist(out.buf, schema=out.arrow))
        out.rows += len(out.buf)
        out.buf = []

    def close(self) -> list[tuple[str, int]]:
        """Flush and close every file. Returns [(path, rows)]."""
        for out in self.outputs.values():
            self._flush(out)
            if out.writer is not None:
                out.writer.close()
        # A single schema keeps the file name you asked for.
        if len(self.outputs) == 1:
            out = next(iter(self.outputs.values()))
            os.replace(out.path, self.out_path)
            out.path = self.out_path
        return [(o.path, o.rows) for o in self.outputs.values()]


# --- CLI ---------------------------------------------------------------------

UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(v: str) -> float:
    """Seconds, either a plain number (3600) or with a unit suffix (30s, 15m, 2h, 7d)."""
    try:
        secs = float(v[:-1]) * UNITS[v[-1]] if v and v[-1] in UNITS else float(v)
    except ValueError:
        raise argparse.ArgumentTypeError(f"bad duration {v!r}: want seconds (3600) or 30s, 15m, 2h, 7d")
    if not 0 < secs < float("inf"):
        raise argparse.ArgumentTypeError("duration must be positive")
    return secs


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("-b", default=os.environ.get("KAFKA_BROKER", "localhost:9094"),
                    help="broker (env KAFKA_BROKER)")
    ap.add_argument("-r", default=os.environ.get(
        "KAFKA_SCHEMA_REGISTRY", "https://schemaregistry.cper.eng.neoninternal.org"),
                    help="schema registry URL (env KAFKA_SCHEMA_REGISTRY)")
    ap.add_argument("topic", help="Kafka topic")
    which = ap.add_mutually_exclusive_group()
    which.add_argument("-n", type=int, default=10, help="last N messages per partition (default 10)")
    which.add_argument("-s", type=parse_duration, metavar="DURATION",
                       help="read messages from this long ago to now: seconds (3600) or 30s, 15m, 2h, 7d")
    which.add_argument("--all", action="store_true", help="read the entire topic")
    ap.add_argument("-o", default="out.parquet", help="output Parquet file")
    ap.add_argument("--row-group-size", type=int, default=122_880,
                    help="rows per Parquet row group (default 122,880, same as DuckDB)")
    args = ap.parse_args()

    since_ms = None if args.s is None else int((time.time() - args.s) * 1000)
    sink = ParquetSink(args.o, registry_fetcher(args.r), args.row_group_size)
    count = 0
    try:
        for raw in read_raw(args.b, args.topic, None if args.all else args.n, since_ms):
            sink.add(raw)
            count += 1
            if count % 100_000 == 0:
                print(f"  {count:,} messages", file=sys.stderr)
    except KeyboardInterrupt:
        print(f"interrupted after {count:,} messages; keeping what was read", file=sys.stderr)
    finally:
        # Always close, so files are valid Parquet even after Ctrl-C or an error.
        results = sink.close()

    if not results:
        print("no messages", file=sys.stderr)
    for path, rows in results:
        print(f"wrote {rows:,} rows -> {path}", file=sys.stderr)


if __name__ == "__main__":
    main()
