# Kafka Connect FlattenNested SMT

[![CI](https://github.com/shubhajit93/kafka-connect-flatten-smt/actions/workflows/ci.yml/badge.svg)](https://github.com/shubhajit93/kafka-connect-flatten-smt/actions/workflows/ci.yml)
![Java 11+](https://img.shields.io/badge/Java-11%2B-007396?logo=openjdk&logoColor=white)
![Kafka Connect](https://img.shields.io/badge/Kafka%20Connect-3.x-231F20?logo=apachekafka&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)

A Kafka Connect **Single Message Transform** that flattens **nested structs, arrays, and maps** into flat columns, so any topic can go straight into a relational sink such as the JDBC sink connector.

```text
{ "id": 1,                                       id | customer_name | customer_address_city | tags         | items
  "customer": { "name": "Ana",            ──►   ----+---------------+-----------------------+--------------+------------------------------
                "address": { "city": "Dhaka" } },  1 | Ana           | Dhaka                 | new,priority | [{"sku":"A-1","qty":2},...]
  "tags":  ["new", "priority"],
  "items": [{ "sku": "A-1", "qty": 2 }, ...] }
```

## Why

The JDBC sink can only write primitive columns. Records with nested objects, arrays, or arrays of objects fail with errors like `Unsupported source data type: STRUCT` or `ARRAY`. Kafka's built-in `Flatten` transform handles structs and maps, but **not arrays**. The usual workaround is a custom ETL job for every topic.

I first built this transform for a production analytics pipeline that streamed service data into TimescaleDB for Grafana dashboards. It removed the per-topic ETL jobs and cut processing latency by about 50%. This repository is a **from-scratch, open-source rewrite** of that idea, with a cleaner design and full tests.

## Behavior

| Input | Output |
|---|---|
| Nested struct `customer.address.city` | Column `customer_address_city` (any depth, configurable delimiter) |
| Optional parent struct that is `null` | Its child columns are `null`, and are made optional in the schema so the sink accepts them |
| Array of primitives | `json` mode: `["new","priority"]` · `join` mode: `new,priority` |
| Array of structs, maps, or arrays | Always a JSON string: `[{"sku":"A-1","qty":2}]` |
| Map | JSON string (keys aren't known ahead of time, so they can't be columns) |
| Logical types (Decimal, Date, Time, Timestamp) | Kept as they are on flattened columns. Inside arrays they're rendered as JSON numbers or ISO-8601 strings |
| Schemaless records (`Map`) | Flattened the same way, without a schema |
| Tombstones (`null` value) | Passed through untouched |
| Two fields flatten to the same name | Fails with a clear `DataException` naming the field |

Flattened schemas are cached per input schema, so the per-record cost is just copying values.

## Install

1. Download the jar from [Releases](https://github.com/shubhajit93/kafka-connect-flatten-smt/releases), or build it with `./gradlew jar` (output: `build/libs/`).
2. Copy it into a directory on your worker's `plugin.path`, for example `/opt/connect-plugins/flatten-smt/`.
3. Restart the worker. The jar has **no runtime dependencies** and ships a `ServiceLoader` manifest, so it also works with `plugin.discovery=service_load`.

It requires Java 11+ and Kafka Connect 3.x (including Confluent Platform 7.x).

## Configure

```json
"transforms": "flatten",
"transforms.flatten.type": "io.github.shubhajit93.kafka.connect.smt.FlattenNested$Value",
"transforms.flatten.delimiter": "_",
"transforms.flatten.array.mode": "join",
"transforms.flatten.array.join.separator": ","
```

| Property | Default | Description |
|---|---|---|
| `delimiter` | `_` | Placed between parent and child field names |
| `array.mode` | `json` | `json`: every array becomes a JSON string. `join`: arrays of primitives become a delimited string, and complex arrays fall back to JSON |
| `array.join.separator` | `,` | Element separator in `join` mode |

Use `FlattenNested$Key` to flatten record keys instead.

## Try it end to end

The demo starts Kafka, Kafka Connect with the JDBC sink and this transform, and PostgreSQL. It then produces nested orders and checks the flattened rows:

```bash
./gradlew jar
docker compose up -d --wait
./demo/run-demo.sh
```

```text
 id | customer_name | customer_address_city | customer_address_zip |     tags     |                    items                     | total
----+---------------+-----------------------+----------------------+--------------+----------------------------------------------+-------
  1 | Ana           | Dhaka                 | 1212                 | new,priority | [{"sku":"A-1","qty":2},{"sku":"B-7","qty":1}] |  59.5
  2 | Ben           |                       |                      |              | [{"sku":"C-3","qty":5}]                      |    20
  3 | Chen          | Chattogram            |                      |              | []                                           |     0
```

CI runs this same demo against a real Connect worker on every push.

## Design notes

- **No JSON library.** A small built-in renderer handles Connect data (structs, maps, arrays, logical types, bytes, escaping), so the plugin can never conflict with the worker's own Jackson version.
- **Only optional where it has to be.** Required fields stay required unless they sit under an optional parent. That way the sink's auto-created tables keep `NOT NULL` wherever the data allows it.
- **It fails loudly on collisions.** Silently overwriting a column such as `a_b` would corrupt data, so it fails instead and tells you to pick another delimiter.
- **Java 11 bytecode,** built and tested on JDK 21 with warnings treated as errors.

## Development

```bash
./gradlew build   # compile (release 11) + unit tests
```

---

Built by [Shubhajit Nandi](https://github.com/shubhajit93) · [LinkedIn](https://www.linkedin.com/in/shubhajit-nandi/)
