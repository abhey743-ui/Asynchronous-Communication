# 06 — Debezium Server (Standalone) & Your Compose Config, Property by Property

> Builds on files 01–05. Here we cover the standalone Debezium Server deployment model specifically (since that's what you're using), then walk your actual docker-compose environment variables one by one.

---

## 1. Two Ways to Run Debezium — and Why the Distinction Matters

Debezium doesn't ship as a single "the app" — it's fundamentally a **library of source connectors** (MySQL, Postgres, MongoDB, SQL Server, Oracle, etc.) that needs a **runtime/host** to actually execute inside. There are two common hosts:

### Model A: Kafka Connect Plugin (the "classic"/most common deployment)
- Debezium connectors run **inside a Kafka Connect cluster**, as plugins.
- Kafka Connect provides the scaffolding: REST API for managing connectors, distributed task assignment, offset/config storage **as Kafka topics themselves**, and built-in scaling (Kafka Connect can distribute connector "tasks" across a worker cluster).
- Sink is **always Kafka** — if you want the final destination to be RabbitMQ, you'd typically need a **second hop**: Kafka Connect sink connector (e.g., a Kafka-to-RabbitMQ bridge) or your own consumer reading from Kafka and republishing to RabbitMQ.
- Heavier operationally (you need a running Kafka cluster + Kafka Connect workers), but battle-tested at scale and gives you Kafka's replay/retention benefits as a side effect.

### Model B: Debezium Server (Standalone) — what you're using
- A **single, self-contained Quarkus application** that embeds a Debezium connector directly and ships change events straight to a configurable **sink** — no Kafka required at all.
- Supported sinks include Kafka, **RabbitMQ**, Pulsar, Google Pub/Sub, Amazon Kinesis, Amazon SQS, Azure Event Hubs, Redis Streams, and a few others — configured purely via the `DEBEZIUM_SINK_TYPE` environment variable and sink-specific properties.
- Much lighter operationally for smaller/simpler setups (exactly what makes it a great fit for a learning project or a small service that doesn't otherwise need Kafka in its stack) — you don't have to stand up and operate a Kafka cluster just to get CDC events into RabbitMQ.
- **Tradeoff**: it's a single process (no built-in horizontal scaling/distribution the way Kafka Connect provides — if you need to scale CDC capture itself across multiple workers, Kafka Connect's task-distribution model is the more mature answer). For a single-table (or handful-of-tables), single-instance CDC pipeline — exactly your case — this limitation essentially doesn't matter in practice.

Your compose file confirms Model B directly: `image: debezium/server:3.0.0.Final`, no Kafka broker anywhere in sight, and a direct `DEBEZIUM_SINK_TYPE: rabbitmq`. This is the correct, idiomatic choice for "I want CDC → RabbitMQ with minimal infrastructure," and it's exactly why this file focuses on Debezium Server's own configuration model rather than Kafka Connect's connector-REST-API model (which works completely differently — worth knowing if you ever encounter it elsewhere, but not relevant to your current setup).

---

## 2. Naming Convention: `DEBEZIUM_<SECTION>_<PROPERTY>`

Debezium Server maps environment variables to the underlying Debezium/Kafka-Connect-style configuration property names using a consistent transformation: the env var `DEBEZIUM_SOURCE_DATABASE_HOSTNAME` corresponds to the property `debezium.source.database.hostname`, `DEBEZIUM_SINK_RABBITMQ_EXCHANGE` corresponds to `debezium.sink.rabbitmq.exchange`, and so on (uppercase + underscores ↔ lowercase + dots). Recognizing this pattern means you can always cross-reference the official Debezium documentation (which is written in dotted-property form) against your env-var-based compose file without confusion.

The three top-level sections you'll always see:
- **`debezium.sink.*`** — where events go
- **`debezium.source.*`** — where events come from (the actual connector config — hostname, credentials, tables, etc.)
- Everything else (offset storage, schema history, format) sits alongside these, generally scoped under `source` since it's about tracking the *source's* read progress.

---

## 3. Section 1 of Your Config: The Sink (RabbitMQ)

```yaml
DEBEZIUM_SINK_TYPE: rabbitmq
DEBEZIUM_SINK_RABBITMQ_CONNECTION_HOST: 127.0.0.1
DEBEZIUM_SINK_RABBITMQ_CONNECTION_PORT: 5672
DEBEZIUM_SINK_RABBITMQ_USERNAME: guest
DEBEZIUM_SINK_RABBITMQ_PASSWORD: guest
DEBEZIUM_SINK_RABBITMQ_EXCHANGE: CreatePatientWrite
```

- **`DEBEZIUM_SINK_TYPE: rabbitmq`** activates the RabbitMQ sink extension, which internally uses the RabbitMQ Java client (the same underlying library `spring-amqp` also wraps) to publish messages.
- **Connection properties** (`HOST`/`PORT`/`USERNAME`/`PASSWORD`) — standard AMQP connection parameters, identical in spirit to any RabbitMQ client connection, per file 02's connection/channel model.
- **`DEBEZIUM_SINK_RABBITMQ_EXCHANGE: CreatePatientWrite`** — this is the single most important line for understanding your topology: **the Debezium RabbitMQ sink publishes every captured change event to this one named exchange.** By default, the Debezium RabbitMQ sink creates this exchange as a **fanout exchange** if it doesn't already exist (fanout being the default because Debezium doesn't inherently know or care what routing keys downstream consumers might want — it just broadcasts). This is precisely why your Spring Cloud Stream `destination: CreatePatientWrite` binds a queue to it and receives everything published there.

> **This single-exchange-per-sink-config detail is the crux of the multi-outbox-table question you asked** — with only one `DEBEZIUM_SINK_RABBITMQ_EXCHANGE` value available in a single Debezium Server instance's config, a single Debezium Server process can only target **one exchange** by default. We'll solve this properly in file 07 with two concrete strategies: running multiple Debezium Server instances (one per outbox table/exchange), or using routing-key-based fanout from a single instance via a topic exchange and per-table routing key derivation. Flagging it now so you can see exactly *why* it's a real constraint, not just an arbitrary limitation.

### `network_mode: "host"`
Worth a brief note since it appears in your compose file: this makes the container share the host machine's network namespace directly, rather than getting its own Docker bridge-network IP. This is why `127.0.0.1` works for both `DEBEZIUM_SINK_RABBITMQ_CONNECTION_HOST` and `DEBEZIUM_SOURCE_DATABASE_HOSTNANE` — the container is, network-wise, "the host itself," so it can reach RabbitMQ and MySQL running directly on your machine via localhost. This is a convenient shortcut for local development, but note it's **Linux-only** (Docker Desktop on Mac/Windows doesn't support true host networking the same way) and isn't the pattern you'd use in a real multi-container/orchestrated deployment (there, you'd use a shared Docker network and service-name-based DNS resolution instead, e.g., `DEBEZIUM_SINK_RABBITMQ_CONNECTION_HOST: rabbitmq`).

---

## 4. Section 2: Offset Storage — "How Far Have I Read?"

```yaml
DEBEZIUM_SOURCE_OFFSET_STORAGE: org.apache.kafka.connect.storage.FileOffsetBackingStore
DEBEZIUM_SOURCE_OFFSET_STORAGE_FILE_FILENAME: /tmp/offsets.dat
```

As covered conceptually in file 05, section 4: Debezium needs to persist exactly where in the MySQL binlog it has read up to, so a restart resumes rather than reprocessing from the start (or, worse, missing everything since the last snapshot). `FileOffsetBackingStore` is the simplest possible implementation — it writes this position to a flat file on disk.

**Production caveat worth flagging clearly**: this file lives at `/tmp/offsets.dat` **inside the container**, and your compose doesn't define a volume mount for it. That means:
- If the container is removed/recreated (not just restarted — actually removed), this file is lost, and Debezium will have no memory of its prior read position.
- Without an explicit `debezium.source.snapshot.mode` decision at that point, this could trigger a full re-snapshot (re-publishing every existing row again) or, depending on config, fail to find a valid resume point.

For a learning project this is completely fine and expected. For anything resembling production, the standard recommendation is to either (a) mount `/tmp` (or wherever you configure it) to a persistent Docker volume so the file survives container recreation, or (b) switch to a more robust offset store — Debezium Server also supports storing offsets in a real datastore (e.g., a dedicated Postgres/MySQL table, or Redis) via different storage implementations, which is far more resilient than a bare file in an ephemeral container filesystem. Worth keeping in your back pocket as a "things to harden later" note.

---

## 5. Section 3: Schema History — Why It's Needed at All

```yaml
DEBEZIUM_SOURCE_SCHEMA_HISTORY_INTERNAL: io.debezium.storage.file.history.FileSchemaHistory
DEBEZIUM_SOURCE_SCHEMA_HISTORY_INTERNAL_FILE_FILENAME: /tmp/schema_history.dat
```

This one confuses people because it sounds redundant with offset storage, but it solves a genuinely different problem. The MySQL binlog, in `ROW` format, records **row data** — but to correctly interpret those raw row bytes (which columns they correspond to, in which order, of which types), Debezium needs to know the **table schema at the exact point in binlog history each event occurred**.

Why does this matter specifically? Because **schemas change over time** (someone runs `ALTER TABLE ADD COLUMN`). If Debezium didn't track schema history, and it needed to reprocess an older binlog position (e.g., after a restart, or during a historical snapshot), it could misinterpret old row data using the *current* schema, corrupting the data it emits. So Debezium **also captures DDL statements** (`CREATE TABLE`, `ALTER TABLE`, etc.) it observes in the binlog and maintains a full history of "what did this table's schema look like at each point in time," so it can always correctly decode row events regardless of when they occurred relative to schema changes.

`FileSchemaHistory` is, again, the simplest possible backing store — a flat file. The same production caveat from section 4 applies here too: no volume mount means this history is lost if the container is recreated, which (depending on your MySQL connector's snapshot/history-recovery settings) could force Debezium into a fresh initial snapshot on next startup rather than being able to correctly resume.

---

## 6. Section 4: The MySQL Source Connector

```yaml
DEBEZIUM_SOURCE_CONNECTOR_CLASS: io.debezium.connector.mysql.MySqlConnector
DEBEZIUM_SOURCE_DATABASE_HOSTNAME: 127.0.0.1
DEBEZIUM_SOURCE_DATABASE_PORT: 3306
DEBEZIUM_SOURCE_DATABASE_USER: root
DEBEZIUM_SOURCE_DATABASE_PASSWORD: root
DEBEZIUM_SOURCE_DATABASE_SERVER_ID: 54321
DEBEZIUM_SOURCE_TOPIC_PREFIX: eazybank-mysql-cluster
DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST: cardsdb.create_patient_outbox_table
DEBEZIUM_SOURCE_TOMBSTONES_ON_DELETE: "false"
```

- **`CONNECTOR_CLASS`** — selects the MySQL-specific connector implementation (there's an equivalent class per supported database — `PostgresConnector`, `MongoDbConnector`, etc.). This is what plugs in all the binlog-specific mechanics from file 05, section 5.
- **`DATABASE_HOSTNAME` / `PORT` / `USER` / `PASSWORD`** — standard MySQL connection credentials. As flagged in file 05, using `root` works for local learning but a scoped replication user (`REPLICATION SLAVE`, `REPLICATION CLIENT`, plus `SELECT` on the included tables) is the correct production pattern — principle of least privilege, and it also makes it obvious in an audit which credential is "the CDC reader" versus "the actual application."
- **`DATABASE_SERVER_ID: 54321`** — the unique replication client ID discussed in file 05; must not collide with any other replica/Debezium instance pointed at the same MySQL server.
- **`TOPIC_PREFIX: eazybank-mysql-cluster`** — a slightly confusing name given you're not using Kafka at all here, but this property is a holdover from Debezium's Kafka-Connect-first design: historically, Debezium always generated a Kafka topic name per table as `<topic.prefix>.<database>.<table>`. Even in Debezium Server (non-Kafka) mode, this prefix is still used internally as a **logical namespace/identifier** for the captured stream (and matters for certain internal bookkeeping/schema-history entries), even though there's no literal Kafka topic being created. Think of it as "the logical name of this whole CDC pipeline," not literally a Kafka concept anymore in your setup.
- **`TABLE_INCLUDE_LIST: cardsdb.create_patient_outbox_table`** — the allowlist of exactly which table(s), in `<database>.<table>` form, Debezium should capture. This is the single most important lever for the outbox pattern (file 05, section 3) — it's what ensures Debezium watches *only* your outbox table, not your real business tables. This is also the exact property we'll extend to a **comma-separated list** in file 07 when discussing multiple outbox tables.
- **`TOMBSTONES_ON_DELETE: "false"`** — by default, when a row is deleted, Debezium's Kafka-oriented design emits a normal delete event (with `after: null`) **followed by** a special "tombstone" message (a message with a `null` value entirely) — a convention used for Kafka log-compaction (letting Kafka's compaction process actually remove the row's history from the topic). Since you're not using Kafka at all, tombstone messages serve no purpose in your pipeline and would just be an extra, semantically-empty message your RabbitMQ consumer would need to filter out unnecessarily. Setting this to `false` suppresses that redundant tombstone message — you'll still get the normal delete event (`after: null`), just without the follow-up empty tombstone. This directly explains why your consumer's `envelope.getPayload().getAfter() == null` guard exists — it's handling exactly this kind of delete/non-row-creation event gracefully rather than crashing on it.

---

## 7. Section 5: HTTP Port

```yaml
QUARKUS_HTTP_PORT: 8089
```

Debezium Server is built on **Quarkus**, and exposes a small built-in HTTP server — primarily for **health checks** (`/q/health`) and, if enabled, **metrics** (Micrometer/Prometheus-style, at `/q/metrics`). This isn't the data path at all (no change events flow over HTTP) — it's purely operational tooling. Worth wiring up `/q/health` into whatever monitoring/orchestration you use later (e.g., a Docker healthcheck, or a Kubernetes liveness probe if you ever containerize this beyond local Compose), since it gives you a clean, standard way to know if the Debezium Server process itself is alive and its connector is running, independent of digging through logs.

---

## 8. What's Next

In **07-multi-outbox-multi-exchange.md**, we tackle your actual question directly: with `TABLE_INCLUDE_LIST` extended to multiple outbox tables, how do you get each table's events routed to a **different** RabbitMQ exchange rather than all funneling into the single `DEBEZIUM_SINK_RABBITMQ_EXCHANGE`? We'll cover the routing-based approach (SMTs — single message transforms — for exchange/routing-key derivation) as well as the simpler multi-instance approach, with concrete config for both, and the tradeoffs between them.
