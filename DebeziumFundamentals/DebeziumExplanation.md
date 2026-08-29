# 05 — Debezium Fundamentals: Change Data Capture & the Outbox Pattern

> Builds on files 01–04. We now shift from "how messages move once they exist" to "where these messages actually come from" — Debezium and Change Data Capture (CDC).

---

## 1. The Problem Debezium Solves: Keeping Two Systems in Sync, Reliably

Your architecture has a write-side database (MySQL, holding `create_patient_outbox_table`) and a read-side database (MongoDB, via `mongoPatientService`). Whenever a patient is created on the write side, the read side needs to find out — reliably, in order, without loss.

The naive approach is: **after** committing the write-side transaction, have the application code **also** publish a message to RabbitMQ. This looks fine until you consider failure modes:

- The DB write succeeds, but the app crashes **before** publishing the message → the read side never finds out. Silent data loss.
- The message publish succeeds, but the DB transaction then rolls back → the read side now has a "phantom" patient that never actually existed on the write side.

This is the classic **dual-write problem**: you cannot atomically commit to two independent systems (a database and a message broker) using two separate operations without a distributed transaction coordinator — and distributed transactions (2PC) across a DB and a broker are operationally painful and rarely used in modern architectures.

---

## 2. Change Data Capture (CDC) — the General Solution

CDC flips the problem around: instead of your application code being responsible for *also* publishing an event, you let the **database's own transaction log** be the single source of truth, and have a separate process **tail that log** and turn each committed row-level change into an event.

Why this solves the dual-write problem: the transaction log is only ever appended to **after a transaction has actually and durably committed**. There is no "the DB write succeeded but the log entry didn't happen" scenario — they're the same atomic operation, by construction, at the database engine level. So a CDC-based pipeline can *never* miss a committed write or fabricate an uncommitted one, without needing any distributed transaction machinery.

Debezium is, at its core, a **CDC engine**: it connects to a database's native replication/logging mechanism (for MySQL, this is the **binlog** — more on this in section 5) and streams every row-level insert/update/delete as a structured event.

---

## 3. The Transactional Outbox Pattern — Why Your `payload` Column Exists

Raw CDC on your actual business tables (e.g., a `patients` table with 20 domain columns) has two problems:
1. **Every column change becomes a raw event**, tightly coupling your event schema to your internal table schema — any DB migration (renaming a column, adding an index) risks breaking downstream consumers.
2. You often want to publish a **richer domain event** (e.g., `PatientCreated` with a specific, versioned shape) rather than "here's the literal row that changed."

The **transactional outbox pattern** solves this by adding a dedicated **outbox table** (your `create_patient_outbox_table`) that your application writes to **as part of the same local database transaction** as the actual business write. This outbox table typically has a shape like:

| Column | Purpose |
|---|---|
| `id` | Primary key / event id |
| `aggregate_type` | e.g., `"Patient"` — what kind of entity this event is about |
| `aggregate_id` | The business entity's id |
| `event_type` | e.g., `"PatientCreated"` |
| `payload` | The **fully-formed, pre-serialized JSON** of the domain event you want downstream consumers to receive |
| `created_at` | Timestamp |

Your application writes to `patients` (or wherever the real business data lives) **and** inserts a row into `create_patient_outbox_table` with the event payload, **in the same local ACID transaction**. Since it's one transaction against one database, there's no dual-write problem at the application level either — it's all-or-nothing, guaranteed by MySQL itself.

Then Debezium is configured (via `DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST: cardsdb.create_patient_outbox_table` in your compose file) to **only** watch the outbox table — not your real business tables. This is precisely why your consumer code does the double-deserialize dance from file 03: Debezium's `after` captures the **outbox row** (whose `payload` column happens to itself contain JSON), so your consumer first parses the Debezium envelope, then parses the `payload` **column's string content** as a second, independent JSON document — the actual domain event. This confirms the outbox pattern is exactly what's happening in your system, and it's a very solid architectural choice.

### Why this pattern is considered a best practice
- **Schema decoupling** — you control the exact shape of the published event independently of your internal table structure.
- **Atomicity without distributed transactions** — the "did we actually create a patient AND schedule an event" question is answered by ordinary local ACID guarantees.
- **Ordering preserved** — since the outbox table is CDC'd via the same log-tailing mechanism, event order matches transaction commit order.
- **Consumer isolation from internal refactors** — you could rename/restructure `patients` table columns freely without ever touching the outbox contract, as long as your outbox-writing code still produces the same `payload` shape.

---

## 4. How Debezium Actually Works, Internally

At a high level, Debezium's engine (regardless of "flavor" — Kafka Connect plugin vs. standalone server, which we'll contrast in section 6) does the following:

1. **Initial snapshot** (first run only, typically): Debezium takes a consistent snapshot of the current state of the included table(s), so downstream consumers get a full picture of existing data, not just future changes. This is why your consumer code has the defensive `envelope.getPayload().getAfter() == null` check — some snapshot/maintenance-related messages don't carry a normal row payload.
2. **Log tailing**: after the snapshot, Debezium switches to continuously reading the database's native change-log (binlog for MySQL — see section 5), starting from the exact log position (`offset`) recorded during the snapshot.
3. **Event construction**: for each row-level change it reads from the log, Debezium builds a structured **change event envelope** containing:
   - `before` — the row's state before the change (`null` for inserts)
   - `after` — the row's state after the change (`null` for deletes)
   - `source` — metadata: which table, which transaction, binlog file/position, timestamp, etc.
   - `op` — the operation: `c` (create), `u` (update), `d` (delete), `r` (read, used during snapshot)
   - `ts_ms` — event timestamp
4. **Offset & schema history persistence**: Debezium continuously records **how far it has read** in the log (the `offset`) and, for schema-evolution-aware connectors, a history of DDL changes it has observed. This is what `DEBEZIUM_SOURCE_OFFSET_STORAGE` and `DEBEZIUM_SOURCE_SCHEMA_HISTORY_INTERNAL` in your compose file are for — we'll cover exactly what "file-based" storage means (and its production tradeoffs) in the next file.
5. **Sink delivery**: the constructed event is hand off to whatever **sink** is configured — in your case, RabbitMQ (`DEBEZIUM_SINK_TYPE: rabbitmq`), which is the standalone-server-specific concept we cover in section 6.

### Why offset tracking matters for reliability
If the Debezium process crashes or restarts, it resumes reading the log from the **last persisted offset**, not from scratch. This means Debezium itself provides at-least-once delivery guarantees at the source: a crash might cause a small number of already-sent events to be resent (offset persisted slightly behind actual progress), but it will never silently skip a committed change. This mirrors the exact at-least-once philosophy from file 01 — and is precisely why your consumer's idempotency considerations (also discussed in file 01, section 4) matter here too: Debezium redelivering an event on restart, combined with RabbitMQ's own at-least-once redelivery, means your Mongo write should ideally be an upsert keyed on a stable business id rather than a blind insert.

---

## 5. MySQL Specifically: the Binlog

Your compose file uses `io.debezium.connector.mysql.MySqlConnector`. Here's what makes MySQL CDC specifically work (this becomes relevant later when we contrast it against, say, Postgres):

- MySQL supports a feature called the **binary log (binlog)**, originally built for **replication** (a MySQL replica/slave reads the primary's binlog to stay in sync). Debezium essentially **impersonates a MySQL replica**: it connects to your MySQL server using the same replication protocol a real replica would use, and requests to be streamed binlog events from a given position.
- For this to work, MySQL must be configured with:
  - `binlog_format = ROW` — critical. Row-based binlog format records the actual **row data** that changed (before/after values), rather than just the SQL statement that was executed (`STATEMENT` format) or a hybrid (`MIXED`). Debezium **requires ROW format** because it needs actual row-level before/after data to construct its change events — a `STATEMENT`-format binlog entry like `UPDATE patients SET status = 'active' WHERE created_at < NOW()` doesn't tell Debezium *which specific rows* changed or what their values became without re-executing arbitrary SQL logic, which defeats the purpose.
  - A dedicated **replication user** with `REPLICATION SLAVE` and `REPLICATION CLIENT` privileges (your compose uses `root`, which works for local dev/learning but would never be appropriate in production — a scoped replication-only user is the correct pattern there).
  - `DEBEZIUM_SOURCE_DATABASE_SERVER_ID: 54321` — MySQL replication requires every "replica" (real or, in this case, Debezium acting as one) connected to a primary to have a **unique numeric server ID** across the whole replication topology, so the primary can distinguish between multiple replicas/consumers of its binlog. If you ever ran two Debezium instances (or a real replica) against the same MySQL server with a colliding server ID, you'd get replication errors — worth remembering as you scale this setup.

This is meaningfully different from, say, **Postgres CDC**, which uses **logical replication slots** and the **write-ahead log (WAL)** with a decoding plugin (e.g., `pgoutput` or `wal2json`) instead of a binlog — conceptually similar (tail a native replication-oriented log) but with different setup mechanics, privilege models, and connector configuration keys. Since you're explicitly on MySQL, everything from here forward in this series will use binlog-specific terminology, but it's worth knowing this distinction exists if you ever compare notes with someone running Debezium against Postgres.

---

## 6. What's Next

In **06-debezium-server-and-compose.md**, we go through your actual docker-compose environment variables property by property — the standalone Debezium Server model (vs. the Kafka Connect plugin model), what the file-based offset/schema-history storage means and its tradeoffs, and exactly how the RabbitMQ sink type turns each change event into an exchange publish. Then in **07-multi-outbox-multi-exchange.md**, we tackle your actual question: how to configure multiple outbox tables so that each one routes to a *different* RabbitMQ exchange, using table-based and/or content-based routing.
