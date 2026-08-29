# 07 — Multi-Outbox-Table → Multi-Exchange Routing

> Builds on files 01–06, and directly answers the question you raised: with multiple outbox tables (e.g., one per aggregate type — patients, appointments, billing, etc.), how do you route each table's events to a **different** RabbitMQ exchange, instead of everything funneling into one?

> **Note on scope**: exact property names for Debezium's SMT (single message transform) framework can shift slightly between Debezium versions, and the RabbitMQ sink extension in particular is less heavily documented than the Kafka path. Treat the property names below as accurate for the general mechanism and cross-check them against the Debezium docs / release notes for your exact `3.0.0.Final` version before wiring this into anything beyond a learning environment. The *concepts and architecture* below are solid regardless of minor property-name drift.

---

## 1. Recap: Why This Is a Real Constraint (Not an Oversight)

From file 06, section 3: a Debezium Server instance's RabbitMQ sink is configured with a **single** `debezium.sink.rabbitmq.exchange` value. Every change event this instance's connector captures — across however many tables are in `table.include.list` — gets published to that one exchange. There is no built-in "table A goes to exchange A, table B goes to exchange B" switch at the sink-configuration level, because the sink config is deliberately simple/global, not per-table.

So you have two structurally different ways to solve this, and they trade off differently:

- **Approach A — Multiple Debezium Server instances**, one process per outbox table (and therefore per exchange).
- **Approach B — Single instance, single logical connector, but with a transform that dynamically re-routes events per-table** before they reach the sink.

---

## 2. Approach A: One Debezium Server Instance Per Outbox Table

This is the simplest mental model: just replicate your existing service block, changing three things per copy — the table, the exchange, and the storage/port paths (so they don't collide).

```yaml
services:
  debezium-patient-outbox:
    image: debezium/server:3.0.0.Final
    container_name: debezium-patient-outbox
    network_mode: "host"
    restart: on-failure:3
    environment:
      DEBEZIUM_SINK_TYPE: rabbitmq
      DEBEZIUM_SINK_RABBITMQ_CONNECTION_HOST: 127.0.0.1
      DEBEZIUM_SINK_RABBITMQ_CONNECTION_PORT: 5672
      DEBEZIUM_SINK_RABBITMQ_USERNAME: guest
      DEBEZIUM_SINK_RABBITMQ_PASSWORD: guest
      DEBEZIUM_SINK_RABBITMQ_EXCHANGE: CreatePatientWrite

      DEBEZIUM_SOURCE_OFFSET_STORAGE: org.apache.kafka.connect.storage.FileOffsetBackingStore
      DEBEZIUM_SOURCE_OFFSET_STORAGE_FILE_FILENAME: /tmp/patient-offsets.dat
      DEBEZIUM_SOURCE_SCHEMA_HISTORY_INTERNAL: io.debezium.storage.file.history.FileSchemaHistory
      DEBEZIUM_SOURCE_SCHEMA_HISTORY_INTERNAL_FILE_FILENAME: /tmp/patient-schema-history.dat

      DEBEZIUM_SOURCE_CONNECTOR_CLASS: io.debezium.connector.mysql.MySqlConnector
      DEBEZIUM_SOURCE_DATABASE_HOSTNAME: 127.0.0.1
      DEBEZIUM_SOURCE_DATABASE_PORT: 3306
      DEBEZIUM_SOURCE_DATABASE_USER: root
      DEBEZIUM_SOURCE_DATABASE_PASSWORD: root
      DEBEZIUM_SOURCE_DATABASE_SERVER_ID: 54321
      DEBEZIUM_SOURCE_TOPIC_PREFIX: eazybank-mysql-cluster
      DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST: cardsdb.create_patient_outbox_table
      DEBEZIUM_SOURCE_TOMBSTONES_ON_DELETE: "false"
      QUARKUS_HTTP_PORT: 8089

  debezium-appointment-outbox:
    image: debezium/server:3.0.0.Final
    container_name: debezium-appointment-outbox
    network_mode: "host"
    restart: on-failure:3
    environment:
      DEBEZIUM_SINK_TYPE: rabbitmq
      DEBEZIUM_SINK_RABBITMQ_CONNECTION_HOST: 127.0.0.1
      DEBEZIUM_SINK_RABBITMQ_CONNECTION_PORT: 5672
      DEBEZIUM_SINK_RABBITMQ_USERNAME: guest
      DEBEZIUM_SINK_RABBITMQ_PASSWORD: guest
      DEBEZIUM_SINK_RABBITMQ_EXCHANGE: CreateAppointmentWrite   # <-- different exchange

      DEBEZIUM_SOURCE_OFFSET_STORAGE: org.apache.kafka.connect.storage.FileOffsetBackingStore
      DEBEZIUM_SOURCE_OFFSET_STORAGE_FILE_FILENAME: /tmp/appointment-offsets.dat   # <-- must not collide
      DEBEZIUM_SOURCE_SCHEMA_HISTORY_INTERNAL: io.debezium.storage.file.history.FileSchemaHistory
      DEBEZIUM_SOURCE_SCHEMA_HISTORY_INTERNAL_FILE_FILENAME: /tmp/appointment-schema-history.dat

      DEBEZIUM_SOURCE_CONNECTOR_CLASS: io.debezium.connector.mysql.MySqlConnector
      DEBEZIUM_SOURCE_DATABASE_HOSTNAME: 127.0.0.1
      DEBEZIUM_SOURCE_DATABASE_PORT: 3306
      DEBEZIUM_SOURCE_DATABASE_USER: root
      DEBEZIUM_SOURCE_DATABASE_PASSWORD: root
      DEBEZIUM_SOURCE_DATABASE_SERVER_ID: 54322                # <-- must be unique per instance
      DEBEZIUM_SOURCE_TOPIC_PREFIX: eazybank-mysql-cluster-appt
      DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST: cardsdb.create_appointment_outbox_table
      DEBEZIUM_SOURCE_TOMBSTONES_ON_DELETE: "false"
      QUARKUS_HTTP_PORT: 8090                                  # <-- must be unique per instance (host networking)
```

### The three things that MUST be unique per instance (and why)
1. **`DEBEZIUM_SOURCE_DATABASE_SERVER_ID`** — per file 05, section 5, MySQL replication requires a unique numeric ID per "replica" connected to the primary. Two Debezium instances with the same server ID will cause replication protocol errors.
2. **Offset & schema history file paths** — since both containers use `network_mode: "host"` and (implicitly, given no volume mount) each has its own isolated container filesystem, this isn't strictly a *collision* risk between containers technically — but naming them distinctly is still good practice for clarity and becomes mandatory the moment you introduce shared volumes for persistence (file 06, section 4's hardening note).
3. **`QUARKUS_HTTP_PORT`** — because you're using `network_mode: "host"`, **all containers share the actual host's network stack**. Two containers both trying to bind port `8089` on the host would conflict directly (unlike normal Docker bridge networking, where each container gets its own internal IP and port collisions across containers aren't possible). This is a direct, practical consequence of the host-networking choice noted back in file 06.

### Tradeoffs of Approach A
**Pros:**
- Dead simple to reason about — one process, one table, one exchange, one blast radius. If the "appointments" Debezium instance crashes, "patients" CDC is completely unaffected.
- No dependency on SMT configuration correctness — just plain source/sink wiring you already understand fully from files 05–06.
- Easy to scale/tune independently per table (e.g., give a high-volume table its own resource limits).

**Cons:**
- Operational overhead scales linearly with table count — 10 outbox tables means 10 containers to deploy, monitor, and keep alive.
- Each instance opens its own separate MySQL replication connection — for a handful of tables this is trivial overhead, but at high table counts it's worth being aware this isn't "free."
- Duplicated, copy-pasted configuration across services is a maintenance burden (though this is very manageable with Docker Compose YAML anchors/extensions, or a proper orchestrator's templating if you move beyond Compose later).

---

## 3. Approach B: Single Instance, Outbox Event Router SMT

Debezium ships a purpose-built transform specifically for the outbox pattern: the **`io.debezium.transforms.outbox.EventRouter`** SMT. This is the more "official," architecturally elegant answer to your exact scenario — it was designed precisely so that a single generic outbox table (or, with configuration, multiple outbox tables sharing a common shape) can route each event to a **different destination** based on a column value, rather than needing one Debezium process per destination.

### The idea: one outbox table shape, an `aggregate_type` column drives routing
The canonical outbox-pattern table design (mentioned briefly in file 05, section 3) includes an `aggregate_type` column — e.g., `"Patient"`, `"Appointment"`, `"Billing"`. The Event Router SMT reads this column per event and uses it to compute the **destination name** dynamically, instead of the destination being fixed by static config.

Conceptually, in Kafka-Connect-property form (translated to Debezium-Server env-var form below):

```
debezium.transforms=outbox
debezium.transforms.outbox.type=io.debezium.transforms.outbox.EventRouter
debezium.transforms.outbox.route.by.field=aggregate_type
debezium.transforms.outbox.route.topic.replacement=${routedByValue}
debezium.transforms.outbox.table.field.event.key=aggregate_id
debezium.transforms.outbox.table.field.event.payload=payload
```

As Debezium Server environment variables, this becomes something like:

```yaml
DEBEZIUM_TRANSFORMS: outbox
DEBEZIUM_TRANSFORMS_OUTBOX_TYPE: io.debezium.transforms.outbox.EventRouter
DEBEZIUM_TRANSFORMS_OUTBOX_ROUTE_BY_FIELD: aggregate_type
DEBEZIUM_TRANSFORMS_OUTBOX_ROUTE_TOPIC_REPLACEMENT: "${routedByValue}"
DEBEZIUM_TRANSFORMS_OUTBOX_TABLE_FIELD_EVENT_KEY: aggregate_id
DEBEZIUM_TRANSFORMS_OUTBOX_TABLE_FIELD_EVENT_PAYLOAD: payload
```

What this does: instead of every event landing on a fixed topic/destination name, the SMT computes a **per-event destination name** from the `aggregate_type` column's value (e.g., an event with `aggregate_type = "Patient"` gets routed to a destination literally named `Patient`, one with `aggregate_type = "Appointment"` gets routed to `Appointment`, etc.), and it also **flattens the envelope** — instead of your consumer needing to unwrap `envelope.payload.after.payload` (file 03's double-deserialize), the SMT can emit the `payload` column's content more directly as the message body, with `aggregate_id` promoted to the message key. This is a meaningfully cleaner consumer experience than what your current raw setup produces.

### The remaining piece: mapping computed destination names to actual RabbitMQ exchanges
This is the part where Debezium Server's RabbitMQ sink extension's maturity genuinely matters, and where you should verify current behavior against the docs for `3.0.0.Final` specifically: the Kafka-oriented mental model treats the SMT's computed "topic name" as a literal Kafka topic name, and Kafka Connect sink connectors can typically do topic → destination mapping quite flexibly. For the **RabbitMQ sink** specifically, whether the computed per-event destination name is honored as a dynamic **exchange name** (ideal — exactly what you want) versus only usable as a **routing key** within your one statically-configured exchange (still very useful, just architecturally different) depends on the exact sink extension version's implementation.

**Practical recommendation**: given this nuance, the most robust starting point for a learning project is:
1. Implement the Event Router SMT to get the flattened payload and `aggregate_type`/`aggregate_id` benefits regardless.
2. Configure your **single** static exchange as a **topic exchange** (file 02, section 2) instead of the default fanout.
3. Verify (empirically, by running it and inspecting a captured message) whether the SMT's computed destination lands as the exchange name or as the routing key.
4. If it lands as the **routing key**, that's actually a perfectly good outcome — you then create **separate queues per aggregate type**, each bound to the single topic exchange with a binding pattern matching that aggregate's routing key (e.g., a queue bound with binding key `Patient` receives only patient events, a queue bound with `Appointment` receives only appointment events) — functionally achieving the same "different tables → different downstream consumer queues" goal, just implemented as topic-exchange routing-key filtering rather than literally separate exchanges.

### Tradeoffs of Approach B
**Pros:**
- One Debezium process, one MySQL replication connection, regardless of outbox table count — much lower operational overhead than Approach A at scale.
- Cleaner consumer-side payload (flattened envelope, explicit key) — arguably a strict upgrade over your current manual double-deserialize even independent of the multi-table question.
- This is the pattern Debezium's own documentation and community consider the "correct" outbox-pattern implementation — you'd be aligning with established best practice rather than a bespoke workaround.

**Cons:**
- More configuration surface, and (as flagged above) some genuine version-specific uncertainty on the RabbitMQ sink's exact exchange-vs-routing-key behavior that you'll need to verify empirically for `3.0.0.Final`.
- A single process is a **single point of failure** for CDC across all your outbox tables — if this one instance goes down, every table's event stream pauses, versus Approach A where only the affected table's stream would pause.
- Requires redesigning your outbox tables to share the `aggregate_type`/`aggregate_id`/`payload` convention (or using multiple differently-configured SMT chains) if your existing tables (like `create_patient_outbox_table`) don't already follow that exact generic shape — your current table name suggests a **per-event-type dedicated outbox table** design (`create_patient_outbox_table` specifically, rather than one generic `outbox_events` table), which is itself a valid variant of the outbox pattern, just one that doesn't need `route.by.field` at all — see the note below.

---

## 4. A Third, Simpler Option Worth Naming: You May Already Be Closer to Solved Than You Think

Your actual table is named `create_patient_outbox_table` — singular-purpose, not a shared generic `outbox_events` table. If your design intentionally uses **one dedicated outbox table per event type** (rather than one shared generic outbox table with an `aggregate_type` discriminator column), then you don't need dynamic routing logic at all — **Approach A (one Debezium instance per table) is not just simpler, it's actually the more natural fit for your existing schema design**, since each table already has an unambiguous 1:1 relationship with exactly one exchange.

The Event Router SMT (Approach B) earns its complexity specifically when you have (or want to move to) a **single shared outbox table** serving multiple aggregate/event types — which is a legitimate alternative design (fewer tables to manage, one transactional-write path for all outbox inserts) but is a genuinely different schema decision, not a strict improvement over per-type tables. Worth treating this as a design choice to make deliberately, rather than assuming one approach is objectively "more correct" — both are established, valid variants of the outbox pattern used in real production systems.

---

## 5. Recommendation Summary

| Your situation | Recommended approach |
|---|---|
| A handful of dedicated, per-type outbox tables (matches your current `create_patient_outbox_table` naming) | **Approach A** — one Debezium Server instance per table/exchange. Simple, matches your schema, easy to reason about. |
| Many outbox tables, or planning to consolidate into a shared generic outbox table | **Approach B** — Event Router SMT with `route.by.field`, single instance, topic-exchange + routing-key-based downstream queue separation. |
| Unsure yet / still learning | Start with **Approach A** (you already have a fully working single-table version of this exact config) — it's the lowest-risk way to get a second table flowing end-to-end, and you can revisit Approach B later once the operational overhead of N instances actually becomes a real pain point rather than a theoretical one. |

---

## 6. Series Wrap-Up — What We've Covered

At this point, the full documentation set covers:
1. **01** — Message broker fundamentals (decoupling, delivery guarantees, ack models, retry/DLQ pattern, broker families)
2. **02** — RabbitMQ deep dive (AMQP model, exchange types, connections/channels, protocol-level ack/nack, dead-lettering mechanics)
3. **03** — Spring Cloud Stream functional model + full `CreatePatientInReadDb` code walkthrough
4. **04** — Full yml deep dive (group/destination, Spring Retry vs. binder-level retries, DLQ republish mechanics, full failure-journey trace)
5. **05** — Debezium fundamentals (CDC, dual-write problem, outbox pattern, internal mechanics, MySQL binlog specifics)
6. **06** — Debezium Server standalone model + full compose env-var breakdown
7. **07** — Multi-outbox-table → multi-exchange routing strategies (this file)

This gives you a complete, git-committable reference trail from "what is a broker" all the way to your specific multi-table Debezium/RabbitMQ topology decision. If you want to extend the series further — e.g., a file on idempotent-consumer implementation patterns for MongoDB upserts, or a file specifically on securing this pipeline (scoped MySQL replication users, RabbitMQ vhosts/permissions, TLS) — just say the word and we can add it using the same format.
