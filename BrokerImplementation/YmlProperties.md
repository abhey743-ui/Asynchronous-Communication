# 04 — The YML, Property by Property: Configuring Retry, DLQ, and Ack Behavior

> Builds on files 01–03. Here we take your exact yml block and go property by property, connecting each one back to the RabbitMQ mechanism it configures (file 02) and the exact behavior it produces in your consumer (file 03).

---

## 1. Your Full Config, For Reference

```yaml
spring:
  cloud:
    function:
      definition: createPatient
    stream:
      bindings:
        createPatient-in-0:
          destination: CreatePatientWrite
          group: patient-read-db-group
          consumer:
            max-attempts: 3
            back-off-initial-interval: 1000
            back-off-multiplier: 2.0
            back-off-max-interval: 10000
      rabbit:
        bindings:
          createPatient-in-0:
            consumer:
              acknowledge-mode: AUTO
              auto-bind-dlq: true
              republish-to-dlq: true
              republish-deliver-mode: PERSISTENT
              dead-letter-queue-name: CreatePatientWrite.patient-read-db-group.dlq
              max-attempts: 1
              requeue-rejected: false
              header-mapper: none
            content-type: application/json
```

Notice the structure has **two parallel sections** for the same binding, `createPatient-in-0`:
- `spring.cloud.stream.bindings.createPatient-in-0.*` — **binder-agnostic** settings (would apply identically whether you were on RabbitMQ or Kafka).
- `spring.cloud.stream.rabbit.bindings.createPatient-in-0.*` — **RabbitMQ-binder-specific** settings (only meaningful because you're using `spring-cloud-stream-binder-rabbit`).

Keeping this distinction in your head is the key to not getting confused by why there appear to be two different `max-attempts` (more on that in section 5 — it's the single most common point of confusion in this config).

---

## 2. `destination` and `group`

```yaml
destination: CreatePatientWrite
group: patient-read-db-group
```

- **`destination`** maps to the RabbitMQ **exchange name** the binder listens against — this connects straight back to file 02's "producer publishes to an exchange" model. The binder will bind a queue to this exchange on your behalf.
- **`group`** is critical and easy to underrate. In Spring Cloud Stream terms, a "group" represents a **logical consumer group** — analogous to a Kafka consumer group. Concretely, for the RabbitMQ binder, the group name is used to construct the **actual queue name**: it becomes `<destination>.<group>` (e.g., `CreatePatientWrite.patient-read-db-group`).

Why does this matter so much?
1. **Without a group**, the binder creates an **anonymous, auto-delete queue** unique to that specific application instance. If you scale to 2 replicas, each gets its *own* private queue and *both* receive a copy of every message (fan-out behavior) — almost never what you want for a "process each event once across your fleet" workload.
2. **With a group**, all instances of your app that share the same `group` value bind to the **same durable, named queue**, and RabbitMQ load-balances (competing-consumer pattern) messages across them — exactly the "scale horizontally, each message processed once" behavior you want for syncing patients to a read DB.
3. The group name is also **directly baked into your DLQ name** — see `dead-letter-queue-name: CreatePatientWrite.patient-read-db-group.dlq` below; this is not a coincidence, it follows the same naming convention.

> This is exactly why the comment in your yml says `# REQUIRED for DLQ/retry to work properly` — without a stable, shared group name, you don't get a stable, shared, durable queue, and the entire DLQ story falls apart.

---

## 3. The Binder-Agnostic Retry Block

```yaml
consumer:
  max-attempts: 3
  back-off-initial-interval: 1000
  back-off-multiplier: 2.0
  back-off-max-interval: 10000
```

This section is handled by **Spring Retry**, orchestrated by Spring Cloud Stream itself — *not* by RabbitMQ. It wraps your `createPatient` function invocation in a `RetryTemplate`-style construct entirely in-process, before any nack/requeue ever happens at the broker level. Concretely:

- **`max-attempts: 3`** — try the function up to 3 times total (1 initial attempt + 2 retries) before giving up.
- **`back-off-initial-interval: 1000`** — wait 1000ms (1s) before the first retry.
- **`back-off-multiplier: 2.0`** — each subsequent wait is 2× the previous one (exponential backoff): 1s → 2s → 4s...
- **`back-off-max-interval: 10000`** — cap the wait at 10s even if the exponential formula would produce more (relevant with more attempts or a steeper multiplier).

**Important nuance**: all of this retrying happens **synchronously, in-memory, on the same consumer thread, without ever touching RabbitMQ**. The message is *not* being nacked and redelivered by the broker 3 times — Spring Retry is just calling your lambda again, in a loop, inside the same message-processing invocation. RabbitMQ only sees the *final* outcome: either an eventual success (ack) or a final failure after all 3 attempts are exhausted (which is when the exception propagates out and RabbitMQ-level nack/DLQ logic in section 4 kicks in). This is exactly why your consumer's `catch (Exception e) { ...; throw e; }` branch says "will retry" — it's Spring Retry doing that retrying, invisibly, before RabbitMQ ever gets involved again.

---

## 4. The RabbitMQ-Specific Consumer Block

```yaml
rabbit:
  bindings:
    createPatient-in-0:
      consumer:
        acknowledge-mode: AUTO
        auto-bind-dlq: true
        republish-to-dlq: true
        republish-deliver-mode: PERSISTENT
        dead-letter-queue-name: CreatePatientWrite.patient-read-db-group.dlq
        max-attempts: 1
        requeue-rejected: false
        header-mapper: none
      content-type: application/json
```

### `acknowledge-mode: AUTO`
Covered in depth in file 03, section 4 — the binder acks on normal return, nacks on exception. This is the setting that makes the entire throw-to-fail convention in your Java code meaningful.

### `auto-bind-dlq: true`
This is the RabbitMQ binder's convenience flag that automatically provisions, at application startup:
- A dead-letter-exchange (by default named `<destination>.<group>.dlx`, i.e. `CreatePatientWrite.patient-read-db-group.dlx`)
- A dead-letter queue bound to it
- The `x-dead-letter-exchange` argument set on your **main** queue, pointing at that DLX

This is literally the mechanism from file 02, section 6 — RabbitMQ's native dead-lettering — just provisioned declaratively instead of you writing `channel.queueDeclare` / `channel.exchangeDeclare` / `channel.queueBind` calls by hand.

### `dead-letter-queue-name: CreatePatientWrite.patient-read-db-group.dlq`
Overrides the binder's default DLQ naming convention with an explicit name. Functionally identical to what the default would produce here (since it follows the same `<destination>.<group>.dlq` pattern) — but being explicit is good practice since it makes the DLQ name discoverable directly from yml without needing to know the binder's internal default-naming algorithm, which is genuinely useful when you're setting up RabbitMQ monitoring/alerting dashboards later and need to know the exact queue name to watch.

### `republish-to-dlq: true` — the subtle but important one
Without this flag, RabbitMQ's *native* dead-lettering mechanism would still work — but it dead-letters via a low-level protocol mechanism (nack with no requeue + `x-dead-letter-exchange` on the queue), and the message that lands in the DLQ is just the **original message bytes plus RabbitMQ's own `x-death` header array** (recording queue name, exchange, reason, count).

With `republish-to-dlq: true`, Spring Cloud Stream instead takes over the dead-lettering itself: it catches the final failure, and **explicitly republishes** the message to the DLQ using its own producer, **adding rich diagnostic headers** — critically, the **exception message and stack trace** of what actually went wrong, as message headers. This is enormously valuable for debugging: instead of just knowing "this message failed and got dead-lettered," you can inspect the DLQ message headers and see *exactly* what exception and stack trace caused it, without needing to correlate against application logs from whenever it originally failed.

### `republish-deliver-mode: PERSISTENT`
Ensures the republished DLQ message is marked as **persistent** (survives a broker restart, written to disk) rather than transient (in-memory only, lost on broker restart). For anything you actually intend to inspect/replay later (which is the whole point of a DLQ), this should always be `PERSISTENT` — you don't want your failure evidence to vanish because RabbitMQ happened to restart.

### `max-attempts: 1` — **the binder-level retry count, and why it's intentionally different from the `3` above**
This is the property that causes the most confusion, so let's be very precise: **this `max-attempts` is a completely separate counter from the `max-attempts: 3` in section 3.**

- The `spring.cloud.stream.bindings.createPatient-in-0.consumer.max-attempts: 3` (binder-agnostic, Spring Retry) governs **how many times your Java function is invoked in-process** before giving up.
- The `spring.cloud.stream.rabbit.bindings.createPatient-in-0.consumer.max-attempts: 1` (RabbitMQ-specific) governs a **separate, lower-level RabbitMQ container retry mechanism** — essentially, "if message processing throws all the way out to the RabbitMQ listener container level (after Spring Retry has already exhausted its 3 attempts), how many additional times should the *container itself* attempt redelivery before giving up and nacking permanently?"

Setting this to `1` means: **don't let RabbitMQ's own container-level retry add yet another layer of retries on top of Spring Retry's 3 attempts.** If you left this at a higher default, you could end up with a confusing multiplicative retry situation (Spring Retry's 3 attempts, potentially repeated N times by the container-level mechanism = up to 3×N actual invocations), which is rarely what anyone intends and makes total attempt counts hard to reason about. By pinning the RabbitMQ-level count to `1`, you get a clean, singular, predictable total: **exactly 3 attempts, all governed by the one Spring Retry policy you can see and tune in one place**, with RabbitMQ's own container just handling the final ack/nack decision once. This is precisely why the comment in your original yml says `# rabbit-level attempts (leave at 1, let spring-retry above handle it)` — that's exactly correct, and a best practice worth keeping.

### `requeue-rejected: false`
Covered in file 02, section 7 — this is what prevents the redelivery-loop trap. When the final nack happens (after all Spring Retry attempts are exhausted, or immediately for an `AmqpRejectAndDontRequeueException`), this flag ensures RabbitMQ does **not** put the message back at the head of the *same* queue for another immediate delivery attempt. Combined with `auto-bind-dlq`, the message instead flows to the DLQ. Without this set to `false`, you'd risk an infinite tight retry loop on poison messages, completely bypassing the DLQ.

### `header-mapper: none`
Controls how RabbitMQ AMQP message properties/headers are mapped to/from Spring `Message<T>` headers. Setting this to `none` means the binder does **not** attempt to map RabbitMQ-native headers into Spring message headers (and vice versa) — you're working with the raw payload only (`byte[]`, per file 03), without inheriting a bunch of AMQP-specific header metadata into your Spring message context. This is a reasonable choice given you're manually parsing the Debezium envelope from the raw bytes yourself rather than relying on framework-level header-driven routing/content-negotiation.

### `content-type: application/json`
Declares the expected MIME type of the payload for framework bookkeeping/content-negotiation purposes. Since your function signature is `Consumer<byte[]>` (raw bytes) rather than `Consumer<CreatePatientDto>` (a typed object), Spring Cloud Stream's message-conversion machinery mostly steps out of the way here — you're doing the JSON parsing explicitly with `objectMapper.readValue` in the function body — but declaring the content type is still good practice for observability/tooling and for correctness if you ever add a Spring Cloud Stream-native `MessageConverter`-based path elsewhere in the app.

---

## 5. Putting It All Together — the Full Failure Journey of One Message

Walking through what happens end-to-end when a message causes a transient Mongo failure, tying every property above into the sequence:

1. RabbitMQ delivers the message from queue `CreatePatientWrite.patient-read-db-group` to your consumer (`acknowledge-mode: AUTO` means RabbitMQ is now waiting for an outcome, not yet removing the message).
2. Your `createPatient` lambda runs, `mongoPatientService.createPatient(patient)` throws (say, a Mongo timeout).
3. Spring Retry catches this, waits **1000ms**, retries (**attempt 2**).
4. Fails again → waits **2000ms** (1000 × 2.0 multiplier), retries (**attempt 3**, which is `max-attempts: 3`, so this is the last one).
5. Fails a third time → Spring Retry gives up, the exception propagates out of the function to the RabbitMQ listener container.
6. The RabbitMQ-binder-level `max-attempts: 1` means the container does **not** attempt yet another redelivery cycle — it immediately proceeds to the final failure path.
7. The container nacks the message with `requeue=false` (governed by `requeue-rejected: false`), so it does **not** go back into the main queue.
8. Because `auto-bind-dlq: true` and `republish-to-dlq: true`, instead of relying purely on RabbitMQ's native `x-dead-letter-exchange` mechanism, Spring Cloud Stream itself republishes the message to `CreatePatientWrite.patient-read-db-group.dlq`, attaching the exception/stack-trace headers, marked `PERSISTENT` so it survives a broker restart.
9. The message now sits safely in the DLQ, fully preserved, with rich failure diagnostics attached as headers — ready for manual inspection, alerting, or a future reprocessing tool to pick up.

Total elapsed time for this whole journey: roughly 1s + 2s = ~3 seconds of backoff (the `back-off-max-interval: 10000` cap never actually gets hit at only 3 attempts here — it would only matter if `max-attempts` were higher, e.g. 5–6 attempts, where 1s → 2s → 4s → 8s → capped at 10s would kick in).

---

## 6. What's Next

We've now fully covered the Spring Cloud Stream / RabbitMQ side end to end: concepts → RabbitMQ mechanics → functional code → yml configuration. In **05-debezium-fundamentals.md**, we shift to the *source* of these messages: what Debezium actually is, how change data capture (CDC) works at the database level, the transactional outbox pattern (which your `payload`-column double-deserialization in file 03 strongly suggested you're already using), and how Debezium turns MySQL binlog events into the envelope your consumer parses. After that, we'll get into your Debezium server docker-compose configuration, MySQL-specific CDC mechanics, and finally multi-outbox-table → multi-exchange routing.
