# 03 — Spring Cloud Stream: The Functional Model & Your Consumer, Explained Line by Line

> Builds on `01-message-broker-fundamentals.md` and `02-rabbitmq-deep-dive.md`. Here we move from raw RabbitMQ/AMQP concepts to how Spring Cloud Stream (SCS) wraps all of that into a clean, broker-agnostic programming model, then dissect your actual `CreatePatientInReadDb` class.

---

## 1. What Problem Does Spring Cloud Stream Actually Solve?

If you used raw `spring-amqp` / `amqp-client` directly, you'd have to:
- Manually declare exchanges, queues, and bindings in Java config
- Manually wire up `RabbitListener` annotations with queue names as string literals
- Manually configure retry/backoff via `RetryTemplate` beans
- Manually set up DLQ exchanges/queues/bindings
- Rewrite almost all of it if you ever swapped RabbitMQ for Kafka

Spring Cloud Stream introduces a **binder abstraction**: your business logic is written against a broker-agnostic model (`Consumer<T>`, `Function<T,R>`, `Supplier<T>`), and a pluggable **binder** (`spring-cloud-stream-binder-rabbit`, `spring-cloud-stream-binder-kafka`, etc.) translates that model into broker-specific operations at runtime, driven entirely by yml configuration.

This gives you:
- **Separation of concerns** — your Java code expresses *what* to do with a message; the yml expresses *how* it's delivered (retries, DLQ, concurrency, ack mode).
- **Broker portability** — in theory, swapping RabbitMQ for Kafka is a dependency + yml change, not a code rewrite (in practice there are broker-specific nuances, but the core function signature stays identical).
- **Testability** — you can unit test a `Consumer<byte[]>` as a plain Java function, no broker required.
- **Convention over configuration for infrastructure** — DLQs, retry, and binding provisioning become declarative yml instead of imperative boilerplate.

---

## 2. The Functional Programming Model

Spring Cloud Stream (since version 3.x, using `spring-cloud-function` under the hood) maps three functional interfaces directly onto messaging roles:

| Java type | Role | Meaning |
|---|---|---|
| `Supplier<T>` | Source / Producer | Produces messages, no input — polled or triggered periodically |
| `Function<T, R>` | Processor | Consumes a message, transforms it, and republishes the result |
| `Consumer<T>` | Sink | Consumes a message, does something with it (side effect), produces nothing back onto the stream |

Your `createPatient` bean is a `Consumer<byte[]>` — it's a pure **sink**: read the Debezium event, write to Mongo, done. No output is published back onto any stream.

### How does SCS know which bean maps to which binding?

This is the part that looks like "magic" until you see the wiring:

```yaml
spring:
  cloud:
    function:
      definition: createPatient
    stream:
      bindings:
        createPatient-in-0:
          destination: CreatePatientWrite
```

- `spring.cloud.function.definition: createPatient` tells Spring Cloud Function: "expose the bean named `createPatient` as an active function."
- Spring Cloud Stream then auto-generates a **binding name** from that function name using the convention **`<function-name>-in-<index>`** for inputs and **`<function-name>-out-<index>`** for outputs. Since `createPatient` is a `Consumer` (input only, no output), only `createPatient-in-0` exists.
- The `destination` under that binding name is what actually gets mapped to a RabbitMQ queue/exchange — this is the bridge between "abstract function input" and "concrete broker destination."

This naming convention is not optional or cosmetic — if your bean were named differently, or if you had multiple functions composed together (e.g. `createPatient|enrichPatient`), the binding names would change accordingly. Getting this convention right is the single most common source of "my consumer never receives anything" bugs in Spring Cloud Stream.

---

## 3. Your Code, Explained Section by Section

Here's your class again for reference, then a full breakdown:

```java
@Configuration
@AllArgsConstructor
@Slf4j
public class CreatePatientInReadDb {
    private PatientServiceInterface patientServiceInterface;
    private ObjectMapper objectMapper;
    private MongoPatientService mongoPatientService;

    @Bean
    public Consumer<byte[]> createPatient() {
        return data -> {
            DebeziumEnvelope envelope = objectMapper.readValue(data, DebeziumEnvelope.class);
            if (envelope.getPayload() == null || envelope.getPayload().getAfter() == null) {
                log.info("Skipping Debezium maintenance/delete snapshot message.");
                return;
            }
            String patientJson = String.valueOf(envelope.getPayload().getAfter().getPayload());
            CreatePatientDto patient = objectMapper.readValue(patientJson, CreatePatientDto.class);
            log.debug(patient.toString());
            log.debug("Consumer received the message!");
            try {
                mongoPatientService.createPatient(patient);
                log.info("Patient successfully synced to read DB");
            } catch (JacksonException e) {
                log.error("Failed to deserialize Debezium envelope: {}", data, e);
                throw new AmqpRejectAndDontRequeueException("Invalid message payload", e);
            } catch (Exception e) {
                log.error("Failed to process patient creation event, will retry", e);
                throw e;
            }
        };
    }
}
```

### `@Configuration` + `@Bean`
Spring Cloud Function discovers functional beans (`Supplier`/`Function`/`Consumer`) by scanning the application context for beans of those types, matched by name against `spring.cloud.function.definition`. Declaring it as a `@Bean` inside a `@Configuration` class (rather than, say, a `@Component` implementing `Consumer`) is the idiomatic way to do this — it lets you construct the lambda with full access to injected dependencies via the enclosing method, which is exactly what you're doing with `patientServiceInterface`, `objectMapper`, and `mongoPatientService`.

### `@AllArgsConstructor` (Lombok)
Generates a constructor taking all three fields, which Spring uses for **constructor injection** — the recommended DI style over field injection (`@Autowired` on fields), since it makes dependencies explicit, immutable (`final` is even better here — worth adding), and easily testable without needing a Spring context.

> **Suggestion:** mark the three fields `private final` — with constructor injection you get true immutability, and it also lets Lombok's `@RequiredArgsConstructor` work identically to `@AllArgsConstructor` here while being more idiomatic for DI (it signals "these are required dependencies," not "these are all fields regardless of mutability intent").

### `Consumer<byte[]> createPatient()`
The input type is `byte[]` — the **raw bytes off the wire**, before any framework-level deserialization. This is a deliberate and important choice: it means *you* control deserialization inside the function body (via `objectMapper.readValue`), rather than relying on SCS's built-in content-type-based message conversion. This gives you full control over error handling for malformed payloads — which is exactly what the `catch (JacksonException e)` block is for.

### The Debezium envelope unwrap
```java
DebeziumEnvelope envelope = objectMapper.readValue(data, DebeziumEnvelope.class);
if (envelope.getPayload() == null || envelope.getPayload().getAfter() == null) {
    log.info("Skipping Debezium maintenance/delete snapshot message.");
    return;
}
```
Debezium doesn't just publish "the new row" — it publishes a structured **change event envelope** containing (among other things) `before`, `after`, `source`, and `op` (the operation type: `c`=create, `u`=update, `d`=delete, `r`=snapshot read). A `null` `after` typically means a delete event or a tombstone/heartbeat-style message rather than a genuine row creation. Explicitly guarding for this and returning early (rather than letting a `NullPointerException` propagate) is correct defensive design — we'll go much deeper into the exact shape of this envelope, and how it differs for the outbox pattern specifically, in the Debezium file.

### Double deserialization
```java
String patientJson = String.valueOf(envelope.getPayload().getAfter().getPayload());
CreatePatientDto patient = objectMapper.readValue(patientJson, CreatePatientDto.class);
```
This two-step deserialize (envelope → string → DTO) strongly suggests you're using the **outbox pattern**, where the `after` row itself contains a `payload` **column** holding a pre-serialized JSON string of the actual domain event — rather than Debezium capturing arbitrary business columns directly. This is a very deliberate and good pattern (we'll formalize exactly why in the Debezium/outbox file), but one thing worth flagging now: `String.valueOf(...)` on an already-deserialized object is a slightly unusual way to get the string — if `getPayload()` here returns a `JsonNode`/`Object` rather than a `String` directly, `String.valueOf` will call `.toString()` on it, which usually works for Jackson `JsonNode` types but is a bit fragile compared to explicitly typing that field as `String` in `DebeziumEnvelope`'s DTO and skipping the extra `valueOf` step.

### The two-tier exception handling — this is the important part
```java
} catch (JacksonException e) {
    log.error("Failed to deserialize Debezium envelope: {}", data, e);
    throw new AmqpRejectAndDontRequeueException("Invalid message payload", e);
} catch (Exception e) {
    log.error("Failed to process patient creation event, will retry", e);
    throw e;
}
```
This is the crux of correct retry/DLQ design, and it directly reflects the delivery-guarantee concepts from file 01 and the redelivery-loop trap from file 02:

- **`JacksonException` (malformed/unparseable payload)** — this is a **poison message**. No amount of retrying will ever make bad JSON become good JSON. Retrying it would be pure waste (and would eventually dead-letter it anyway after burning through retry attempts). So instead, you explicitly throw `AmqpRejectAndDontRequeueException`, which is Spring AMQP's signal to **immediately reject without requeue** — skipping the retry loop entirely and sending it straight to the DLQ (via the `auto-bind-dlq`/dead-letter-exchange mechanism from file 02).
- **Any other `Exception` (e.g., Mongo temporarily unreachable)** — this is a plausibly **transient** failure. Simply re-throwing lets Spring Retry's `max-attempts`/backoff configuration (from your yml, covered fully in file 04) kick in and retry with exponential backoff before eventually giving up and dead-lettering it too.

This is a textbook implementation of the "retryable vs. non-retryable error" distinction — one of the most important patterns in resilient consumer design, and something a lot of teams get wrong by either retrying everything blindly (wasting time on poison messages) or retrying nothing (turning transient blips into data loss/manual intervention).

> **Small enhancement worth considering:** currently a `MongoException`-style transient DB error and, say, a `NullPointerException` from a genuine bug in your own mapping code both fall into the same generic `catch (Exception e)` bucket and get retried identically. As this evolves, you may want a third tier — a narrower catch for *known-transient* infra exceptions (Mongo timeouts, connection errors) that retries, versus a catch for unexpected `RuntimeException`s that might indicate a real bug and arguably should dead-letter immediately too (retrying a bug 3 times just delays the DLQ by a few seconds and adds noise). Not urgent, but worth flagging as a refinement for later.

### Logging
Your `log.debug` for the parsed patient and the "Consumer received the message" line are fine for local development but will be very noisy (and potentially expose PII — patient data — in logs) if `debug` level is accidentally enabled in a shared/production environment. Given this is patient/health data, it's worth flagging now (not urgent for a learning project, but a good habit): avoid logging full DTOs containing PII even at debug level in real systems — log identifiers (e.g., patient ID) instead of the whole object.

---

## 4. How the `AUTO` Acknowledge Mode Ties Directly Into This Code

Recall from your yml:
```yaml
consumer:
  acknowledge-mode: AUTO
```

Here's precisely what "AUTO" means in the context of the function you just wrote:

- If your lambda **returns normally** (no exception thrown) → the binder automatically sends `basic.ack` to RabbitMQ. Message is permanently removed from the queue.
- If your lambda **throws** → the binder automatically triggers the **retry mechanism** (Spring Retry, governed by `max-attempts`/backoff in yml) for a normal exception, or immediately `basic.nack`s with `requeue=false` for exceptions like `AmqpRejectAndDontRequeueException` — which, combined with `auto-bind-dlq`, routes it to the DLQ.

In other words: **you never call `channel.basicAck()` or `channel.basicNack()` anywhere in your code** — and that's by design. The entire ack/nack/retry/DLQ decision tree is driven purely by *whether your function throws, and what it throws*. This is precisely the value proposition from section 1: your Java code expresses business logic and error classification; the binder + yml express delivery mechanics.

---

## 5. Benefits of This Approach, Summarized

1. **Declarative resilience** — retry counts, backoff, and DLQ routing live in yml, not scattered across `try/catch`/`RetryTemplate` boilerplate.
2. **Clear separation between "what failed" and "how to react"** — your code just needs to correctly classify errors (retryable vs. not); the framework handles the mechanics.
3. **Testability** — `createPatient()` is a plain function; you can unit test it with a mock `MongoPatientService` and assorted byte payloads with zero RabbitMQ involved.
4. **Broker portability at the code level** — if you ever moved this same function to a Kafka binder, the Java code above would not need to change at all; only the yml `spring.cloud.stream.kafka.*` section would replace the `spring.cloud.stream.rabbit.*` section.
5. **Consistent semantics across the team** — every consumer in the codebase follows the same "throw AmqpRejectAndDontRequeueException for poison messages, throw normally for transient ones" convention, making the system predictable to reason about at scale.

---

## 6. What's Next

In **04-yml-deep-dive.md**, we take your full yml, property by property, and connect each one back to the exact RabbitMQ mechanism it configures (from file 02) and the exact behavior it produces in your Java consumer (from this file) — including precisely how `max-attempts: 3` (Spring Retry) and `max-attempts: 1` (RabbitMQ binder-level) interact without conflicting, why `group` is mandatory for DLQ naming, and what `republish-to-dlq` actually changes about the message that lands in your DLQ.
