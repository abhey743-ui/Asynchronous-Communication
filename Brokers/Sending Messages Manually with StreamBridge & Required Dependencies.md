# 08 — Sending Messages Manually with `StreamBridge` & Required Dependencies

> Builds on files 01–07. Two things we hadn't covered yet: how to **publish** a message to the broker imperatively (outside the declarative functional-binding model from file 03), and the exact Maven/Gradle dependencies needed to make everything in this series actually work.

---

## 1. Why You'd Ever Need `StreamBridge` at All

Everything in file 03 was about **`Consumer<byte[]>`** — a function Spring Cloud Stream wires up declaratively at startup, tied to a fixed binding (`createPatient-in-0`) defined in yml. That model works great when the trigger for sending/receiving a message is "a message arrived on this pre-configured input." But plenty of real scenarios don't fit that shape:

- A **REST controller** handles an incoming HTTP request and, as a side effect, needs to publish an event — there's no "input message" here, the trigger is an HTTP call.
- You need to publish to a **destination decided at runtime** (e.g., based on a tenant ID, a feature flag, or exactly the multi-exchange routing scenario from file 07) — not a name fixed in yml at startup.
- You're inside **existing imperative service code** (a `@Service` class, a scheduled job, an exception handler) and just need to "fire a message" without restructuring that code around the functional `Supplier<T>` model.

For all of these, Spring Cloud Stream provides `StreamBridge` — a Spring-managed bean you can `@Autowired`/constructor-inject anywhere in your application and call imperatively, at any time, from any context, to publish a message to a named destination.

---

## 2. Basic Usage

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class PatientEventPublisher {

    private final StreamBridge streamBridge;
    private final ObjectMapper objectMapper;

    public void publishPatientCreated(CreatePatientDto patient) {
        try {
            byte[] payload = objectMapper.writeValueAsBytes(patient);

            boolean sent = streamBridge.send("patientCreatedOut-out-0", payload);

            if (!sent) {
                log.error("StreamBridge failed to send patient-created event for patientId={}",
                        patient.getId());
                throw new MessagePublishException("Failed to publish patient-created event");
            }
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize patient event", e);
            throw new MessageSerializationException(e);
        }
    }
}
```

Key points:
- **`streamBridge.send(bindingName, payload)`** — the first argument is a **binding name**, exactly like `createPatient-in-0` was for a `Consumer`, except here it's an **output** binding, conventionally suffixed `-out-0`.
- **Return value is a `boolean`** — `true` if the broker accepted the send, `false` otherwise. Unlike the declarative model (where a thrown exception drives retry/DLQ automatically, per file 04), `StreamBridge` puts the responsibility for checking success and deciding what to do about failure **on you** — there's no automatic retry wrapper here unless you add one yourself (e.g., wrap the call with Spring Retry's `@Retryable`, or use `streamBridge.send(...)` inside a method annotated accordingly).
- You still need to configure this binding in yml, same as any other:

```yaml
spring:
  cloud:
    stream:
      bindings:
        patientCreatedOut-out-0:
          destination: PatientCreatedEvents
      rabbit:
        bindings:
          patientCreatedOut-out-0:
            producer:
              declare-exchange: true
              exchange-type: topic
```

Notice this is a **producer** binding config now (not `consumer`) — the RabbitMQ-specific producer properties (like `declare-exchange`, `exchange-type`) mirror the consumer-side properties from file 04, just for the publish path instead of the consume path.

---

## 3. Dynamic Destinations — Solving File 07's Routing Problem From the Application Side

`StreamBridge` also supports sending to a destination **not pre-declared in yml at all** — you can pass any destination name at call time, and the binder will provision it on the fly (using default binder settings) if it doesn't already have a binding configured for that exact name:

```java
public void publishToExchange(String exchangeName, byte[] payload) {
    streamBridge.send(exchangeName, payload);
}
```

This is directly relevant to file 07's multi-exchange discussion: if you ever needed your **Spring Boot application itself** (rather than Debezium) to be the one deciding which exchange an event goes to at runtime — e.g., a generic `EventPublishingService` that looks at an `aggregateType` field and calls `streamBridge.send(aggregateType, payload)` — this is exactly the mechanism that makes that possible, without needing a separate hardcoded binding per aggregate type in yml. It's the application-code equivalent of what the Debezium Event Router SMT does at the CDC layer.

> **Caveat worth knowing**: dynamically-created destinations use the binder's **default** producer settings (whatever `spring.cloud.stream.rabbit.default.producer.*` specifies, if you've set that), not any binding-specific customization — so if different exchanges genuinely need different settings (e.g., different exchange types), you're better off pre-declaring each as an explicit binding in yml rather than relying purely on dynamic destination names.

---

## 4. `StreamBridge` vs. the Declarative `Supplier<T>` Model — When to Use Which

| | `Supplier<T>` (declarative) | `StreamBridge` (imperative) |
|---|---|---|
| Trigger | Framework polls/schedules it automatically | You call it explicitly, whenever your code decides to |
| Best fit | Regular, framework-driven periodic emission | Event-driven, triggered by application logic (HTTP requests, business events, exception handlers) |
| Destination | Fixed at startup via yml | Fixed, or fully dynamic at call time |
| Failure handling | Framework-managed | Your responsibility (check the boolean, or wrap with your own retry) |

For your architecture specifically — where the *primary* event source is Debezium/CDC (files 05–07), not your application code directly — you likely won't need `StreamBridge` for the core patient-sync flow at all (Debezium is already doing the "publish on write" job via the outbox pattern, which is the whole point of avoiding the dual-write problem from file 05). `StreamBridge` becomes useful for **secondary, non-CDC-driven events** your app might want to emit directly — e.g., publishing an "read-model sync completed" notification after `mongoPatientService.createPatient(patient)` succeeds in your consumer, or emitting operational/audit events that don't originate from a database write at all.

---

## 5. Required Dependencies — the Full Picture

Here's everything needed to make the RabbitMQ + Spring Cloud Stream setup from files 01–04 (and `StreamBridge` from this file) actually work, explained by purpose rather than just pasted as a block.

### 5.1 The Spring Cloud BOM (Bill of Materials) — Manage Versions Centrally

Spring Cloud Stream's version needs to align with your Spring Boot version — mismatches are one of the most common sources of confusing startup failures in Spring Cloud projects. The standard way to avoid this is importing the **Spring Cloud BOM**, which pins compatible versions of every Spring Cloud module for you, so you never specify a Spring Cloud Stream version number directly.

**Maven** (in `<dependencyManagement>`):
```xml
<properties>
    <spring-cloud.version>2024.0.0</spring-cloud.version>
</properties>

<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-dependencies</artifactId>
            <version>${spring-cloud.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

**Gradle** (using the `io.spring.dependency-management` plugin):
```groovy
plugins {
    id 'io.spring.dependency-management' version '1.1.6'
}

dependencyManagement {
    imports {
        mavenBom "org.springframework.cloud:spring-cloud-dependencies:2024.0.0"
    }
}
```

> Check the [Spring Cloud release train compatibility matrix](https://spring.io/projects/spring-cloud#overview) for the exact BOM version matching your specific Spring Boot version — this pairing changes over time and getting it wrong is a very common source of dependency-resolution errors.

### 5.2 The RabbitMQ Binder — the Core Dependency

```xml
<dependency>
    <groupId>org.springframework.cloud</groupId>
    <artifactId>spring-cloud-stream-binder-rabbit</artifactId>
</dependency>
```
```groovy
implementation 'org.springframework.cloud:spring-cloud-stream-binder-rabbit'
```

This single dependency **transitively pulls in**:
- `spring-cloud-stream` — the core binding/functional-model framework (file 03's `Consumer`/`Function`/`Supplier` machinery, binding name resolution, etc.)
- `spring-boot-starter-amqp` — Spring's AMQP/RabbitMQ integration layer (`RabbitTemplate`, connection factory management, etc.)
- `spring-rabbit` and the underlying `amqp-client` (the actual RabbitMQ Java client library) — the low-level protocol implementation from file 02's connections/channels model.

You do **not** need to separately declare `spring-boot-starter-amqp` or `amqp-client` — they arrive transitively via the binder starter. Declaring them separately and explicitly is only necessary if you need a **newer/different version** than what the binder's transitive dependency brings in, which is uncommon.

### 5.3 `spring-cloud-function-context` — Usually Automatic, Worth Knowing About

The functional programming model (`Consumer<T>`/`Function<T,R>`/`Supplier<T>` bean discovery, `spring.cloud.function.definition`) is actually implemented by a separate module, **`spring-cloud-function-context`**, which `spring-cloud-stream` depends on transitively. You never need to declare this explicitly — but it's worth knowing it exists as a distinct module, because it's also usable **standalone** (without any messaging binder at all) for pure serverless/FaaS-style function composition — a good thing to know if you ever explore that side of the Spring Cloud ecosystem separately.

### 5.4 Retry Support — `spring-retry` and `spring-boot-starter-aop`

The `max-attempts`/backoff configuration from file 04, section 3 is implemented using **Spring Retry**, which itself relies on **Spring AOP** (to wrap your function invocation in a retryable proxy). For Spring Boot 3.x + recent Spring Cloud Stream versions, these are typically pulled in transitively via the binder starter as well — but if you ever see a runtime error related to retry configuration not being applied, or `@Retryable`/`RetryTemplate`-related `ClassNotFoundException`s, explicitly adding them resolves it:

```xml
<dependency>
    <groupId>org.springframework.retry</groupId>
    <artifactId>spring-retry</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-aop</artifactId>
</dependency>
```
```groovy
implementation 'org.springframework.retry:spring-retry'
implementation 'org.springframework.boot:spring-boot-starter-aop'
```

### 5.5 JSON Handling — Jackson

Your code uses `ObjectMapper` directly (file 03), and also references `tools.jackson.core.JacksonException` / `tools.jackson.databind.ObjectMapper` in your imports — worth flagging directly: **`tools.jackson.*` is the package namespace for Jackson 3.x**, which is a notably newer major version than the traditional `com.fasterxml.jackson.*` (Jackson 2.x) namespace most Spring Boot projects (including most Spring Boot 3.x versions as of this writing) still ship with by default via `spring-boot-starter-json`. Double-check which major Jackson version your Spring Boot BOM actually resolves — if your project is still on Jackson 2.x transitively (the far more common case), your imports should be `com.fasterxml.jackson.core.JacksonException` and `com.fasterxml.jackson.databind.ObjectMapper` instead, and the `tools.jackson.*` imports in your pasted code would fail to compile unless you've deliberately pulled in Jackson 3.x yourself. This is worth verifying directly against your `pom.xml`/`build.gradle`'s resolved dependency tree (`mvn dependency:tree | grep jackson` or Gradle's equivalent) rather than assuming — it's an easy thing to get bitten by since both namespaces compile fine in isolation but represent genuinely different major versions with API differences.

```xml
<!-- Usually already present transitively via spring-boot-starter-web / spring-boot-starter-json -->
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
</dependency>
```

### 5.6 Lombok — for `@AllArgsConstructor`, `@Slf4j`, etc.

```xml
<dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <optional>true</optional>
</dependency>
```
```groovy
compileOnly 'org.projectlombok:lombok'
annotationProcessor 'org.projectlombok:lombok'
```

Not part of the messaging stack at all, but present throughout your code (`@AllArgsConstructor`, `@Slf4j` in file 03) — included here for completeness of "everything needed to compile and run this exact codebase."

### 5.7 MongoDB — for `MongoPatientService`

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-mongodb</artifactId>
</dependency>
```
```groovy
implementation 'org.springframework.boot:spring-boot-starter-data-mongodb'
```

Needed for whatever `MongoPatientService`/`MongoRepository`-style code backs your read-side sync (file 03's `mongoPatientService.createPatient(patient)` call).

### 5.8 Full Consolidated List (Maven)

```xml
<dependencies>
    <!-- Core RabbitMQ binder — pulls in spring-cloud-stream, spring-boot-starter-amqp, amqp-client -->
    <dependency>
        <groupId>org.springframework.cloud</groupId>
        <artifactId>spring-cloud-stream-binder-rabbit</artifactId>
    </dependency>

    <!-- Retry support for max-attempts/backoff (file 04) -->
    <dependency>
        <groupId>org.springframework.retry</groupId>
        <artifactId>spring-retry</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-aop</artifactId>
    </dependency>

    <!-- MongoDB read-side persistence -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-mongodb</artifactId>
    </dependency>

    <!-- Lombok -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>

    <!-- Testing (recommended for testing Consumer<byte[]> beans per section 5 of file 03) -->
    <dependency>
        <groupId>org.springframework.cloud</groupId>
        <artifactId>spring-cloud-stream-test-binder</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

> **Note on `spring-cloud-stream-test-binder`**: this is worth calling out even though it wasn't asked for directly — it's the standard way to test declarative function bindings (like your `createPatient` `Consumer<byte[]>`) with an in-memory test binder instead of a real RabbitMQ instance, letting you assert on published/consumed messages in fast, broker-free unit/integration tests. Genuinely useful once this project grows beyond manual testing.

---

## 6. What's Next

This closes out the practical "how do I actually build and run this" side of the series. If you want to continue extending the documentation set, the two follow-ups mentioned at the end of file 07 are still open: **idempotent-consumer / Mongo-upsert patterns** (directly relevant given the at-least-once guarantees discussed throughout this series), and **security hardening** (scoped MySQL replication user, RabbitMQ vhosts/permissions, TLS for both the AMQP and MySQL connections). Let me know which you'd like next, or if there's another gap you've spotted.
