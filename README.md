# Apache Flink Prometheus Connector — **Flink 2.x port**

> **This repository targets Flink 2.2.1.** The 1.20 line is untouched in
> `8.9.1.PROMETHEUS_Sink_Connector`.
>
> This connector was expected to be "just a recompile" — it is Sink V2 at the provider
> level (4 x `SinkV2Provider`, 0 x `SinkFunction`), so nothing should have needed to
> change. It did. It extends `AsyncSinkWriter`, and that is where Flink 2 changed the
> contract.

## What the port actually changed — 3 files, no logic

| 1.20 | 2.2.1 | Where |
|:---|:---|:---|
| `Sink.InitContext` (**nested in `Sink`**) | `WriterInitContext` (**top-level**) | `PrometheusSinkWriter` (2 constructors), `PrometheusSink` (2 signatures) |
| `submitRequestEntries(List, Consumer<List<T>>)` | `submitRequestEntries(List, ResultHandler<T>)` | writer + HTTP callback |
| `requestResult.accept(Collections.emptyList())` | `requestResult.complete()` | `HttpResponseCallback` (`retryForEntries` / `completeExceptionally` also exist) |
| `StatefulSink` interface | gone — state is `SupportsWriterState.restoreWriter(WriterInitContext, Collection<WriterStateT>)`, already provided by `AsyncSinkBase` | no change needed |

### The trap worth remembering

`PrometheusSink` named `InitContext` and `StatefulSinkWriter` **unqualified** and
compiled on 1.20 only because nested types of an implemented interface are inherited
into scope — `AsyncSinkBase implements Sink`, so `Sink.InitContext` was in scope for
free. Remove the nesting and the same, untouched file fails with *cannot find symbol*.
Neither class had an import to fix, because neither ever needed one.

`AsyncSinkBase`'s own constructor already took an `AsyncSinkWriterConfiguration`, so
that part of the 2.x API was already satisfied — only the two types above moved.

## Build

```bash
docker run --rm -v "$PWD:/src" -w /src maven:3.9-eclipse-temurin-17 \
    mvn -q -B -DskipTests -Dmaven.test.skip=true clean package \
    -pl flink-sql-connector-prometheus -am
```

**`-DskipTests` alone is NOT enough.** It still *compiles* `src/test/java`, and those
contain `DataStreamExample` / `TableAPIExample` written against `SourceFunction` and
`org.apache.flink.api.java.utils` — both removed in Flink 2.0.

> **Known gap:** until those demos are ported to `DataGeneratorSource`, this
> repository's test suite cannot compile on 2.x at all. The artifact builds; the tests
> do not. Recorded rather than hidden, because a green artifact build can otherwise be
> mistaken for a green module.

## How it was verified

Reading the built jar, not trusting the build exit code:

```bash
javap -classpath lib/flink-sql-connector-prometheus-*.jar \
    org.apache.flink.connector.prometheus.sink.PrometheusSinkWriter
# must show org.apache.flink.api.connector.sink2.WriterInitContext
# and          org.apache.flink.connector.base.sink.writer.ResultHandler
```

Both types exist only in 2.x, so the jar cannot be a stale 1.20 build.

Then, on a running Flink 2.2.1 cluster: the factory **plans** (a bare `CREATE TABLE`
resolves nothing), a datagen INSERT writes 3 samples, and they are queryable back
through Thanos with the semicolon-separated label contract intact —
`iot_flink2_probe{region=north, siteId=probe, deviceId=probe-device, sensorId=probe-sensor}`.

---

# Upstream readme (unchanged)

# Apache Flink Prometheus Connector


> Cloned from Original Apache Project (flink-connector-prometheus), with some local mods.


This repository contains the official Apache Flink Prometheus connector.

* [More details](flink-connector-prometheus/README.md) about the connector and its usage.
* [Example application](flink-connector-prometheus/src/test/java/org/apache/flink/connector/prometheus/sink/examples/DataStreamExample.java)
  demonstrating the usage of the connector.

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Modules

This repository contains the following modules

* [Prometheus Connector](./flink-connector-prometheus): Flink Prometheus Connector implementation; supports optional
  request signer
* [Sample application](./flink-connector-prometheus/src/test/java/org/apache/flink/connector/prometheus/sink/examples/DataStreamExample.java):
  This class contains a complete application demonstrating set up and usage of the sink connector.
* [Amazon Managed Prometheus Request Signer](./flink-connector-prometheus-request-signer-amp): Implementation of request
  signer for Amazon Managed Prometheus (AMP)

## Building the Apache Flink Prometheus Connector from Source

Prerequisites:

* Unix-like environment (we use Linux, Mac OS X)
* Git
* Maven (we recommend version 3.8.6)
* Java 11

```bash
git clone https://github.com/georgelza/flink-connector-prometheus.git
cd flink-connector-prometheus
mvn clean package -DskipTests
```

The resulting jars can be found in the `target` directory of the respective module.


## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.

---
*End of Document*

---

## Building this connector

**It compiles to Java 17.** The Flink parent POM sets `target.java.version` to
`1.8` — Flink's own build baseline — but Flink 1.20 does not *run* on Java 8; it
runs on Java 11 or 17, and the image this connector is deployed into is
`flink:1.20.1-scala_2.12-java17`. This fork overrides the property to **17** in
`pom.xml` so the bytecode matches the JVM that loads it (verified:
`mvn help:evaluate -Dexpression=target.java.version` → `17`).

**Consequence, stated plainly: this jar will not load on a Java-11 Flink
cluster.** Revert the override to 11 if that ever becomes a requirement.

**Maven is pinned.** `./mvnw` (wrapper, Maven 3.9.9) is committed, so the build does
not depend on the host's Maven. It still needs a **JDK 17 on `PATH`** — this host
only has JDK 8, hence the container:

```bash
docker run --rm -v "$PWD:/src" -w /src maven:3.9-eclipse-temurin-17 \
    mvn -q -B -DskipTests clean package -pl flink-sql-connector-prometheus -am
```

`-pl … -am` builds only the SQL connector and what it needs, rather than every
module in the reactor.

### What it produces — ONE shaded jar

`flink-sql-connector-prometheus/target/flink-sql-connector-prometheus-<version>.jar`

That is the deliverable: `maven-shade-plugin` bundles the connector and its
transport dependencies into it. **Ignore `original-flink-sql-connector-prometheus-*.jar`**
in the same directory — that is the unshaded pre-shade artefact, and using it
gives a `NoClassDefFoundError` at runtime for the HTTP client.

### Using it from the IoTStream stack

```bash
FLINK_LIB=$HOME/Desktop/IoTStream/deploy/infrastructure/flink/lib
cp flink-sql-connector-prometheus/target/flink-sql-connector-prometheus-*.jar \
   "$FLINK_LIB"/
```

Then rebuild the Flink image from `deploy/infrastructure/flink` and bump its
`VERSION`. The DDL identifier is `connector='prometheus'`, writing to Prometheus'
remote-write receiver.

`target/` is build output and is git-ignored; do not commit the jars here.

### Two deliberate deviations from upstream

Both are documented where they happen, and both are the reason this tree cannot
simply be pushed back to `apache/flink-connector-prometheus`:

1. **Attribution header above the ASF licence** in every `*.java` — project, file,
   repo, original source. ASF convention puts the licence first, so
   `spotless:check` fails on all 77 sources; `spotless.check.skip` is set in
   `pom.xml` with the reason inline. Checkstyle stays enabled.
2. **The starting point is PR #22** (`darenwkt:FLINK-33139`, `c847a3a1`), which is
   open and unmerged, not apache `main`. Sync from that branch, not from `main`,
   or you will merge a base this tree never contained.
