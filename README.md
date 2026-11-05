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
