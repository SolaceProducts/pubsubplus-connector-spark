---
type: concept
title: Testing
description: How the connector is tested - integration suites against a real broker and Spark in containers, the local validation suite, what runs in CI, and what needs a real Databricks workspace.
status: draft
tags:
  - connector
  - spark
  - testing
sources:
  - id: readme
    resource: https://github.com/SolaceProducts/pubsubplus-connector-spark/blob/master/README.md
    title: pubsubplus-connector-spark README
  - id: jira-142122
    resource: https://sol-jira.atlassian.net/browse/DATAGO-142122
    title: "DATAGO-142122: Update to JDK 11 and modernize the 3.x build"
  - id: jira-130808
    resource: https://sol-jira.atlassian.net/browse/DATAGO-130808
    title: "DATAGO-130808: Integration tests refactoring"
  - id: jira-130445
    resource: https://sol-jira.atlassian.net/browse/DATAGO-130445
    title: "DATAGO-130445: Unity Catalog Volume as checkpoint location"
---

# Testing

There are no unit tests. All tests are integration tests against a real Solace software broker in a Docker container. Most also run Spark in containers; the validation suite runs Spark in the test JVM.[^jira-130808]

## Suites

| Suite | What it proves | Runs against |
|---|---|---|
| Source | Reading, batching, multiple consumers, checkpoint and LVQ behaviour, restart and deduplication, non-comparable message IDs | Broker and Spark containers |
| Sink | Publishing, topic from column or option, headers, publish-ack timeout, ACL denial, `foreachBatch` | Broker and Spark containers |
| Message replay | Each replay strategy | Broker container with a replay log |
| OAuth | Client credentials and token file, token refresh | Broker plus a Keycloak container as the authorization server |
| TLS | Client certificate, and username with or without password over TLS | Broker container with TLS |
| Validation | About 50 checks on option validation and broker-side errors (missing options, bad credentials, LVQ permission and ACL failures) | A local Spark session in the test JVM, plus a broker container[^jira-142122] |

The integration tests are run as one suite in CI (`mvn verify` with the integration-test suite). Building without tests is `mvn clean install -DskipTests`.[^readme]

## Running locally

- Docker must be running for every integration test.
- The build pins the Docker API version the test containers library uses, because recent Docker engines reject the older default it negotiates.
- On JDK 9 and later, the tests add the `--add-opens` flags Spark needs; a build profile does this automatically.
- **Windows:** the validation suite runs Spark in the test JVM, and Spark's checkpointing on Windows needs Hadoop's native helpers. Set `HADOOP_HOME` to a folder whose `bin` holds `winutils.exe` and `hadoop.dll` for Hadoop 3.3.x. Without it, the suite fails with "HADOOP_HOME and hadoop.home.dir are unset". Linux and CI need nothing extra.[^readme]

## What the tests do not cover

- **Databricks.** Unity Catalog Volume checkpoints, access modes and secret scopes need a Databricks workspace, so the container suites do not exercise them.[^jira-130445]
- **Long-running stability.** Multi-day runs, memory growth on the driver, and network blips are not covered. See [Performance and tuning](performance-and-tuning.md).
- **Performance.** Throughput numbers come from manual benchmarks, not from CI.

## Competency questions

After reading this concept, you should be able to answer these. The short answers point to the detail above.

- **Do the tests use a real broker?** Yes, the Solace software broker in a Docker container. Most suites also run Spark in containers.
- **Why does the validation suite fail on Windows with "HADOOP_HOME and hadoop.home.dir are unset"?** It runs Spark in the test JVM, and Spark needs Hadoop's Windows helpers. Set `HADOOP_HOME` to a folder with `bin\winutils.exe` and `hadoop.dll`.
- **Is Unity Catalog Volume checkpointing covered by the integration suites?** No. It needs a Databricks workspace, which the container suites do not have.
- **How is OAuth tested?** Against a Keycloak container acting as the authorization server.
- **What must be running before the integration tests start?** Docker, because every suite starts the broker (and most start Spark) in containers.

## The non-obvious part

**A test for the sink's `TimeStamp` column as a Spark timestamp is disabled.** The enabled test sends the timestamp as epoch milliseconds in a long column. Treat timestamp mapping on the sink as untested for Spark `TimestampType`.

**The broker image is pinned.** The containers use Solace software broker 10.11. A behaviour that depends on a newer broker feature passes or fails based on that pin, not on what customers run.

[^readme]: pubsubplus-connector-spark README
[^jira-142122]: DATAGO-142122: Update to JDK 11 and modernize the 3.x build
[^jira-130808]: DATAGO-130808: Integration tests refactoring
[^jira-130445]: DATAGO-130445: Unity Catalog Volume as checkpoint location
