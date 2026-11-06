---
type: concept
title: Compatibility and distribution
description: The two release lines, which Spark, Scala, Java and Databricks runtime each supports, and how the library is published and installed.
status: draft
stale_after: 2027-09-30T00:00:00Z
tags:
  - connector
  - spark
  - compatibility
  - release
sources:
  - id: docs-overview
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Overview.htm
    title: Connector for Apache Spark - Overview (self-managed)
  - id: docs-migration
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Migration-Guide.htm
    title: Connector for Apache Spark - Migrating to the 4.x line
  - id: cf-getting-started
    resource: https://sol-jira.atlassian.net/wiki/spaces/EventInteg/pages/6628999206
    title: "Getting Started Guide: Solace PubSub+ Connector for Apache Spark"
  - id: cf-ebp2255
    resource: https://sol-jira.atlassian.net/wiki/spaces/PLMT/pages/6893764626
    title: "[KA] EBP-2255 - Remove Java 8 and 11 runtime support across JCSMP and JMS/JAKARTA"
  - id: jira-142122
    resource: https://sol-jira.atlassian.net/browse/DATAGO-142122
    title: "DATAGO-142122: Update to JDK 11 and modernize the 3.x build"
  - id: jira-142067
    resource: https://sol-jira.atlassian.net/browse/DATAGO-142067
    title: "DATAGO-142067: Upgrade to Spark 4.0.0 (Java 17, Scala 2.13)"
  - id: jira-130732
    resource: https://sol-jira.atlassian.net/browse/DATAGO-130732
    title: "DATAGO-130732: Productize the Spark Connector (epic)"
  - id: jira-86131
    resource: https://sol-jira.atlassian.net/browse/DATAGO-86131
    title: "DATAGO-86131: Change Scala version to 2.12"
  - id: jira-151933
    resource: https://sol-jira.atlassian.net/browse/DATAGO-151933
    title: "DATAGO-151933: pom.xml declares the wrong license"
  - id: user-guide
    resource: https://github.com/SolaceProducts/pubsubplus-connector-spark/blob/master/src/docs/asciidoc/User-Guide.adoc
    title: Solace Spark Connector User Guide (repository)
  - id: maven-central
    resource: https://repo1.maven.org/maven2/com/solacecoe/connectors/pubsubplus-connector-spark/maven-metadata.xml
    title: Maven Central metadata for com.solacecoe.connectors:pubsubplus-connector-spark
  - id: spark-4-release
    resource: https://spark.apache.org/releases/spark-release-4-0-0.html
    title: Apache Spark 4.0.0 release notes
  - id: databricks-libraries
    resource: https://docs.databricks.com/aws/en/libraries/cluster-libraries
    title: Databricks - Compute-scoped libraries
---

# Compatibility and distribution

Two release lines are maintained in parallel. They have the same options and the same read and write behaviour. They differ only in the Spark, Scala and Java platform they are built for.[^cf-getting-started]

## Release lines

| Line | Latest release | Apache Spark | Scala | Java (minimum) | Databricks Runtime | Status |
|---|---|---|---|---|---|---|
| 4.x | 4.0.0 (2026-08-07) | 4.0.x | 2.13 | 17 | 17.3 LTS | Recommended for new deployments |
| 3.x | 3.2.0 (2026-08-04) | 3.5.x | 2.12 | 11 | 15.4 LTS, 16.4 LTS | Maintenance, supported until about September 2027 |

Sources: docs.solace.com, the Getting Started guide and the tickets.[^docs-overview][^cf-getting-started][^jira-130732]

- **4.x exists because Spark 4.0 forces three upgrades at once.** Spark 4.0 dropped Java 8 and 11 and dropped Scala 2.12 ([SPARK-45315], [SPARK-45314]).[^spark-4-release] The 4.0.0 release made exactly those changes and, by its acceptance criteria, added no new read or write capabilities.[^jira-142067]
- **3.x stays on Spark 3.5 and Scala 2.12 on purpose.** The 3.2.0 release moved the runtime from JDK 8 to JDK 11 and refreshed dependencies, with "no Spark/Scala major-version change and no new capabilities".[^jira-142122]
- **A 4.x build cannot load on Spark 3.5, and a 3.x build cannot load on Spark 4.** Spark, Scala and Java move together. The error is usually a `NoSuchMethodError` or `ClassNotFoundException` at startup.[^cf-getting-started]
- **Tested Databricks runtimes for 3.x** are 15.4 LTS (Spark 3.5.0, Scala 2.12, with Photon disabled) and 16.4 LTS (Spark 3.5.2, Scala 2.12). It is also validated on vanilla Apache Spark.[^user-guide]

Upgrading from 3.x to 4.x is a platform change, not a configuration change. The Spark 4 defaults still affect pipelines: ANSI SQL mode is on, so invalid casts and overflow raise errors instead of returning null. `spark.sql.ansi.enabled=false` restores the old behaviour. See the [migration guide](https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Migration-Guide.htm).[^docs-migration]

## Distribution

- **Maven Central**, coordinate `com.solacecoe.connectors:pubsubplus-connector-spark`. Both lines publish under this coordinate; 4.0.0 is the latest version there.[^maven-central] A new group and artifact ID is planned, but has not been made yet.[^jira-130732]
- **Packaging**: one shaded jar per release, with dependencies included as they are, not relocated. On 4.x, `spark-sql`, `spark-streaming` and the SLF4J API are excluded and must come from the Spark runtime. `spark-core` is still bundled, and with it the Scala library and the Hadoop client.
- **Branches**: 3.x is released from `master`; 4.x is released from the `spark_4.X` branch.
- **Databricks**: install it as a compute-scoped library from Maven. Standard (shared) access mode also needs the coordinate on the Unity Catalog allowlist. See [Running on Databricks](running-on-databricks.md).
- **Vanilla Spark**: `--packages` or `--jars` with `spark-submit`, or any other way the platform loads Spark libraries.
- **License**: Solace Community License, Version 1.0, as in the repository's `LICENSE` file. The POM declares a different, commercial license on every tag up to 4.0.0. That is a known defect, with the fix planned for the next patch on each line.[^jira-151933]
- **Support**: best effort through the [Solace Developer Community](https://solace.community/), with premium options through Solace.

## Version history that still matters

| Version | Change |
|---|---|
| 2.0.3 | Scala moved from 2.13 back to 2.12, because Databricks clusters are Scala 2.12 and the 2.13 build failed to load there.[^jira-86131] |
| 3.0.0 | The sink (Spark to Solace) and message replay were added. |
| 3.1.0 | Solace consumers moved from the driver to the executors. |
| 3.1.4 | The LVQ is read with a queue browser instead of a consumer. LVQ permission problems stop the stream at startup instead of being logged. |
| 3.1.6 | Partition-key checkpointing was disabled; partitioned queues are unsupported. |
| 3.1.7 | Unity Catalog Volume checkpoints, publish-ack timeout options, and `ignoreCheckpointMessageIdComparisonError`. |
| 3.2.0 / 4.0.0 | JDK 11 baseline for 3.x; Spark 4.0, Scala 2.13 and Java 17 for 4.x. |

The reasons are in [Regression guards](regression-guards.md).

## Competency questions

After reading this concept, you should be able to answer these. The short answers point to the detail above.

- **A customer runs Databricks 17.3 LTS. Which line do they need?** 4.x. DBR 17.3 LTS is Spark 4.0 with Scala 2.13.
- **A customer runs Databricks 16.4 LTS. Which line do they need?** 3.x. DBR 16.4 LTS is Spark 3.5.2 with Scala 2.12.
- **Did any option names change between 3.x and 4.x?** No. Both lines have the same options and read and write behaviour.
- **How long is 3.x supported?** Until about September 2027.
- **What is the Maven coordinate?** `com.solacecoe.connectors:pubsubplus-connector-spark`, for both lines. A new coordinate is planned but not yet in use.
- **Does the 4.x jar bundle Spark?** Partly. It leaves out `spark-sql` and `spark-streaming`, but still bundles `spark-core`, the Scala library and the Hadoop client.
- **Why is 3.x on Scala 2.12 and not 2.13?** Databricks Spark 3.5 runtimes are Scala 2.12, and a 2.13 build fails to load there (fixed in 2.0.3).

## The non-obvious part

**Some documents say the 4.x jar no longer bundles Spark, Scala or Hadoop, and that is not what the build does.** Only `spark-sql`, `spark-streaming` and the SLF4J API are left out of the 4.0.0 jar. `spark-core` and its Scala and Hadoop dependencies are still inside. Treat the build configuration as the truth when diagnosing classpath conflicts.

**Remove the old version before installing a new one.** If two versions of the library are on a Databricks cluster's classpath, classes conflict at startup. An uninstalled library is removed only when the cluster restarts.[^databricks-libraries] So uninstall, restart, then install.

**Upgrading the Solace Java API is not free on 3.x.** Both lines pin JCSMP 10.28.3. JCSMP 10.30.2 is the last Java 8 release, and 10.31.0 is the first that needs Java 17 at runtime. Moving 3.x past 10.30.x would therefore force Java 17 on a line whose point is to stay on Java 11.[^cf-ebp2255]

[^docs-overview]: Connector for Apache Spark - Overview (self-managed)
[^docs-migration]: Connector for Apache Spark - Migrating to the 4.x line
[^cf-getting-started]: Getting Started Guide: Solace PubSub+ Connector for Apache Spark
[^cf-ebp2255]: [KA] EBP-2255 - Remove Java 8 and 11 runtime support across JCSMP and JMS/JAKARTA
[^jira-142122]: DATAGO-142122: Update to JDK 11 and modernize the 3.x build
[^jira-142067]: DATAGO-142067: Upgrade to Spark 4.0.0 (Java 17, Scala 2.13)
[^jira-130732]: DATAGO-130732: Productize the Spark Connector (epic)
[^jira-86131]: DATAGO-86131: Change Scala version to 2.12
[^jira-151933]: DATAGO-151933: pom.xml declares the wrong license
[^user-guide]: Solace Spark Connector User Guide (repository)
[^maven-central]: Maven Central metadata for com.solacecoe.connectors:pubsubplus-connector-spark
[^spark-4-release]: Apache Spark 4.0.0 release notes
[^databricks-libraries]: Databricks - Compute-scoped libraries

[SPARK-45315]: https://issues.apache.org/jira/browse/SPARK-45315
[SPARK-45314]: https://issues.apache.org/jira/browse/SPARK-45314
