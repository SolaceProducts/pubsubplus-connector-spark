---
type: concept
title: Overview
description: What the Spark Connector is, which directions it supports, where it runs, and what this spoke covers.
status: draft
tags:
  - connector
  - spark
  - connectors
  - overview
sources:
  - id: docs-overview
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Overview.htm
    title: Connector for Apache Spark - Overview (self-managed)
  - id: cf-getting-started
    resource: https://sol-jira.atlassian.net/wiki/spaces/EventInteg/pages/6628999206
    title: "Getting Started Guide: Solace PubSub+ Connector for Apache Spark"
  - id: cf-intro
    resource: https://sol-jira.atlassian.net/wiki/spaces/TCoE/pages/4632608848
    title: Solace Spark MI Introduction
  - id: cf-design-study
    resource: https://sol-jira.atlassian.net/wiki/spaces/CPD/pages/1531413196
    title: Databricks/Spark Connector (early design study)
  - id: jira-97452
    resource: https://sol-jira.atlassian.net/browse/DATAGO-97452
    title: "DATAGO-97452: Move Solace connection to Spark partitions"
  - id: jira-91049
    resource: https://sol-jira.atlassian.net/browse/DATAGO-91049
    title: "DATAGO-91049: Solace data source write support"
  - id: jira-143351
    resource: https://sol-jira.atlassian.net/browse/DATAGO-143351
    title: "DATAGO-143351: Real-Time / Continuous read mode (epic)"
---

# Overview

The Spark Connector is a connector, or adapter, that Spark jobs use to consume data from Solace and publish data to Solace. It is a library, not a deployable service: it runs inside the customer's own Spark application, on their own cluster.[^docs-overview] It was built by the TechCoE team and later taken over by product engineering.

## What it is

- A native Spark **DataSource V2** implementation, registered under the short name `solace`. Users write `spark.readStream.format("solace")` and `df.writeStream.format("solace")`.[^cf-getting-started]
- Built on the Solace Java API (JCSMP). Any JCSMP session property can be passed through with the `solace.apiProperties.` prefix. See [Configuration](configuration.md).
- Published to Maven Central and installed as a Spark library, most often as a Databricks cluster library. See [Compatibility and distribution](compatibility-and-distribution.md).
- Self-managed only. Solace does not host or run it.

## Directions

"Source" and "sink" are relative to Spark.[^docs-overview]

| Direction | Spark API | Solace side | Since |
|---|---|---|---|
| Source (Solace to Spark) | `readStream` (micro-batch streaming) | Consumes from an exclusive or non-exclusive queue | 1.0.0 |
| Sink (Spark to Solace) | `writeStream`, or `write` inside `foreachBatch` | Publishes persistent messages to topics | 3.0.0[^jira-91049] |

Not supported:[^docs-overview]

- **Batch reads** (`spark.read.format("solace")`). The source is streaming only.
- **Continuous or real-time read mode.** Only micro-batch reading exists; continuous mode is an open epic.[^jira-143351]
- **Partitioned queues** as a source. See [Read path](read-path.md).
- **Exactly-once delivery.** Delivery is at least once. See [Checkpointing and acknowledgment](checkpointing-and-acknowledgment.md).
- **Creating the source queue.** The queue must exist. The connector only creates its own checkpoint queue (the LVQ).

## How it works, in one paragraph

On each Spark trigger, the driver checks that the queue has messages and plans one Spark task per configured partition. Each task runs on an executor and opens its own Solace session and flow on the queue. This placement has applied since 3.1.0, when the Solace connection moved from the driver to the executors "for better utilization of resources and improvement in processing of data".[^jira-97452] Each task pulls up to `batchSize` messages and turns them into rows. When Spark commits the batch, the connector records the last processed message IDs in a Solace last value queue (LVQ) as well as in the Spark checkpoint. It acknowledges the messages to the broker when the next batch starts.[^cf-intro]

## Where it runs

It runs on any Spark platform with the matching Spark and Scala versions: vanilla Apache Spark, Databricks, and other managed Spark services. Databricks is the main target and the one with platform-specific setup (access modes, secret scopes, Unity Catalog Volume checkpoints). See [Running on Databricks](running-on-databricks.md).[^cf-getting-started]

## What this spoke does not cover

- Step-by-step customer setup. That is on [docs.solace.com](https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Overview.htm).
- Spark Structured Streaming itself (triggers, output modes, sink semantics). See the [Spark Structured Streaming guide](https://spark.apache.org/docs/latest/streaming/getting-started.html).
- Solace broker configuration beyond what this connector needs (queues, ACLs, client profiles, replay log). See the Solace broker documentation.

## Competency questions

After reading this concept, you should be able to answer these. The short answers point to the detail above.

- **Is this a micro-integration?** No. It is a connector library used inside Spark jobs, self-managed only, even though some pages file it under Micro-Integrations.
- **Can it both read from and write to Solace?** Yes. It reads from queues with `readStream` and publishes to topics with `writeStream` or `foreachBatch`.
- **Can a user run `spark.read.format("solace")` as a batch job?** No. The source supports micro-batch streaming only.
- **Where does the Solace connection run, on the driver or the executors?** Consumers run on the executors (since 3.1.0). The driver keeps one session for queue checks and the LVQ.
- **Does it create the queue it consumes from?** No. It creates only its checkpoint LVQ, and only when the client profile allows it.

## The non-obvious part

**It is a connector, not a micro-integration.** Some internal pages call it the "Solace Spark MI", and docs.solace.com lists it under Micro-Integrations. It is still a library used inside Spark jobs, with none of the micro-integration runtime, deployment, or Solace Cloud management. Its product name is the "Solace PubSub+ Connector for Apache Spark".

**Solace does not fit Spark's source contract naturally, and the design works around it.** Spark expects a source that can replay an exact offset range on request. A Solace queue cannot do that: it cannot bind a consumer to a partition, redeliver deterministically, or bulk-acknowledge up to a message ID. An early design study recorded these gaps.[^cf-design-study] The shipped design therefore uses a counter as Spark's offset, holds acknowledgments until Spark has committed, and keeps its own record of processed message IDs in an LVQ. That design is why delivery is at least once and why the LVQ matters on restart.

[^docs-overview]: Connector for Apache Spark - Overview (self-managed)
[^cf-getting-started]: Getting Started Guide: Solace PubSub+ Connector for Apache Spark
[^cf-intro]: Solace Spark MI Introduction
[^cf-design-study]: Databricks/Spark Connector (early design study)
[^jira-97452]: DATAGO-97452: Move Solace connection to Spark partitions
[^jira-91049]: DATAGO-91049: Solace data source write support
[^jira-143351]: DATAGO-143351: Real-Time / Continuous read mode (epic)
