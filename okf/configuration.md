---
type: concept
title: Configuration
description: The options the source and sink accept, their defaults and validation, and what the connector sets on the Solace session regardless of options.
status: draft
tags:
  - connector
  - spark
  - configuration
sources:
  - id: docs-config
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Configuring-Connection-Details.htm
    title: Connector for Apache Spark - Configuring connection details
  - id: docs-troubleshooting
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Troubleshooting.htm
    title: Connector for Apache Spark - Troubleshooting
  - id: cf-getting-started
    resource: https://sol-jira.atlassian.net/wiki/spaces/EventInteg/pages/6628999206
    title: "Getting Started Guide: Solace PubSub+ Connector for Apache Spark"
  - id: jira-97452
    resource: https://sol-jira.atlassian.net/browse/DATAGO-97452
    title: "DATAGO-97452: Move Solace connection to Spark partitions"
  - id: jira-111402
    resource: https://sol-jira.atlassian.net/browse/DATAGO-111402
    title: "DATAGO-111402: Idle timeout, repeated operations on one DataFrame, no batch on an empty queue, LVQ browser"
  - id: jira-92181
    resource: https://sol-jira.atlassian.net/browse/DATAGO-92181
    title: "DATAGO-92181: Solace message replay support"
  - id: jira-130445
    resource: https://sol-jira.atlassian.net/browse/DATAGO-130445
    title: "DATAGO-130445: Unity Catalog Volume as checkpoint location"
  - id: jira-89636
    resource: https://sol-jira.atlassian.net/browse/DATAGO-89636
    title: "DATAGO-89636: OAuth authentication support"
---

# Configuration

All configuration is passed as Spark options on `readStream` or `writeStream`. Option names are **case-sensitive** and camelCase (for example `batchSize`, `sparkRuntimePlatform`). An option with the wrong case is silently ignored. The full public reference is on [docs.solace.com](https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Configuring-Connection-Details.htm). This concept adds what the reference does not say: validation, interactions, and defaults that changed.[^docs-config]

## Connection options (source and sink)

| Option | Default | Notes |
|---|---|---|
| `host` | none, required | Protocol, host and port, for example `tcps://broker:55443`. A comma-separated host list is passed to the Solace API. |
| `vpn` | none, required | Message VPN. |
| `username`, `password` | none | Validated as required only when `solace.apiProperties.AUTHENTICATION_SCHEME` is `AUTHENTICATION_SCHEME_BASIC`, except for `df.write` in `foreachBatch`, which always requires them. Otherwise a missing value is sent empty and the broker rejects it. |
| `connectRetries` | Solace API default (0) | -1 retries forever. |
| `reconnectRetries` | Solace API default (3) | -1 retries forever. |
| `connectRetriesPerHost` | Solace API default (0) | For host lists. |
| `reconnectRetryWaitInMillis` | Solace API default (3000) | 0 to 60000. |
| `connectIdleTimeoutInMillis`, `connectIdleTimeoutCheckInMillis` | 0 (off) | Source only in effect. Both must be above 0. See [Read path](read-path.md).[^jira-111402] |
| `includeHeaders` | false | Adds the `Headers` column on read; maps the `Headers` column on write. |
| `solace.apiProperties.<PROPERTY>` | none | Passes any JCSMP session property, for example `solace.apiProperties.client_channel_properties.receiveBuffer=0`. See [JCSMPProperties](https://docs.solace.com/API-Developer-Online-Ref-Documentation/java/constant-values.html#com.solacesystems.jcsmp.JCSMPProperties). |

Authentication options (`solace.oauth.client.*` and certificate properties) are in [Authentication](authentication.md).

## Source options

| Option | Default | Notes |
|---|---|---|
| `queue` | none, required | The queue must already exist. |
| `batchSize` | 0 | Maximum messages per partition per micro-batch. 0 means no limit. A negative value is rejected. |
| `partitions` | 1 | Number of Spark tasks per batch, each with its own session and flow. The broker sees up to `partitions` times executor nodes consumers. 0 means one per executor node. See [Read path](read-path.md). |
| `queue.receiveWaitTimeout` | 10000 ms | How long a task waits for the next message before it closes its share of the batch. |
| `closeReceiversOnPartitionClose` | false | When true, each task closes its flow at the end of the batch instead of keeping it open. |
| `offsetIndicator` | `MESSAGE_ID` | Which message field becomes the `Id` column and the checkpointed ID: `MESSAGE_ID` (Replication Group Message ID), `CORRELATION_ID`, `APPLICATION_MESSAGE_ID`, or a user-property name. |
| `ackLastProcessedMessages` | false | On restart or reconnect, acknowledge and drop messages at or before the checkpoint. Ignored when a replay strategy is set. See [Checkpointing and acknowledgment](checkpointing-and-acknowledgment.md). |
| `ignoreCheckpointMessageIdComparisonError` | false | Only matters with `ackLastProcessedMessages=true`. |
| `lvq.name` | `solace.spark.connector.state` | Checkpoint LVQ. |
| `lvq.topic` | `solace/spark/connector/offset` | Topic the LVQ subscribes to. |
| `replayStrategy` | none | `BEGINNING`, `TIMEBASED` or `REPLICATION-GROUP-MESSAGE-ID` (case-sensitive).[^jira-92181] |
| `replayStartTime`, `replayStartTimeTimezone` | none, UTC | For `TIMEBASED`. Format `yyyy-MM-dd'T'HH:mm:ss`. |
| `replayReplicationGroupMessageId` | none | For `REPLICATION-GROUP-MESSAGE-ID`. |
| `sparkRuntimePlatform` and `databricks*` | `DATABRICKS` | Unity Catalog Volume checkpoints. See [Running on Databricks](running-on-databricks.md).[^jira-130445] |

## Sink options

| Option | Default | Notes |
|---|---|---|
| `topic` | none | Topic for every row. Overrides a `Topic` column. If unset, a `Topic` column is required. |
| `id` | none | Deprecated. Sets one message ID for every row and overrides the `Id` column. |
| `batchSize` | documented as 1 | Documented for the sink, but the sink does not read it. Each Spark task publishes all the rows of its partition. |
| `publishAckTimeout` | 5000 ms | Maximum wait for broker acknowledgments after a partition's batch. |
| `publishAckTimeoutFailOnError` | true | On timeout, fail the task (true) or log a warning and continue (false). |

The sink's required columns are in [Write path](write-path.md).

## What the connector sets regardless of options

- A client name of the form `solace/spark/connector/<role>...`, where the role is the driver's monitoring session, a consumer, or a producer. This makes the sessions easy to find on the broker.
- `PUB_ACK_WINDOW_SIZE=50` and receive timestamps on every session.
- Persistent delivery mode on every published message.

## Competency questions

After reading this concept, you should be able to answer these. The short answers point to the detail above.

- **What is the default `batchSize`, and what does it mean?** 0, which means no limit per partition. Set it explicitly.
- **Which options are always required?** `host` and `vpn`, plus `queue` for the source. `username` and `password` are checked only for basic authentication.
- **How do you set a Solace API property the connector has no option for?** Prefix it with `solace.apiProperties.`, for example `solace.apiProperties.client_channel_properties.receiveBuffer=0`.
- **Is `BatchSize` the same as `batchSize`?** No. Option names are case-sensitive, and the wrong case is ignored without an error.
- **Where do the Unity Catalog options go?** On the source, with `sparkRuntimePlatform=DATABRICKS`. See Running on Databricks.

## The non-obvious part

**The `batchSize` default changed, and older documentation is wrong.** It was 1 before 3.1.0 and became 0 (no limit) in 3.1.0.[^jira-97452] docs.solace.com gives 0.[^docs-troubleshooting] The user guide in the repository still gives 1, and the Getting Started guide records both values.[^cf-getting-started] With 0, one task can pull a whole backlog into a single micro-batch, so production jobs should always set it.

**Options were renamed twice, and old names fail silently.** The replay options were renamed during development, before replay shipped in 3.0.0 (for example `replayStrategyStartTime` became `replayStartTime`).[^jira-92181] The Databricks options were snake_case during 3.1.7 development and shipped as camelCase.[^jira-130445] `skipDuplicates`, `createFlowsOnSameSession` and `preferred_locations` were removed in 3.1.3.[^jira-111402] Because unknown options are ignored, a configuration copied from an old example keeps running without the setting it thinks it has.

**The sink's documented `batchSize` has no effect.** The sink configuration reference and the Getting Started guide list it, with advice to set it to `dataframe.count()`.[^cf-getting-started] The sink never reads it; the number of rows per publish is decided by Spark's partitioning.

**JMS property names do not work.** This connector uses the Solace Java API (JCSMP), not JMS, so `Solace_JMS_*` properties do nothing. Use the JCSMP name with the `solace.apiProperties.` prefix.

**OAuth token refresh needs no restart.** The OAuth options were designed so that a new token is set on the live Solace session without interrupting it.[^jira-89636] See [Authentication](authentication.md).

[^docs-config]: Connector for Apache Spark - Configuring connection details
[^docs-troubleshooting]: Connector for Apache Spark - Troubleshooting
[^cf-getting-started]: Getting Started Guide: Solace PubSub+ Connector for Apache Spark
[^jira-97452]: DATAGO-97452: Move Solace connection to Spark partitions
[^jira-111402]: DATAGO-111402: Idle timeout, repeated operations on one DataFrame, no batch on an empty queue, LVQ browser
[^jira-92181]: DATAGO-92181: Solace message replay support
[^jira-130445]: DATAGO-130445: Unity Catalog Volume as checkpoint location
[^jira-89636]: DATAGO-89636: OAuth authentication support
