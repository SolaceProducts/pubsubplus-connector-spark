---
type: concept
title: Write path
description: How the sink publishes Spark rows to Solace topics - required columns, topic choice, delivery mode, publish acknowledgments and the timeout, and streaming versus foreachBatch.
status: draft
tags:
  - connector
  - spark
  - sink
sources:
  - id: docs-config
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Configuring-Connection-Details.htm
    title: Connector for Apache Spark - Configuring connection details
  - id: docs-troubleshooting
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Troubleshooting.htm
    title: Connector for Apache Spark - Troubleshooting
  - id: cf-intro
    resource: https://sol-jira.atlassian.net/wiki/spaces/TCoE/pages/4632608848
    title: Solace Spark MI Introduction
  - id: cf-kba-driver
    resource: https://sol-jira.atlassian.net/wiki/spaces/SS/pages/6912180244
    title: "KBA: Solace Spark Connector Driver Instability on Long-Running Databricks Structured Streaming Jobs"
  - id: jira-91049
    resource: https://sol-jira.atlassian.net/browse/DATAGO-91049
    title: "DATAGO-91049: Solace data source write support"
  - id: jira-130445
    resource: https://sol-jira.atlassian.net/browse/DATAGO-130445
    title: "DATAGO-130445: Unity Catalog Volume as checkpoint location"
---

# Write path

The sink publishes each row of a Spark DataFrame as one persistent Solace message to a topic. It was added in 3.0.0.[^jira-91049] It can be used directly with `writeStream.format("solace")`, or as `df.write.format("solace")` inside `foreachBatch`, with a static topic or a topic per row.[^cf-intro]

## Columns

| Column | Required | Type | Maps to |
|---|---|---|---|
| `Id` | Yes | string | Application message ID, correlation ID and correlation key of the published message. Used to match broker acknowledgments to rows and to report failures. |
| `Payload` | Yes | binary | Message body. A missing column or a null value fails the write. |
| `Topic` | Yes, unless the `topic` option is set | string | Destination topic. The `topic` option overrides the column. |
| `PartitionKey` | No | string | Read, but not applied to the published message in current releases. See the non-obvious part. |
| `TimeStamp` | No | timestamp | Sender timestamp, passed to the broker without unit conversion. The integration tests send it as epoch milliseconds in a long column. |
| `Headers` | No; mapped only with `includeHeaders=true` | map&lt;string, binary&gt; | User properties. Keys that match writable Solace header fields (for example `solace_priority`, `solace_timeToLive`) also set that field. |

Sources: docs.solace.com and the ticket.[^docs-config][^jira-91049]

- The destination is always a topic. The sink cannot publish to a queue directly; subscribe a queue to the topic instead.
- Every message is published with persistent (guaranteed) delivery.[^docs-config]

## Publish acknowledgments

Each Spark task (one per DataFrame partition) opens its own producer session and publishes its rows asynchronously. Before the task commits, it waits until the broker has acknowledged every message, or until `publishAckTimeout` (default 5000 ms) runs out.[^jira-130445]

| `publishAckTimeoutFailOnError` | On timeout |
|---|---|
| true (default) | The task fails with `SolacePublishAckTimeoutException`, and Spark retries or fails the query according to its own settings. |
| false | A warning is logged and the task commits. Messages the broker did not acknowledge are not retried, so any that did not reach the broker are lost. |

Publish errors from the broker, such as `403: Publish ACL Denied`, are collected and raised on the task's next write or commit, with the IDs of the failed rows.

Tune `publishAckTimeout` against network latency, rows per task, and broker load.

## Streaming write versus `foreachBatch`

| | `writeStream.format("solace")` | `df.write.format("solace")` in `foreachBatch` |
|---|---|---|
| Authentication | Basic, client certificate or OAuth | Always requires `username` and `password`, even with certificate or OAuth authentication[^docs-troubleshooting] |
| Typical use | Straight Spark-to-Solace pipelines | Joins, writes to several sinks, or per-batch logic |

## Competency questions

After reading this concept, you should be able to answer these. The short answers point to the detail above.

- **Which columns must the DataFrame have?** `Id` and `Payload`, plus `Topic` unless the `topic` option is set.
- **Can the sink publish to a queue?** No. It publishes to topics. Subscribe a queue to the topic.
- **How does a user publish each row to a different topic?** Put the destination in a `Topic` column and do not set the `topic` option.
- **What happens if the broker does not acknowledge a batch within `publishAckTimeout`?** By default the task fails with `SolacePublishAckTimeoutException`. With `publishAckTimeoutFailOnError=false` it logs a warning and continues.
- **What delivery mode do published messages use?** Persistent, always.
- **Why does `foreachBatch` fail with an OAuth configuration?** The batch write path always checks for `username` and `password`. Supply them, or use `writeStream`.

## The non-obvious part

**The `PartitionKey` column does not reach the message.** The key is added to the message's properties after they have already been built, so published messages carry no partition key. Downstream partitioned queues therefore see messages without a key. This affects 3.2.0 and 4.0.0.

**`Id` must be unique within each Spark partition.** Acknowledgments are matched to rows by `Id`. If two rows in the same partition share an `Id`, the expected count is never reached, and the task waits the full `publishAckTimeout`. The deprecated `id` option, which gives every row the same ID, has the same effect.

**A new producer session is opened per partition per batch.** Each task connects, including a DNS lookup, at the start of every batch. A short DNS or network failure can therefore fail a task. This was one of the confirmed findings in a long-running Databricks support case. See [Performance and tuning](performance-and-tuning.md).[^cf-kba-driver]

[^docs-config]: Connector for Apache Spark - Configuring connection details
[^docs-troubleshooting]: Connector for Apache Spark - Troubleshooting
[^cf-intro]: Solace Spark MI Introduction
[^cf-kba-driver]: KBA: Solace Spark Connector Driver Instability on Long-Running Databricks Structured Streaming Jobs
[^jira-91049]: DATAGO-91049: Solace data source write support
[^jira-130445]: DATAGO-130445: Unity Catalog Volume as checkpoint location
