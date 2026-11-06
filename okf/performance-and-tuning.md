---
type: concept
title: Performance and tuning
description: Measured throughput, the settings that move it (batch size, consumers, unacknowledged-message window, the Id column, TCP buffers), and known stability issues on long-running jobs.
status: draft
stale_after: 2027-05-05T00:00:00Z
tags:
  - connector
  - spark
  - performance
  - troubleshooting
sources:
  - id: docs-config
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Configuring-Connection-Details.htm
    title: Connector for Apache Spark - Configuring connection details
  - id: cf-intro
    resource: https://sol-jira.atlassian.net/wiki/spaces/TCoE/pages/4632608848
    title: Solace Spark MI Introduction
  - id: cf-best-practices
    resource: https://sol-jira.atlassian.net/wiki/spaces/TCoE/pages/5670344732
    title: Solace Spark MI Best Practices & Troubleshooting Guide
  - id: cf-kba-cross-region
    resource: https://sol-jira.atlassian.net/wiki/spaces/SS/pages/6918275100
    title: "KBA: Slow Egress Rate with Solace Spark Connector in Cross-Region Deployments"
  - id: cf-kba-driver
    resource: https://sol-jira.atlassian.net/wiki/spaces/SS/pages/6912180244
    title: "KBA: Solace Spark Connector Driver Instability on Long-Running Databricks Structured Streaming Jobs"
  - id: cf-debug
    resource: https://sol-jira.atlassian.net/wiki/spaces/SS/pages/6525354080
    title: How to debug Spark connector
  - id: jira-146987
    resource: https://sol-jira.atlassian.net/browse/DATAGO-146987
    title: "DATAGO-146987: Spark throughput investigation (support case)"
  - id: jira-149324
    resource: https://sol-jira.atlassian.net/browse/DATAGO-149324
    title: "DATAGO-149324: Driver instability on long-running Databricks jobs (support case)"
---

# Performance and tuning

Throughput is set mostly by how many messages Spark takes per batch and how fast the output sink writes them. The connector's own work is small next to Spark's `addBatch` step (read, transform, write).

## Reference benchmark

Measured on Azure Databricks 16.4 LTS (Spark 3.5.2, Photon on), version 3.1.6. The setup had a driver with 4 cores and 3 workers with 8 cores each, so 24 consumer slots. The broker was a PubSub+ Standard software broker on an 8-core VM in the same region, with 1 to 3 ms latency. Messages were 3000 bytes, written to a Delta table with no transformation. The queue held over 6 million messages, and the maximum delivered unacknowledged messages per flow was 20000.[^cf-intro]

| Consumers | `batchSize` | Messages per Spark batch | Rate |
|---|---|---|---|
| 8 | 20000 | 160000 | 30k to 40k msg/s |
| 8, `Id` column dropped | 20000 | 160000 | 40k to 60k msg/s |
| 24 | 10000 | 240000 | 40k to 60k msg/s |
| 24, `Id` column dropped | 10000 | 240000 | 65k to 90k msg/s |

All messages were processed and acknowledged, and the Delta row count matched the queue. With the `Id` column, `addBatch` took about 3 s per batch; without it, 1 to 2 s.[^cf-intro]

These numbers are a reference point. Message size, latency to the broker, the sink, and transformations all change them.

## Tuning

| Setting | Guidance |
|---|---|
| `batchSize` | The main lever. Start at 10000 for small messages. Lower it for large messages: one support case with ~20 KB messages at 150 to 200 msg/s was stable with `batchSize` 200 and 4 consumers.[^jira-146987] |
| Queue: maximum delivered unacknowledged messages per flow | 2 times `batchSize`, or 1 times for large messages. Below `batchSize`, each consumer stalls before it can fill a batch.[^cf-best-practices] |
| `partitions` | Up to the number of cores per worker, times workers. One consumer per queue if order matters; then add queues for throughput.[^cf-best-practices] |
| `Id` column | Drop it in the query (`df.drop("Id")`) unless the pipeline needs the message ID for deduplication or replay. It is the most expensive column to write.[^cf-intro] |
| `queue.receiveWaitTimeout` | Default 10000 ms. Lower it for low-volume or bursty queues so batches do not wait to fill.[^cf-best-practices] |

To find the bottleneck, compare batch durations in the logs (`batchDuration`) and `addBatch` time in the Spark UI. If `addBatch` dominates, the sink or the transformation is the limit, not the connector.[^cf-debug]

## Cross-region latency

When the broker and the cluster are in different regions (for example after a DR failover moves the active broker), egress can drop to tens or hundreds of messages per second, with batches taking 20 to 30 s. The cause is the TCP receive buffer: the default is too small for the higher round-trip time. The fix is to let the operating system size it:[^cf-kba-cross-region]

```
solace.apiProperties.client_channel_properties.receiveBuffer=0
```

In the support case behind this fix, cross-region egress rose from about 400 to over 1900 msg/s. Compression (`client_channel_properties.compressionLevel` on the compressed port) also helped, but costs broker CPU.[^jira-146987]

## Long-running jobs

A support case on 3.1.6 found that streams running for days on Databricks lost their driver every one to three days. The investigation confirmed three weaknesses:[^cf-kba-driver][^jira-149324]

1. The LVQ checkpoint publish on commit has no retry, so a "Channel is closed by peer" ends the query.
2. The sink opens a new producer session (with a DNS lookup) per partition per batch, so a DNS blip fails the task.
3. The driver's empty-queue check, meant to wait a few seconds, could block far longer when the queue was empty; 80 minutes was observed. This is a defect in the released versions, not the intended behaviour.

The driver's out-of-memory error itself has no identified root cause. The fixes (retries on the LVQ publish and on producer session creation, session reuse, and a bounded queue check) are built on top of 3.1.7. They are planned for the 3.2.x and 4.0.x lines and are not in 3.2.0 or 4.0.0.[^jira-149324]

## Competency questions

After reading this concept, you should be able to answer these. The short answers point to the detail above.

- **What throughput can a customer expect?** As a reference, 30k to 90k msg/s for 3000-byte messages on three 8-core Databricks workers in the same region as the broker, depending on consumers and whether the `Id` column is kept.
- **What queue setting must match `batchSize`?** The maximum delivered unacknowledged messages per flow: 2 times `batchSize`, or 1 times for large messages.
- **Why is throughput far lower when the broker is in another region?** The default TCP receive buffer is too small for the round-trip time. Set `solace.apiProperties.client_channel_properties.receiveBuffer=0`.
- **How do you tell whether Spark or the connector is the bottleneck?** Look at `addBatch` time in the Spark UI. If it dominates the batch duration, the sink or the transformation is the limit.
- **Are the long-running stability fixes in the latest release?** No. They are on a branch built from 3.1.7 and planned for later 3.2.x and 4.0.x releases.

## The non-obvious part

**The size of the `Id` column effect is quoted inconsistently.** The Introduction page and the Getting Started guide describe it as a 10 to 15 times improvement. The benchmark's own rates show about 1.3 to 1.6 times, and docs.solace.com says roughly 1.5 times.[^docs-config] Use the measured rates in the table above.

**More consumers do not help if the sink is slow.** In one support case, batches took 15 s at `batchSize` 1000 and 26 s at 2000, and the time went into the Delta write, not into consuming.[^jira-146987]

[^docs-config]: Connector for Apache Spark - Configuring connection details
[^cf-intro]: Solace Spark MI Introduction
[^cf-best-practices]: Solace Spark MI Best Practices & Troubleshooting Guide
[^cf-kba-cross-region]: KBA: Slow Egress Rate with Solace Spark Connector in Cross-Region Deployments
[^cf-kba-driver]: KBA: Solace Spark Connector Driver Instability on Long-Running Databricks Structured Streaming Jobs
[^cf-debug]: How to debug Spark connector
[^jira-146987]: DATAGO-146987: Spark throughput investigation (support case)
[^jira-149324]: DATAGO-149324: Driver instability on long-running Databricks jobs (support case)
