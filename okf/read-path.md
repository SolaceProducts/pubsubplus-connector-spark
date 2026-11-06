---
type: concept
title: Read path
description: How the source turns a Solace queue into Spark micro-batches - consumers, batch size, the row schema, headers, empty queues, and idle connections.
status: draft
tags:
  - connector
  - spark
  - source
sources:
  - id: docs-overview
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Overview.htm
    title: Connector for Apache Spark - Overview (self-managed)
  - id: cf-getting-started
    resource: https://sol-jira.atlassian.net/wiki/spaces/EventInteg/pages/6628999206
    title: "Getting Started Guide: Solace PubSub+ Connector for Apache Spark"
  - id: cf-best-practices
    resource: https://sol-jira.atlassian.net/wiki/spaces/TCoE/pages/5670344732
    title: Solace Spark MI Best Practices & Troubleshooting Guide
  - id: jira-97452
    resource: https://sol-jira.atlassian.net/browse/DATAGO-97452
    title: "DATAGO-97452: Move Solace connection to Spark partitions"
  - id: jira-111402
    resource: https://sol-jira.atlassian.net/browse/DATAGO-111402
    title: "DATAGO-111402: Idle timeout, repeated operations on one DataFrame, no batch on an empty queue, LVQ browser"
  - id: jira-120936
    resource: https://sol-jira.atlassian.net/browse/DATAGO-120936
    title: "DATAGO-120936: Disable partition key handling for checkpointing"
  - id: jira-84486
    resource: https://sol-jira.atlassian.net/browse/DATAGO-84486
    title: "DATAGO-84486: Extracting the payload as a string"
  - id: jira-124815
    resource: https://sol-jira.atlassian.net/browse/DATAGO-124815
    title: "DATAGO-124815: Duplicates with parallel consumers (support case)"
  - id: cf-release-313
    resource: https://sol-jira.atlassian.net/wiki/spaces/TCoE/pages/5670666449
    title: Spark v3.1.3 release notes
---

# Read path

The source consumes from one Solace queue and hands Spark a micro-batch per trigger. Each micro-batch is the union of what every consumer pulled in that trigger.

## One trigger, step by step

1. **The driver checks the queue.** It browses the queue with a short wait. If it sees no message, it reports no new offset and Spark starts no batch, apart from the follow-up batch that acknowledges the previous one (see [Checkpointing and acknowledgment](checkpointing-and-acknowledgment.md)).[^jira-111402] The driver keeps one Solace session for this check and for the checkpoint LVQ.
2. **The driver plans `partitions` tasks.** Each task gets a stable ID derived from the queue name and its index. Spark is asked to place it on a preferred executor, chosen from that ID, so a consumer tends to stay on the same node across batches.[^cf-getting-started]
3. **Each task consumes on its executor.** The task opens (or reuses) a Solace session and a client-acknowledgment flow on the queue. It keeps reading until it has `batchSize` messages, or until one wait of `queue.receiveWaitTimeout` passes with no message.[^jira-97452]
4. **Messages become rows.** See the schema below.
5. **Acknowledgment and checkpointing follow Spark's commit.** See [Checkpointing and acknowledgment](checkpointing-and-acknowledgment.md).

Sessions are cached in the executor JVM and reused across batches. By default the flow also stays open between batches. `closeReceiversOnPartitionClose=true` closes it at the end of each task instead.

## Consumers and `partitions`

| `partitions` | Spark tasks per batch | Use |
|---|---|---|
| 1 (default) | One | Keeps queue order on a single node |
| N | N, each with its own flow | Throughput. Order across consumers is not kept. |
| 0 | One per executor node, scaling as nodes are added | Discouraged: the number of consumers on the queue is unbounded |

**Consumers on the broker are `partitions` times executor nodes.** Each executor node keeps its own session and flow for every partition it has run, and Spark runs partitions on whichever nodes it chooses. So with 4 partitions on 3 nodes, expect 12 consumers bound to the queue. Size the broker's connection and flow limits for that number.[^cf-best-practices]

Sources: the Getting Started guide and the best-practices guide.[^cf-getting-started][^cf-best-practices]

- **Each consumer is a Spark task, so it needs a core.** On an executor with 4 cores, more than 4 consumers on that node do not run in parallel.[^cf-best-practices]
- **The queue can be exclusive or non-exclusive.** Every flow binds as a regular consumer. On an exclusive queue only one flow receives at a time, so extra partitions add sessions without adding throughput.
- **Duplicates between consumers are expected after redelivery.** When the broker redelivers a message after a failure, another consumer may receive it. This was confirmed as expected behaviour in a support case.[^jira-124815]

## Row schema

| Column | Type | Content |
|---|---|---|
| `Id` | string | The message ID chosen by `offsetIndicator`. By default, the Replication Group Message ID. |
| `Payload` | binary | Message body. Text messages are UTF-8 bytes; otherwise the binary attachment, or the XML content. |
| `PartitionKey` | string | The message's partition key (`JMSXGroupID`) if present, otherwise empty. |
| `Topic` | string | The topic the message was published to. |
| `TimeStamp` | timestamp | Sender timestamp if present; otherwise the time the connector received it. |
| `Headers` | map&lt;string, binary&gt; | Only with `includeHeaders=true`. User properties, plus `solace_sequenceNumber`, `solace_expiration`, `solace_timeToLive`, `solace_priority` and `solace_redelivered`. Values are UTF-8 text bytes. |

To read the payload as text: `df.select($"payload".cast("STRING"))`. Before 2.0.2, text messages were passed as raw encoded bytes, so a cast to string showed extra characters. The fix maps text messages to plain UTF-8.[^jira-84486]

## Empty queues and slow queues

- **An empty queue produces no batch.** Since 3.1.3 the driver checks the queue first, instead of starting a batch with no rows.[^cf-release-313] One exception: after messages are processed, Spark runs one more batch to acknowledge them, and that batch can be empty. See [Checkpointing and acknowledgment](checkpointing-and-acknowledgment.md).
- **A queue with fewer messages than `batchSize` makes each task wait.** A task stops only after one full `queue.receiveWaitTimeout` with no message. With the default 10 s, a quiet queue produces a batch about every 10 s. Lower the timeout for low-volume queues.[^cf-best-practices]

## Running several actions on one DataFrame

Since 3.1.3, if Spark asks for the same batch again (for example `count()` and then a write in `foreachBatch`), each task returns the same messages it already read, topped up from the queue if it had fewer than `batchSize`. It acknowledges nothing until a new batch starts.[^cf-release-313]

## Idle connections

`connectIdleTimeoutInMillis` and `connectIdleTimeoutCheckInMillis` close a consumer session that has not produced a row for longer than the timeout. Both are off by default and both must be set. The next task for that partition reconnects. Unacknowledged messages on a closed session are redelivered, so the downstream system must handle duplicates.[^jira-111402] Use it on clusters where Spark sessions do not end cleanly and leave consumers on the broker. The timeout must be longer than the slowest batch, or busy consumers are closed too.

## Competency questions

After reading this concept, you should be able to answer these. The short answers point to the detail above.

- **How many messages are in one micro-batch?** Up to `batchSize` per consumer, so up to `batchSize` times `partitions` in total. Each consumer stops early after one empty `queue.receiveWaitTimeout`.
- **Is message order preserved?** Only with one consumer (`partitions=1`). For order and throughput, split the load across several queues with one consumer each.
- **Why does a low-volume stream deliver a batch only every 10 seconds?** Each consumer waits up to `queue.receiveWaitTimeout` (default 10 s) to fill its batch. Lower it.
- **Can it consume from a partitioned queue?** No. The connector needs queue browsing, which partitioned queues do not support.
- **How do you get the payload as a string?** Cast the binary column: `df.select($"payload".cast("STRING"))`.
- **How many consumers does `partitions=4` give on a 3-node cluster?** 12. Each executor node keeps its own consumer for each partition it runs.
- **Why is `partitions=0` discouraged?** It creates one consumer per executor node, so the number of consumers on the queue grows with the cluster and can exhaust broker connection limits.

## The non-obvious part

**Partitioned queues are unsupported, even though the schema has a `PartitionKey` column.** The driver browses the source queue to decide whether to start a batch, and partitioned queues do not allow browsing. Partition-key checkpointing was switched off in 3.1.6 for the same reason.[^jira-120936][^docs-overview] Older release notes that mention "checkpoint handling for partitioned queues" describe the work that was later disabled. The `PartitionKey` column is still filled on read.

**The source browses the queue, so the client needs browse permission on it.** This comes on top of consume permission. The driver's check uses a browser flow, which shows on the broker as a browser bind, separate from the consumer binds.

**`partitions` sets the Spark tasks, not the broker consumers.** Placement on executors is a preference, not a rule, and sessions are cached per executor node. Over time each node opens a consumer for each partition it runs, which is why the broker shows `partitions` times nodes. Idle flows keep receiving messages up to the queue's unacknowledged-message limit, so those messages wait until that node runs the partition again. `closeReceiversOnPartitionClose=true` closes the flow at the end of each task instead.

[^docs-overview]: Connector for Apache Spark - Overview (self-managed)
[^cf-getting-started]: Getting Started Guide: Solace PubSub+ Connector for Apache Spark
[^cf-best-practices]: Solace Spark MI Best Practices & Troubleshooting Guide
[^jira-97452]: DATAGO-97452: Move Solace connection to Spark partitions
[^jira-111402]: DATAGO-111402: Idle timeout, repeated operations on one DataFrame, no batch on an empty queue, LVQ browser
[^jira-120936]: DATAGO-120936: Disable partition key handling for checkpointing
[^jira-84486]: DATAGO-84486: Extracting the payload as a string
[^jira-124815]: DATAGO-124815: Duplicates with parallel consumers (support case)
[^cf-release-313]: Spark v3.1.3 release notes
