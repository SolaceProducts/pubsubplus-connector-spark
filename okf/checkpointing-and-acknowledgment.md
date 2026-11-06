---
type: concept
title: Checkpointing and acknowledgment
description: When source messages are acknowledged, how processed message IDs are checkpointed in the Spark checkpoint and a Solace LVQ, what restart, replay and DR failover do, and why delivery is at least once.
status: draft
tags:
  - connector
  - spark
  - source
  - checkpointing
  - lvq
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
  - id: cf-best-practices
    resource: https://sol-jira.atlassian.net/wiki/spaces/TCoE/pages/5670344732
    title: Solace Spark MI Best Practices & Troubleshooting Guide
  - id: cf-kba-rgmid
    resource: https://sol-jira.atlassian.net/wiki/spaces/SS/pages/6246727735
    title: "KBA: Spark Connector Fails After Broker Recreation with JCSMPNotComparableException"
  - id: cf-release-314
    resource: https://sol-jira.atlassian.net/wiki/spaces/TCoE/pages/5670666468
    title: Spark v3.1.4 release notes
  - id: jira-97452
    resource: https://sol-jira.atlassian.net/browse/DATAGO-97452
    title: "DATAGO-97452: Move Solace connection to Spark partitions"
  - id: jira-118237
    resource: https://sol-jira.atlassian.net/browse/DATAGO-118237
    title: "DATAGO-118237: LVQ non-owner permission and other enhancements"
  - id: jira-128386
    resource: https://sol-jira.atlassian.net/browse/DATAGO-128386
    title: "DATAGO-128386: Replication Group Message ID comparison fails during DR failover"
  - id: jira-147415
    resource: https://sol-jira.atlassian.net/browse/DATAGO-147415
    title: "DATAGO-147415: Failure loop after DR failover with ackLastProcessedMessages"
  - id: jira-154398
    resource: https://sol-jira.atlassian.net/browse/DATAGO-154398
    title: "DATAGO-154398: Document LVQ handling during DR failover"
  - id: jira-149305
    resource: https://sol-jira.atlassian.net/browse/DATAGO-149305
    title: "DATAGO-149305: Start-up fails without endpoint permission override (support case)"
  - id: jira-92181
    resource: https://sol-jira.atlassian.net/browse/DATAGO-92181
    title: "DATAGO-92181: Solace message replay support"
  - id: spark-datastream
    resource: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/read/streaming/SparkDataStream.html
    title: Apache Spark Javadoc - SparkDataStream
---

# Checkpointing and acknowledgment

The source never acknowledges a message to the broker until Spark has committed the batch it belongs to. That makes it safe against data loss, at the cost of duplicates. Delivery is **at least once**, and the downstream system must be idempotent.[^cf-getting-started]

## The model

Spark's contract is offset based. When Spark calls `commit(end)`, it has finished all data up to `end` and will only ask for later offsets.[^spark-datastream] A Solace queue has no such offsets, so the connector keeps two things:

| Record | What it holds | Where |
|---|---|---|
| Spark offset | A counter that advances with each non-empty trigger, plus the checkpoint list | The Spark checkpoint directory, written by Spark |
| Processed message IDs | The last processed message ID of each consumer | A file per consumer in the checkpoint location, merged by the driver into one message on the LVQ |

## When messages are acknowledged

1. A task reads its messages and Spark writes the batch to its output.
2. When the task completes, it writes the last processed message ID for its consumer to a file in the checkpoint location, and clears its list of processed IDs.
3. On Spark's commit, the driver reads those files, merges them and publishes the result as a persistent message to `lvq.topic`, which the LVQ holds.[^jira-97452]
4. When the next batch starts on that consumer, the task first acknowledges the previous batch's messages to the broker, then reads new ones. Messages it receives that are already in the checkpoint, matched by Replication Group Message ID, are skipped.

So a consumer's messages stay unacknowledged until Spark has committed their batch and started the next one. Spark runs that next batch even when no new messages have arrived, so the last messages are acknowledged without waiting for more traffic; the batch itself can be empty. If the job stops before that batch starts, the last batch is redelivered on restart.

## The checkpoint LVQ

A last value queue (LVQ) keeps only the most recent message. The connector uses it to carry checkpoint state from the executors to the driver, and to recover the last processed IDs on restart.[^cf-getting-started]

- **Defaults**: `lvq.name=solace.spark.connector.state`, `lvq.topic=solace/spark/connector/offset`. The defaults do not depend on the queue name. Give each pipeline its own LVQ name and topic.[^cf-best-practices]
- **Reading**: since 3.1.4 the LVQ is read with a queue browser on demand, instead of a bound consumer, so checkpoint messages no longer pile up.[^cf-release-314]
- **Do not delete or edit the LVQ message.** Recovery uses it to find the last processed messages.[^cf-getting-started]

LVQ requirements, from the public documentation:[^docs-config][^docs-troubleshooting][^jira-118237]

| | The connector creates the LVQ | An administrator creates the LVQ |
|---|---|---|
| Queue settings | Created as exclusive with spool quota 0, owned by the connecting client username, with the topic subscription | Exclusive; spool quota 0; topic subscription to `lvq.topic` already added; owner explicitly set to the connecting client username (creating the queue does not do this); non-owner access set to No Access |
| Client profile | "Allow Client to Create Endpoints" (`allow-guaranteed-endpoint-create` on software and appliance brokers) | The same, because the connector asks to create the LVQ at every start |
| ACL | Publish and subscribe on `lvq.topic` | Publish and subscribe on `lvq.topic` |

If an administrator creates the LVQ and leaves the owner blank or set to another user, the connector cannot use it and fails at startup with a permission error.[^jira-149305] No other client may consume, delete, or delete messages from the LVQ; any of those stops the connector from resuming from its checkpoint.[^docs-troubleshooting]

Since 3.1.4, a permission failure while setting up the LVQ stops the stream at startup. Before 3.1.4 it was logged and largely ignored.[^jira-149305]

## Restart and `ackLastProcessedMessages`

With `ackLastProcessedMessages=true`, on every new flow the connector compares each incoming message's Replication Group Message ID with the checkpointed ID. It acknowledges and drops messages at or before the checkpoint. That removes the duplicates left by the last uncommitted batch. It works only when the downstream system did process the previous run's data. It has no effect when a replay strategy is set.[^docs-config] The best-practices guide recommends deduplicating downstream instead and using this option with caution.[^cf-best-practices]

## Replay

`replayStrategy` starts the flow from the broker's replay log: from the beginning, from a time, or after a Replication Group Message ID. The broker must have a replay log enabled.[^jira-92181] Replay is applied whenever a flow is created, which includes reconnects and, with `closeReceiversOnPartitionClose=true`, every batch. Use replay for a one-off recovery run, then remove it.

## DR failover and non-comparable message IDs

Replication Group Message IDs are only comparable within the same replication group. After a DR failover, or after brokers are recreated, the checkpointed ID comes from the old site. Comparing it with new IDs throws `JCSMPNotComparableException`, and with `ackLastProcessedMessages=true` the query fails and loops on restart.[^jira-147415][^cf-kba-rgmid]

| Option | Effect |
|---|---|
| `ignoreCheckpointMessageIdComparisonError=true` (3.1.7+) | Logs a warning and treats the message as new. It is not deduplicated.[^jira-128386] |
| `ackLastProcessedMessages=false` | No comparison happens. Deduplicate downstream. |
| Clear the LVQ on the new active site, then restart | Needed only if the checkpoint was replicated. If the LVQ was not replicated, no action is needed.[^jira-154398] |

## Competency questions

After reading this concept, you should be able to answer these. The short answers point to the detail above.

- **What delivery guarantee does the source give?** At least once. Duplicates are possible, and the downstream system must be idempotent.
- **When is a message acknowledged to the broker?** After Spark has committed its batch, when the next batch starts on the same consumer.
- **Why does the connector need an LVQ when Spark already has a checkpoint?** Consumers run on executors, and the LVQ carries their processed message IDs to the driver and keeps them for restart.
- **What permissions does the LVQ need?** Publish and subscribe on `lvq.topic` in the ACL, and "Allow Client to Create Endpoints" in the client profile, even when an administrator created the LVQ. An administrator-created LVQ must also be owned by the connecting client username.
- **After a DR failover the query fails with "Replication Group Message Id are not comparable". What are the options?** Set `ignoreCheckpointMessageIdComparisonError=true` (3.1.7+), or turn off `ackLastProcessedMessages` and deduplicate downstream, or clear the replicated LVQ and restart.
- **Can two pipelines share the default LVQ?** They should not. The defaults do not depend on the queue, so give each pipeline its own `lvq.name` and `lvq.topic`.

## The non-obvious part

**Duplicates have several causes, and all of them are normal.** These include an uncommitted batch redelivered after a failure, the last batch before a stop, a session closed by the idle timeout, a checkpoint write lost in a crash, and redelivery to another consumer. `ackLastProcessedMessages` removes only the first kind, and only when the IDs are comparable.

**`ignoreCheckpointMessageIdComparisonError` defaults to false on purpose.** Skipping the comparison automatically would also skip deduplication for valid messages, so the connector leaves the choice to the operator.[^jira-147415] The error message suggests a kebab-case name, `ignore-checkpoint-message-id-comparison-error`. The option that works is `ignoreCheckpointMessageIdComparisonError`.

**The driver must be able to read what the executors wrote.** Each task writes its checkpoint file to the checkpoint location from its executor, and the driver reads all of them on commit. A location that is local to each node breaks this. On Databricks, Unity Catalog Volumes need extra setup, because executors cannot write to them with ordinary file APIs. See [Running on Databricks](running-on-databricks.md).

**The LVQ setup runs at every start.** Without permission to create endpoints, startup fails with subcode 36 even when the LVQ already exists.[^docs-troubleshooting]

[^docs-config]: Connector for Apache Spark - Configuring connection details
[^docs-troubleshooting]: Connector for Apache Spark - Troubleshooting
[^cf-getting-started]: Getting Started Guide: Solace PubSub+ Connector for Apache Spark
[^cf-best-practices]: Solace Spark MI Best Practices & Troubleshooting Guide
[^cf-kba-rgmid]: KBA: Spark Connector Fails After Broker Recreation with JCSMPNotComparableException
[^cf-release-314]: Spark v3.1.4 release notes
[^jira-97452]: DATAGO-97452: Move Solace connection to Spark partitions
[^jira-118237]: DATAGO-118237: LVQ non-owner permission and other enhancements
[^jira-128386]: DATAGO-128386: Replication Group Message ID comparison fails during DR failover
[^jira-147415]: DATAGO-147415: Failure loop after DR failover with ackLastProcessedMessages
[^jira-154398]: DATAGO-154398: Document LVQ handling during DR failover
[^jira-149305]: DATAGO-149305: Start-up fails without endpoint permission override (support case)
[^jira-92181]: DATAGO-92181: Solace message replay support
[^spark-datastream]: Apache Spark Javadoc - SparkDataStream
