---
type: concept
title: Regression guards
description: Past decisions with their tickets and reasons, and what a future change must not undo.
status: draft
tags:
  - connector
  - spark
  - regression
sources:
  - id: jira-97452
    resource: https://sol-jira.atlassian.net/browse/DATAGO-97452
    title: "DATAGO-97452: Move Solace connection to Spark partitions"
  - id: jira-86131
    resource: https://sol-jira.atlassian.net/browse/DATAGO-86131
    title: "DATAGO-86131: Change Scala version to 2.12"
  - id: jira-111402
    resource: https://sol-jira.atlassian.net/browse/DATAGO-111402
    title: "DATAGO-111402: Idle timeout, repeated operations on one DataFrame, no batch on an empty queue, LVQ browser"
  - id: jira-143720
    resource: https://sol-jira.atlassian.net/browse/DATAGO-143720
    title: "DATAGO-143720: Browser binds on the LVQ (support case)"
  - id: jira-149305
    resource: https://sol-jira.atlassian.net/browse/DATAGO-149305
    title: "DATAGO-149305: Start-up fails without endpoint permission override (support case)"
  - id: jira-118237
    resource: https://sol-jira.atlassian.net/browse/DATAGO-118237
    title: "DATAGO-118237: LVQ non-owner permission and other enhancements"
  - id: cf-release-315
    resource: https://sol-jira.atlassian.net/wiki/spaces/TCoE/pages/5989368000
    title: Spark v3.1.5 release notes
  - id: jira-128386
    resource: https://sol-jira.atlassian.net/browse/DATAGO-128386
    title: "DATAGO-128386: Replication Group Message ID comparison fails during DR failover"
  - id: jira-147415
    resource: https://sol-jira.atlassian.net/browse/DATAGO-147415
    title: "DATAGO-147415: Failure loop after DR failover with ackLastProcessedMessages"
  - id: jira-120936
    resource: https://sol-jira.atlassian.net/browse/DATAGO-120936
    title: "DATAGO-120936: Disable partition key handling for checkpointing"
  - id: seot-102
    resource: https://sol-jira.atlassian.net/browse/SEOT-102
    title: "SEOT-102: Partitioned queue support (field request)"
  - id: jira-84486
    resource: https://sol-jira.atlassian.net/browse/DATAGO-84486
    title: "DATAGO-84486: Extracting the payload as a string"
  - id: jira-92181
    resource: https://sol-jira.atlassian.net/browse/DATAGO-92181
    title: "DATAGO-92181: Solace message replay support"
  - id: jira-89636
    resource: https://sol-jira.atlassian.net/browse/DATAGO-89636
    title: "DATAGO-89636: OAuth authentication support"
  - id: jira-142122
    resource: https://sol-jira.atlassian.net/browse/DATAGO-142122
    title: "DATAGO-142122: Update to JDK 11 and modernize the 3.x build"
  - id: jira-142067
    resource: https://sol-jira.atlassian.net/browse/DATAGO-142067
    title: "DATAGO-142067: Upgrade to Spark 4.0.0 (Java 17, Scala 2.13)"
  - id: jira-147861
    resource: https://sol-jira.atlassian.net/browse/DATAGO-147861
    title: "DATAGO-147861: Publish the 4.x line from its own branch"
  - id: cf-ebp2255
    resource: https://sol-jira.atlassian.net/wiki/spaces/PLMT/pages/6893764626
    title: "[KA] EBP-2255 - Remove Java 8 and 11 runtime support across JCSMP and JMS/JAKARTA"
---

# Regression guards

Each guard is a decision that was made for a reason and that a reasonable-looking change could undo. Read the ticket before changing the behaviour.

## Where things run

**Consumers run on the executors, not the driver.** Since 3.1.0 the Solace connection and data processing run on Spark worker nodes, "for better utilization of resources and improvement in processing of data". Do not move consumption back to the driver.[^jira-97452]

**The driver browses the source queue before planning a batch.** This stops micro-batches from starting when the queue has nothing to deliver (3.1.3). Do not replace the check with an unconditional batch.[^jira-111402]

## Checkpoint LVQ

**Read the LVQ with a queue browser, not a consumer.** Since 3.1.4 the LVQ is browsed on demand, so checkpoint messages do not pile up. A browser bind on the LVQ is normal and is not a leak.[^jira-111402][^jira-143720]

**LVQ permission failures stop the stream at startup.** Before 3.1.4 they were logged and largely ignored, which hid broken checkpointing until a restart. Do not go back to logging and continuing.[^jira-149305]

**When the connector creates the LVQ, it leaves the non-owner permission at the broker default.** 3.1.5 made this change to avoid a mismatch with an LVQ that an administrator pre-provisioned. Administrator-created LVQs still follow the documented rules: owner set to the client username, non-owner access No Access.[^jira-118237][^cf-release-315]

## Restart and deduplication

**`ignoreCheckpointMessageIdComparisonError` stays `false` by default.** The comparison of Replication Group Message IDs fails after a DR failover, because the two sites' IDs are not comparable. Skipping the comparison automatically "can lead to skipping de-duplication for valid messages", so it must stay the operator's explicit choice.[^jira-128386][^jira-147415]

**Partition-key checkpointing stays off while partitioned queues are unsupported.** It was disabled in 3.1.6. Partitioned queues do not allow browsing, which the connector needs.[^jira-120936][^seot-102] Re-enabling it needs a design for consuming without browsing, not just a flag.

## Data mapping

**Text messages become UTF-8 payload bytes.** Before 2.0.2, a text message's raw encoded bytes were passed through, and casting the payload to a string showed extra characters. Do not return to passing raw bytes.[^jira-84486]

**Keep the shipped option names.** The replay options were renamed during development, before replay shipped in 3.0.0 (`replayStrategyStartTime` to `replayStartTime` and so on), and customers use the shipped names. Unknown options are ignored silently, so a rename breaks configurations without an error.[^jira-92181]

**OAuth token refresh updates the session without a restart.** That was the goal of the OAuth work: refresh "without any interruptions".[^jira-89636]

## Release lines and platform

**3.x stays on Scala 2.12.** A 2.13 build failed to load on Databricks Spark 3.5 clusters ("conflicting Scala libraries"); 2.0.3 moved back to 2.12.[^jira-86131]

**Neither line gains capabilities the other lacks.** 3.2.0 was "no Spark/Scala major-version change and no new capabilities", and 4.0.0 was "no new read/write capabilities".[^jira-142122][^jira-142067]

**Keep the 4.x branch separate until 3.x reaches end of life.** The 4.x line is released from its own branch, and that branch is not merged into the 3.x mainline until 3.x support ends.[^jira-147861]

**Upgrading JCSMP on 3.x past 10.30.x forces Java 17.** JCSMP 10.31.0 is the first release that needs Java 17 at runtime. Both lines pin 10.28.3 today. Plan any JCSMP upgrade on 3.x as a Java baseline change.[^cf-ebp2255]

## Competency questions

After reading this concept, you should be able to answer these. The short answers point to the detail above.

- **Why are there browser binds on the LVQ?** Since 3.1.4 the LVQ is read with a queue browser on demand. That is expected.
- **Can `ignoreCheckpointMessageIdComparisonError` default to true to save customers trouble after a DR failover?** No. It would silently skip deduplication for valid messages, so it stays an explicit choice.
- **Why not switch 3.x to Scala 2.13?** Databricks Spark 3.5 runtimes are Scala 2.12, and a 2.13 build fails to load there.
- **Can partitioned-queue support be turned back on with a flag?** No. The read path needs browsing, which partitioned queues do not support.
- **Why should an LVQ permission problem fail startup instead of being logged?** Logging it hid broken checkpointing until the next restart, when recovery failed.

## The non-obvious part

**Fixes that exist are not necessarily released.** The long-running stability fixes for a Databricks support case sit on a branch built from 3.1.7 and are not in 3.2.0 or 4.0.0. When a customer is told "fixed", check which line and version actually contain it. See [Performance and tuning](performance-and-tuning.md).

[^jira-97452]: DATAGO-97452: Move Solace connection to Spark partitions
[^jira-86131]: DATAGO-86131: Change Scala version to 2.12
[^jira-111402]: DATAGO-111402: Idle timeout, repeated operations on one DataFrame, no batch on an empty queue, LVQ browser
[^jira-143720]: DATAGO-143720: Browser binds on the LVQ (support case)
[^jira-149305]: DATAGO-149305: Start-up fails without endpoint permission override (support case)
[^jira-118237]: DATAGO-118237: LVQ non-owner permission and other enhancements
[^cf-release-315]: Spark v3.1.5 release notes
[^jira-128386]: DATAGO-128386: Replication Group Message ID comparison fails during DR failover
[^jira-147415]: DATAGO-147415: Failure loop after DR failover with ackLastProcessedMessages
[^jira-120936]: DATAGO-120936: Disable partition key handling for checkpointing
[^seot-102]: SEOT-102: Partitioned queue support (field request)
[^jira-84486]: DATAGO-84486: Extracting the payload as a string
[^jira-92181]: DATAGO-92181: Solace message replay support
[^jira-89636]: DATAGO-89636: OAuth authentication support
[^jira-142122]: DATAGO-142122: Update to JDK 11 and modernize the 3.x build
[^jira-142067]: DATAGO-142067: Upgrade to Spark 4.0.0 (Java 17, Scala 2.13)
[^jira-147861]: DATAGO-147861: Publish the 4.x line from its own branch
[^cf-ebp2255]: [KA] EBP-2255 - Remove Java 8 and 11 runtime support across JCSMP and JMS/JAKARTA
