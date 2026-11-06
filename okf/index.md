---
okf_version: "0.2"
---

# Spark Connector

Knowledge spoke for the Spark Connector (repository `pubsubplus-connector-spark`): a Spark DataSource V2 library that Spark jobs use to consume from Solace queues and publish to Solace topics. It is a standalone library that runs inside the customer's Spark application, so this spoke covers it end to end. Customer-facing setup lives on [docs.solace.com](https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Overview.htm).

# Start here

- [Overview](overview.md) - What the Spark Connector is, which directions it supports, where it runs, and what this spoke covers.

# Concepts

- [Compatibility and distribution](compatibility-and-distribution.md) - The two release lines, which Spark, Scala, Java and Databricks runtime each supports, and how the library is published and installed.
- [Configuration](configuration.md) - The options the source and sink accept, their defaults and validation, and what the connector sets on the Solace session regardless of options.
- [Read path](read-path.md) - How the source turns a Solace queue into Spark micro-batches - consumers, batch size, the row schema, headers, empty queues, and idle connections.
- [Checkpointing and acknowledgment](checkpointing-and-acknowledgment.md) - When source messages are acknowledged, how processed message IDs are checkpointed in the Spark checkpoint and a Solace LVQ, what restart, replay and DR failover do, and why delivery is at least once.
- [Write path](write-path.md) - How the sink publishes Spark rows to Solace topics - required columns, topic choice, delivery mode, publish acknowledgments and the timeout, and streaming versus foreachBatch.
- [Authentication](authentication.md) - How the connector authenticates to the Solace broker - basic, client certificate, OAuth client credentials or token file - and how credentials are kept out of notebooks.
- [Running on Databricks](running-on-databricks.md) - Databricks-specific setup - library install and access modes, compute choices, and Unity Catalog Volume checkpoints with a service principal and secret rotation.
- [Performance and tuning](performance-and-tuning.md) - Measured throughput, the settings that move it (batch size, consumers, unacknowledged-message window, the Id column, TCP buffers), and known stability issues on long-running jobs.

# Working on it

- [Testing](testing.md) - How the connector is tested - integration suites against a real broker and Spark in containers, the local validation suite, what runs in CI, and what needs a real Databricks workspace.
- [Regression guards](regression-guards.md) - Past decisions with their tickets and reasons, and what a future change must not undo.
