---
type: concept
title: Running on Databricks
description: Databricks-specific setup - library install and access modes, compute choices, and Unity Catalog Volume checkpoints with a service principal and secret rotation.
status: draft
tags:
  - connector
  - spark
  - databricks
  - checkpointing
sources:
  - id: docs-config
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Configuring-Connection-Details.htm
    title: Connector for Apache Spark - Configuring connection details
  - id: cf-getting-started
    resource: https://sol-jira.atlassian.net/wiki/spaces/EventInteg/pages/6628999206
    title: "Getting Started Guide: Solace PubSub+ Connector for Apache Spark"
  - id: cf-best-practices
    resource: https://sol-jira.atlassian.net/wiki/spaces/TCoE/pages/5670344732
    title: Solace Spark MI Best Practices & Troubleshooting Guide
  - id: cf-kba-driver
    resource: https://sol-jira.atlassian.net/wiki/spaces/SS/pages/6912180244
    title: "KBA: Solace Spark Connector Driver Instability on Long-Running Databricks Structured Streaming Jobs"
  - id: jira-130445
    resource: https://sol-jira.atlassian.net/browse/DATAGO-130445
    title: "DATAGO-130445: Unity Catalog Volume as checkpoint location"
  - id: databricks-allowlist
    resource: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/allowlist
    title: Databricks - Allowlist libraries and init scripts on standard-access compute
  - id: databricks-checkpoints
    resource: https://docs.databricks.com/aws/en/structured-streaming/checkpoints
    title: Databricks - Structured Streaming checkpoints
  - id: databricks-m2m
    resource: https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/oauth-m2m
    title: Azure Databricks - OAuth machine-to-machine authentication
---

# Running on Databricks

Databricks is the main platform for this connector. The only Databricks-specific behaviour in the library is Unity Catalog Volume checkpointing. The rest of this concept is platform setup: installing the library and choosing compute.[^cf-getting-started]

## Installing the library

- Pick the line that matches the runtime: 4.x for DBR 17.3 LTS, 3.x for DBR 15.4 and 16.4 LTS. See [Compatibility and distribution](compatibility-and-distribution.md).
- Install it as a compute-scoped library from Maven with the coordinate `com.solacecoe.connectors:pubsubplus-connector-spark:<version>`.
- **Dedicated (single-user) access mode** works without extra steps. **Standard (shared) access mode** needs the coordinate on the Unity Catalog allowlist, which needs the `MANAGE ALLOWLIST` privilege, normally held by a metastore administrator.[^databricks-allowlist]

## Choosing compute

From the best-practices guide:[^cf-best-practices]

| Choice | Recommendation | Why |
|---|---|---|
| Job compute or all-purpose | Job compute | All-purpose clusters do not end Spark sessions cleanly, so consumer connections are left behind on the broker. The idle timeout helps, but can fail if the JVM is in trouble at shutdown. |
| Spot or on-demand | On-demand | A reclaimed spot node restarts its consumers while the old ones end uncleanly, which causes redelivery and leftover connections. |
| One big cluster or several | Several smaller job clusters, grouped by payload size and rate | A queue with large payloads should get its own cluster. |
| Consumers per executor | At most one per core | Each consumer is a Spark task. |

## Unity Catalog Volume checkpoints

Since 3.1.7, the checkpoint location can be a Unity Catalog Volume, for example `/Volumes/catalog/schema/volume/path`.[^jira-130445]

**Why it needs extra setup.** Each executor writes its consumer's checkpoint file directly. Executors cannot write to Unity Catalog Volumes with ordinary file APIs, and their JVMs have no Databricks credentials. So the connector writes those files through the Databricks SDK, authenticated as a workspace-level service principal (OAuth machine to machine).[^jira-130445][^databricks-m2m]

| Option | Value |
|---|---|
| `sparkRuntimePlatform` | `DATABRICKS` (default). `OTHER` turns the Volume support off. |
| `databricksSecretScope` | The secret scope holding the service principal's credentials |
| `databricksHost` | Name of the secret in that scope that holds the workspace URL |
| `databricksClientId` | Name of the secret that holds the client ID |
| `databricksClientSecret` | Name of the secret that holds the client secret |
| `databricksClientSecretRefreshInterval` | Minutes between re-reads of the client secret. Default 1440. |

Source: docs.solace.com.[^docs-config]

- The options hold **secret names**, not the values. The connector reads the values from the scope.
- The service principal needs access to the Volume path and to the secret scope.
- The Volume options are required only when the platform is Databricks **and** the checkpoint path contains `Volumes`. On Databricks without a Volume checkpoint, set `sparkRuntimePlatform=OTHER`.[^cf-getting-started]
- Each streaming query needs its own checkpoint location.[^databricks-checkpoints]

**Secret rotation.** The connector does not rotate the client secret. An administrator rotates it and updates the scope. The connector re-reads it every `databricksClientSecretRefreshInterval` minutes, without a restart. The refresh must fall after the rotation finishes and before the old secret expires. If the refresh runs before rotation completes, it keeps the old secret until the next refresh.[^docs-config]

## Competency questions

After reading this concept, you should be able to answer these. The short answers point to the detail above.

- **A shared (standard access mode) cluster cannot install the library. Why?** The Maven coordinate must be on the Unity Catalog allowlist, which a metastore administrator manages.
- **Should a production stream run on an all-purpose cluster?** No. Use job compute: all-purpose clusters leave consumer connections behind on the broker.
- **Can the checkpoint be a Unity Catalog Volume?** Yes, from 3.1.7, with a service principal whose credentials are in a Databricks secret scope.
- **Do the `databricks*` options take the client ID and secret themselves?** No. They take the names of secrets in `databricksSecretScope`.
- **Does rotating the service principal's secret need a restart?** No. The connector re-reads it every `databricksClientSecretRefreshInterval` minutes (default 1440).
- **A non-Databricks deployment fails because of Databricks options. What should it set?** `sparkRuntimePlatform=OTHER`.

## The non-obvious part

**`sparkRuntimePlatform` defaults to `DATABRICKS`.** On vanilla Spark this is harmless unless the checkpoint path happens to contain the text `Volumes`, which is how a Volume path is detected. In that case, set `sparkRuntimePlatform=OTHER`.

**Version 3.1.6 cannot checkpoint to a Volume.** It fails to create its offset directory on the `/dbfs/Volumes` mount. Upgrade to 3.1.7 or later, or use the DBFS root on 3.1.6.[^cf-kba-driver]

**Every executor needs network access to the broker, not just the driver.** Consumers connect from the executors, so the worker nodes must reach the broker's SMF port (55555, or 55443 for TLS).[^cf-getting-started]

**Uninstalling a library takes effect only after a cluster restart.** Remove the old version, restart, and then install the new one, or both versions load and conflict.[^cf-getting-started]

[^docs-config]: Connector for Apache Spark - Configuring connection details
[^cf-getting-started]: Getting Started Guide: Solace PubSub+ Connector for Apache Spark
[^cf-best-practices]: Solace Spark MI Best Practices & Troubleshooting Guide
[^cf-kba-driver]: KBA: Solace Spark Connector Driver Instability on Long-Running Databricks Structured Streaming Jobs
[^jira-130445]: DATAGO-130445: Unity Catalog Volume as checkpoint location
[^databricks-allowlist]: Databricks - Allowlist libraries and init scripts on standard-access compute
[^databricks-checkpoints]: Databricks - Structured Streaming checkpoints
[^databricks-m2m]: Azure Databricks - OAuth machine-to-machine authentication
