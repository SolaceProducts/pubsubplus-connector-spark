# Getting Started
## Solace Spark Connector
Solace Spark Connector is based on Spark DataSourceV2 API provided in Spark. 

The "Getting Started" tutorials will get you up to speed and sending messages with Solace technology as quickly as possible. There are three ways you can get started:

* Follow [these instructions](https://cloud.solace.com/learn/group_getting_started/ggs_signup.html) to quickly spin up a cloud-based Solace messaging service for your applications.
* Follow [these instructions](https://docs.solace.com/Solace-SW-Broker-Set-Up/Setting-Up-SW-Brokers.htm) to start the Solace VMR in leading Clouds, Container Platforms or Hypervisors. The tutorials outline where to download and how to install the Solace VMR.
* If your company has Solace message routers deployed, contact your middleware team to obtain the host name or IP address of a Solace message router to test against, a username and password to access it, and a VPN in which you can produce and consume messages.

# Contents

This repository contains code that can stream messages from Solace to Spark Internal row and also publish messages to Solace from Spark.

## User Guide

For complete user guide, please check the connector page on [Solace Integration Hub](https://solace.com/integration-hub/apache-spark/).

## Maven Central

The connector is available in maven central as [solace-connector-spark-4](https://mvnrepository.com/artifact/com.solace/solace-connector-spark-4)

# Build the connector

The connector requires **JDK 17 or later** to build and to run. It targets Java 17 bytecode
(`maven.compiler.release=17`) and is built against Apache Spark 4.0.0 / Scala 2.13.

> **Upgrading from the 3.x line?** Spark 4.0.0 requires Java 17 and Scala 2.13 — Java 8/11 and
> Scala 2.12 were both dropped upstream, so all three move together. See
> [Behavior and compatibility changes in 4.x](#behavior-and-compatibility-changes-in-4x) below.
> The 3.x maintenance line (Spark 3.5.x / Scala 2.12 / JDK 11) continues on its own branch.

`mvn clean install` to build connector with integration tests
`mvn clean install -DskipTests ` to build connector without integration tests.

# Behavior and compatibility changes in 4.x

| Change | Impact |
|---|---|
| **Java 17 required** | Spark 4.0.0 dropped JDK 8 and 11 (`SPARK-45315`). The connector now emits Java 17 bytecode and will not load on an older JVM. |
| **Scala 2.13 required** | Spark 4.0.0 dropped Scala 2.12 (`SPARK-45314`). Your cluster's Scala version must match; a 2.12 runtime cannot load this build. |
| **Spark 4.0.0 baseline** | Built and tested against Spark 4.0.0 (bundles Hadoop 3.4.1, Scala 2.13.16). On Databricks the target runtime is **DBR 17.3 LTS** (Spark 4.0.0, Scala 2.13); DBR 15.4/16.4 LTS ship Spark 3.5.x + Scala 2.12 and cannot load a 4.x build. |
| **The jar no longer bundles Spark** | Spark, Scala, Hadoop and the logging backend are now `provided` rather than packaged (313.8 MB → 50.0 MB). No action for the documented Databricks install paths; if you embed the jar *outside* a Spark runtime you must supply Spark yourself. |
| **Logging goes through SLF4J only** | The connector no longer references Log4j 2 directly and packages no logging library. It binds to whatever backend the runtime provides. |
| **ANSI SQL mode is ON by default** | This is a Spark 4 default change, not a connector change, but it affects any pipeline reading from or writing to this connector. Casting, arithmetic overflow, and division by zero now raise errors instead of silently producing `NULL`. Restore the old behavior with `spark.sql.ansi.enabled=false` if a pipeline depends on it. See the [Spark 4.0.0 release notes](https://spark.apache.org/releases/spark-release-4-0-0.html). |
| **OAuth token errors report more detail** | The connector now sends `Accept: application/json` on token requests and, when the authorization server returns something that is not a parseable OAuth error, reports the HTTP status and response body instead of the literal text `null`. Messages for well-formed OAuth errors are unchanged. |

No connector configuration options were added, removed, or renamed in 4.x, and no read/write
behavior changed — the platform baseline moved, the connector's API surface did not.

## Running the integration tests on Windows

Most integration tests run Spark inside Docker containers, but the configuration-validation
suite (`SolaceSparkValidationIT`) starts a local `SparkSession` in the test JVM so it doesn't pay
a `spark-submit` startup per test. On Windows,
Spark's checkpointing goes through Hadoop, which needs the Hadoop Windows native helpers:

1. Obtain `winutils.exe` and `hadoop.dll` for Hadoop 3.4.x (Spark 4.0.0 bundles Hadoop 3.4.1).
   There is no official Apache Windows build; the commonly used community source is
   https://github.com/cdarlint/winutils
2. Place them in a `bin` directory, e.g. `C:\hadoop\bin\winutils.exe`.
3. Set `HADOOP_HOME` to the **parent** of `bin` (e.g. `C:\hadoop`) — Hadoop appends `\bin\winutils.exe`.
4. Copy `hadoop.dll` into `C:\Windows\System32` as well; some code paths load it from the default
   library path rather than from `HADOOP_HOME`.

Without this, that suite fails with
`java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset`.

Linux and CI need no extra setup. Docker must be running for all integration tests.

# Exploring the Code using IDE

## Using Intellij IDEA

Import the project into IntelliJ IDEA and all the maven commands are enabled automatically. Refer Build the connector section for next steps

# Authors

See the list of contributors who participated in this project.

 




