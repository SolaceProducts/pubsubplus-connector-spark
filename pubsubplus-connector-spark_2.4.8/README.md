# Getting Started
## Solace Spark Connector
Solace Spark Connector is based on DataSourceV2 API provided in Spark.

The "Getting Started" tutorials will get you up to speed and sending messages with Solace technology as quickly as possible. There are three ways you can get started:

* Follow [these instructions](https://cloud.solace.com/learn/group_getting_started/ggs_signup.html) to quickly spin up a cloud-based Solace messaging service for your applications.
* Follow [these instructions](https://docs.solace.com/Solace-SW-Broker-Set-Up/Setting-Up-SW-Brokers.htm) to start the Solace VMR in leading Clouds, Container Platforms or Hypervisors. The tutorials outline where to download and how to install the Solace VMR.
* If your company has Solace message routers deployed, contact your middleware team to obtain the host name or IP address of a Solace message router to test against, a username and password to access it, and a VPN in which you can produce and consume messages.

# Contents

This repository contains code that connects to specified Solace service and inserts data into Spark Internal row. Please note that this connector only support Spark read operation.

## Features

* The current connector enables user to configure Solace service details as required
* It supports processing of specific number of messages in a partition(BatchSize option can be used to configure the number of messages per partition)
* Message are acknowledged to Solace once commit method is triggered from Spark. Commit is triggered from Spark on successful write operation.
* Message IDs are persisted in checkpoint location once commit is successfull. This helps in deduplication of messages and guaranteed processing.

## Architecture

<p align="center">
   <img width="700" height="400" src="https://user-images.githubusercontent.com/83568543/154064846-c1b5025f-9898-4190-90b5-84f05bd7fba9.png"/>
</p>

# Prerequisites

Apache Spark 2.4, Scala 2.11

# Build the connector

`mvn clean install`

# Running the connector

## Databricks environment

Create a Databricks cluster with Runtime with above Spark version and follow the steps below

1. In the libraries section upload the pubsubplus-connector-spark jar and also add the following maven dependencies using install new option available in Databricks cluster libraries section

<p align="center">
   <img src="https://github.com/SolaceTechCOE/spark-connector-v2/assets/83568543/b2854793-a297-4ffa-b67f-f7986342aea8"/>
</p>


## Running as a Job
If you are running Spark Connector as a job, use the jar files provided as part of distribution to configure your job. In case of thin jar you need to provide dependencies as in above screenshot for the job. For class name and other connector configuration please refer sample scripts and configuration option sections

## Thin Jar vs Fat Jar
Thin jar is light weight jar where only Spark Streaming API related dependencies are included. In this case Solace and Log4j related dependencies should be added during configuration of Spark Job. Spark supports adding these dependencies via maven or actual jars.

```com.solacesystems:sol-jcsmp:10.21.0``` & ```log4j:log4j:1.2.17```

Fat jar includes all the dependencies and it can be used directly without any additional dependencies.

## Solace Spark Schema

Solace Spark Connector transforms the incoming message to Spark row with below schema.

| Column Name  | Column Type |
| ------------- | ------------- |
| Id  | String  |
| Payload  | Binary  |
| Topic  | String  |
| TimeStamp  | Timestamp  |
| Headers  | Map<string, binary>  |

## Using Sample Script

### Databricks

1.	Solace_Read_Stream_Script.txt

Create a new notebook in Databricks environment and set the language to Scala. Copy the Script to notebook and provide the required details. Once started, script reads data from Solace Queue and writes to parquet files. 

2.	Read_Parquet_Script.txt
This scripts reads the data from parquet and displays the count of records. The output should match the number of records available in Solace queue before processing.

## Spark Job or Spark Submit

Spark Job or Spark Submit requires jar as input. You can convert above Scala scripts to jar and provide it as input and add pubsubplus-connector-solace jar(thin or fat) as dependency. In case of thin jar please note that additional dependencies need to be configured as mentioned in Thin Jar vs Fat Jar section.

## Configuration Options
| Config Option  | Type | Valid Values | Default Value | Description |
| ------------- | ------------- | ------------- | ------------- | ------------- |
| host  | String  | tcp(s)://hostname:port  | Empty  | Fully Qualified Solace Hostname with protocol and port number  |
| vpn  | String  |   | Empty  | Solace VPN Name  |
| username | String |  | Empty | Solace Client Username |
| password | String |  | Empty | Solace Client Password |
| queue | String |  | Empty | Solace Queue name |
| batchSize | Integer |  | 1 | Set number of messages to be processed in batch. Default is set to 1 |
| ackLastProcessedMessages | Boolean |  | false | Set this value to true if connector needs to determine processed messages in last run. The connector purely depends on offset file generated during Spark commit. In some cases connector may acknowledge the message based on offset but same may not be available in your downstream systems. In general we recommend leaving this false and handle process/reprocess of messages in downstream systems | 
| skipDuplicates | Boolean |  | false | Set this value to true if connector needs check for duplicates before adding to Spark row. This scenario occurs when the tasks are executing more than expected time and message is not acknowledged before the start of next task. In such cases the message will be added again to Spark row. |
| offsetIndicator | String |  | MESSAGE_ID | Set this value if your Solace Message has unique ID in message header. Supported Values are <ul><li> MESSAGE_ID </li><li> CORRELATION_ID </li> <li> APPLICATION_MESSAGE_ID </li> <li> CUSTOM_USER_PROPERTY </li> </ul> CUSTOM_USER_PROPERTY refers to one of headers in user properties header |
| includeHeaders | Boolean |  | false | Set this value to true if message headers need to be included in output |


# Exploring the Code using IDE

## Using Intellij IDEA

Import the project into IntelliJ IDEA and all the maven commands are enabled automatically. Refer Build the connector section for next steps

# Authors

See the list of contributors who participated in this project.

 




