# Getting Started
## Solace Spark Connector
Solace Spark Connector is based on DataSourceV2 API provided in Spark 3. The following are good places to start

* https://levelup.gitconnected.com/easy-guide-to-create-a-custom-read-data-source-in-apache-spark-3-194afdc9627a

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

Java

# Build the connector

`mvn clean install`

# Running the connector

## Local environment

Navigate to the file `LoadSolaceConnector.java` and update the options to specify the relevant Solace Service details(Host, VPN, User, Password, Queue, BatchSize) and run the file.

## Databricks environment

The following are the prerequisites to run the connector in DataBricks environment

* Runtime 9.1 LTS (includes Apache Spark 3.1.2, Scala 2.12)

Steps to run the connector

* Create a cluster in Databricks using the specified Runtime
* Upload the Jar file generated from `mvn clean install` command in the libraries section
* Add `com.solacesystems:sol-jcsmp:version and log4j:log4j:version` as maven dependency
* Create a notebook with the code required to run the connector and attach it to cluster. Refer `LoadSolaceConnector.java` for the code to run the connector(Notebooks are generally written in scala)
* Run the notebook and you should be able to see the connector reading data from Solace and inserting into Spark Internal rows

# Exploring the Code using IDE

## Using Intellij IDEA

Import the project into IntelliJ IDEA and all the maven commands are enabled automatically. Refer Build the connector section for next steps

# Authors

See the list of contributors who participated in this project.

 




