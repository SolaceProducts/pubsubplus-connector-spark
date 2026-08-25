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

The connector is available in maven central as [pubsubplus-connector-spark](https://mvnrepository.com/artifact/com.solacecoe.connectors/pubsubplus-connector-spark)

# Build the connector

`mvn clean install` to build connector with integration tests
`mvn clean install -DskipTests ` to build connector without integration tests.

# Exploring the Code using IDE

## Using Intellij IDEA

Import the project into IntelliJ IDEA and all the maven commands are enabled automatically. Refer Build the connector section for next steps

# Authors

See the list of contributors who participated in this project.

 




