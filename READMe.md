# Spark CDM Connector

This repo represents examples to interact with the Spark-CDM-Connector that works with the 0.9 CDM data format.

This repo contains the following items:
1. Databricks notebook showing examples how to read/write CDM data through Spark. In order for the notebook below ro run, the JAR listed below should be installed as a library in your DataBricks cluster
2. The Spark V2 CDM DataSource connnector is published at
 [mvn repo for spark-cdm-connector](https://mvnrepository.com/artifact/com.microsoft.azure/spark-cdm-connector). This driver enables Spark ro read/write CDM data residing in ADLS.
    * It leverage the [CDM Libray](https://github.com/microsoft/CDM) to manipulate the CDM metadata.