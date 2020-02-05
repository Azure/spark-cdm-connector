# Spark CDM Connector

This repo represents examples to interact with the Spark-CDM-Connector that works with the 0.9 CDM data format.

This repo contains the following items:
1. HDI Jupyter notebook showing examples how to read/write CDM data through Spark:
    * Upload this notebook to your HDI (4.0) Spark cluster
    * Configure your account settings to reflect your account settings:
        * Storage account you wish to read/write CDM data to
        * Account keys for the above storage account
        * Container in the storage account to read/write CDM (meta)data to.
        * Name of the entity you wish to write
2. The Spark V2 CDM DataSource connnector is published at
 [mvn repo for spark-cdm-connector](https://mvnrepository.com/artifact/com.microsoft.azure/spark-cdm-connector). This driver enables Spark ro read/write CDM data residing in ADLS.
    * It leverage the [CDM Libray](https://github.com/microsoft/CDM) to manipulate the CDM metadata.
