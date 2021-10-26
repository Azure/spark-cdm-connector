package com.microsoft.cdm.read

import com.microsoft.cdm.utils.DataConverter
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

/**
 * Factory class for creating a CDMDataReader responsible for reading a single partition of CDM data.
 * @param remoteCSVPath ADLSgen2 URI of partition data in CSV format.
 * @param schema Spark schema of the data in the CSV file
 * @param adlProvider Provider for ADLSgen2 data
 * @param dataConverter Converts CSV data into types according to schema
 */
case class CDMInputPartition(val storage: String,
                        val container: String,
                        val fileReader: ReaderConnector,
                        val header: Boolean,
                        var schema: StructType,
                        var dataConverter: DataConverter,
                        val mode: String) extends InputPartition {
}