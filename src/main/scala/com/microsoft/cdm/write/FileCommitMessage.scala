package com.microsoft.cdm.write

import org.apache.spark.sql.connector.write.WriterCommitMessage

// TODO: there's a better scala idiom for this than class
/**
 * Commit message returned from CDMDataWriter on successful write. One for each partition gets returned to
 * CDMDataSourceWriter.
 * @param name name of the partition.
 * @param fileLocation output csv/parquet file for the partition.
 */
class FileCommitMessage(val name: String, val fileLocation: String, val extension: String) extends WriterCommitMessage {
  name + fileLocation

  // return the partition name
  def getPartition(): String = {
    name
  }
}
