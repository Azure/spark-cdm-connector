package com.microsoft.cdm.write

import java.io._

import com.microsoft.cdm.utils.{CsvParserFactory, DataConverter}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType


/**
 * Writes a single partition of CDM data to a single CSV in ADLSgen2.
 * @param schema schema of the data to write.
 * @param fileWriter Type of file writer CSV/parquet
 * @param dataConverter converter between Spark and CDM data.
 */
class CDMDataWriter( var schema: StructType,
                     var fileWriter: WriterConnector,
                     var dataConverter: DataConverter) extends DataWriter[InternalRow] {

  fileWriter.build(schema)
  /**
   * Called by Spark runtime. Writes a row of data to an in-memory csv file.
   * @param row row of data to write.
   */
  def write(row: InternalRow): Unit = {

    // TODO: periodically dump buffer when it gets to a certain size
    fileWriter.writeRow(row, dataConverter)
  }

  /**
   * Called by Spark runtime when all data has been written. Uploads the in-memory buffer to the output CSV/parquet file.
   * @return commit message specifying location of the output csv/parquet file.
   */
  def commit: WriterCommitMessage = {

    fileWriter.upload

    // Pass the file path back so we can add it as a file to the CDM model
    val path = fileWriter.getPath.stripPrefix("/")
    val name = FilenameUtils.getBaseName(path)
    val extension = FilenameUtils.EXTENSION_SEPARATOR_STR + FilenameUtils.getExtension(path)
    new FileCommitMessage(name=name, fileLocation = path, extension)
  }

  /**
   * Called by spark runtime.
   */
  def abort(): Unit = {
    /* TODO: Closing is not aborting*/
    fileWriter.abort
  }

  override def close(): Unit = {
    fileWriter.close
  }
}
