package com.microsoft.cdm.read


import com.microsoft.cdm.utils.{Constants, DataConverter, Messages}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import java.io.{File, FileOutputStream, InputStream}

/**
 * Reads a single partition of CDM data.
 * @param csvPath ADLSgen2 URI of partition data in CSV format.
 * @param schema Spark schema of the data in the CSV file
 *
 */

@SerialVersionUID(100L)
class CDMDataReader(val storage: String,
                    val container: String,
                    val fileReader: ReaderConnector,
                    val hasHeader: Boolean,
                    var schema: StructType,
                    var dataConverter: DataConverter,
                    val mode: String) extends PartitionReader[InternalRow] with Serializable {
  var cnt = 0
  var row: Array[Any] = _
  var stream: InputStream =_
  var headerRead = false
  fileReader.build
  /**
   * Called by the Spark runtime.
   * @return Boolean indicating whether there is any data left to read.
   */
  def next: Boolean = {
    if (hasHeader && !headerRead) {
      fileReader.readRow

      //TODO: verify header names match with what we have in CDM
      // println("TODO: Verify header names match")
      headerRead = true
    }

    row = fileReader.readRow
    row != null
  }

  def createTempFile(in: InputStream): File = {
    val tempFile = File.createTempFile("data", ".parquet")
    tempFile.deleteOnExit()
    val out = new FileOutputStream(tempFile)
    try IOUtils.copy(in, out)
    finally if (out != null) out.close()
    tempFile
  }
  /**
   * Called by the Spark runtime if there is data left to read.
   * @return The next row of data.
   */
  def get: InternalRow = {
    if(mode == Constants.FAILFAST && row.length != schema.fields.length) {
      throw new Exception(Messages.incompatibleFileWithDataframe)
    }
    var seq: Seq[Any] = null
    var setRowToNull = false
    // When there are more columns in the CSV file than the # of attributes in the cdm entity file.
    if (row.length > schema.fields.length) {
      seq = schema.zipWithIndex.map{ case (col, index) =>
        val dataType = schema.fields(index).dataType
        fileReader.jsonToData(dataType, row.apply(index), index, mode)
      }
    } else if (row.length < schema.fields.length) {
      // When there are fewer columns in the CSV file the # of attributes in cdm entity file at the end
      seq = schema.zipWithIndex.map{ case (col, index) =>
        if (index >= row.length) {
          null
        } else {
          val dataType = schema.fields(index).dataType
          fileReader.jsonToData(dataType, row.apply(index), index, mode)
        }
      }
    } else {
      seq = row.zipWithIndex.map { case (col, index) =>
        val dataType = schema.fields(index).dataType
        fileReader.jsonToData(dataType, row.apply(index), index, mode)
      }
    }

    /*
     * If we want to return null for the entire row if any entity fails to be converted, uncomment
     */
    /*
    if (!fileReader.isValidRow) {
      seq = schema.zipWithIndex.map { _ =>null}
    }
  */

    InternalRow.fromSeq(seq)
  }

      /**
   * Called by the Spark runtime.
   */
  def close(): Unit = {
    fileReader.close
  }

}
