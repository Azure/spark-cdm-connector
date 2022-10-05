package com.microsoft.cdm.read

import java.text.SimpleDateFormat
import java.util.TimeZone
import com.microsoft.cdm.utils.Constants
import com.microsoft.cdm.utils.TimestampFormatter
import org.apache.spark.sql.types.DataType

@SerialVersionUID(100L)
trait ReaderConnector extends Serializable {

  val dateFormatter = new SimpleDateFormat(Constants.SINGLE_DATE_FORMAT)
  val timestampFormatter = TimestampFormatter(Constants.TIMESTAMP_FORMAT, TimeZone.getDefault)

  /**
   * build() is used as a constructor, to initialize local variables
   */
  def build

  /**
   * Close any open streams if they exist
   */
  def close

  /**
   * This method is to used to convert to Spark/CDM data types
   * @param dataType
   * @param col
   * @param schemaIndex Used in CSVReaderConnector for schema mapping. In ParquetReaderConnector, it has no functional purpose other than keeping the same interface.
   * @return
   */
  def jsonToData(dataType: DataType, col: Any, schemaIndex: Int, mode: String): Any


  def isValidRow(): Boolean

  /**
   * Read a Row as a string.
   * XXX: This is not sufficient for complex types
   */
  def readRow: Array[Any]
}
