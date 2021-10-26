package com.microsoft.cdm.write

import com.microsoft.cdm.utils.DataConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

@SerialVersionUID(100L)
trait WriterConnector extends Serializable {
  def getPath: String

  def build(schema: StructType)

  def writeRow(row: InternalRow, dataConverter: DataConverter)

  def upload

  def abort

  def close
}
