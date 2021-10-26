package com.microsoft.cdm.read

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

class CDMPartitionReader(inputPartition: CDMInputPartition) extends PartitionReader[InternalRow]{

  var index = 0
  val values = Array("1", "2", "3", "4", "5")

  var iterator: Iterator[String] = null

  @transient
  def next: Boolean = index < values.length

  def get = {
    val stringValue = values(index)
    val stringUtf = UTF8String.fromString(stringValue)
    val row = InternalRow(stringUtf)
    index = index + 1
    row
  }

  def close() = Unit

}
