package com.microsoft.cdm.read

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class CDMPartitionReaderFactory() extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[CDMInputPartition]
    new CDMDataReader(p.storage,
      p.container,
      p.fileReader,
      p.header,
      p.schema,
      p.dataConverter,
      p.mode)
  }
}
