package com.microsoft.cdm.write

import com.microsoft.cdm.utils.{CDMOptions, Constants}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsOverwrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

 class CDMWriterBuilder(queryId: String, schema: StructType, writeMode: SaveMode, options: CDMWriteOptions) extends WriteBuilder
  with SupportsOverwrite
  with SupportsTruncate {

   Constants.MODE = "write"

   override def buildForBatch(): BatchWrite = new CDMBatchWriter(queryId, writeMode, schema, options)

   override def overwrite(filters: Array[Filter]): WriteBuilder = new CDMWriterBuilder(queryId, schema, SaveMode.Overwrite, options)

 }

