package com.microsoft.cdm

import java.util

import com.microsoft.cdm.read.{CDMReadOptions, CDMScanBuilder}
import com.microsoft.cdm.write.{CDMWriteOptions, CDMWriterBuilder}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class SparkTable(schema: StructType, options: CaseInsensitiveStringMap) extends Table
  with SupportsRead
  with SupportsWrite {


  override def name(): String = this.getClass.toString

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.BATCH_WRITE,
    TableCapability.BATCH_READ,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC,
    TableCapability.TRUNCATE).asJava


  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new CDMScanBuilder(new CDMReadOptions(options))
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new CDMWriterBuilder(info.queryId(), info.schema(), SaveMode.Append, new CDMWriteOptions(options))
  }
}