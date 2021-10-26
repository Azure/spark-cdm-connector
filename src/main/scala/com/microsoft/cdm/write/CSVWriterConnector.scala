package com.microsoft.cdm.write

import java.io.OutputStreamWriter

import com.microsoft.cdm.log.SparkCDMLogger
import com.microsoft.cdm.utils.{CsvParserFactory, DataConverter, FileFormatSettings, SparkSerializableConfiguration}
import com.univocity.parsers.csv.CsvWriter
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.PositionOutputStream
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

import scala.collection.JavaConversions

class CSVWriterConnector(prefix: String,
                         filePath: String,
                         cdmSchema: List[Any],
                         var serConf:SparkSerializableConfiguration,
                         fileFormatSettings: FileFormatSettings) extends WriterConnector {
  val logger  = LoggerFactory.getLogger(classOf[CSVWriterConnector])
  private var stream:PositionOutputStream = _
  private var writer:CsvWriter = _
  private var schema: StructType = _
  private val httpPath= prefix + filePath
  SparkCDMLogger.log(Level.INFO, "CSV Writer for partition at path: " + prefix + filePath, logger)

  def getPath(): String = httpPath

  def build(inSchema: StructType): Unit = {
    try {
      schema = inSchema
      val path = new Path(filePath)
      val fs = path.getFileSystem(serConf.value)
      val oFile = HadoopOutputFile.fromPath(path, serConf.value)
      stream = oFile.create(fs.getDefaultBlockSize(path))
      writer = CsvParserFactory.buildWriter(new OutputStreamWriter(stream), fileFormatSettings.delimiter)
      if(fileFormatSettings.showHeader) {
        val headers = schema.fields.map(_.name)
        writer.writeHeaders(headers: _*)
      }
    } catch {
      case e: Throwable => SparkCDMLogger.log(Level.ERROR, e.printStackTrace.toString, logger)
    }
  }

  def upload() = {
    writer.close()
  }


  def writeRow(row: InternalRow, dataConverter: DataConverter): Unit =   {
    val strings = JavaConversions.seqAsJavaList(row.toSeq(schema).zipWithIndex.map{ case(col, index) =>
      dataConverter.dataToString(col, schema.fields(index).dataType, cdmSchema(index))
    })

    var strArray = new Array[String](strings.size)
    strArray = strings.toArray(strArray)
    writer.writeRow(strArray)
  }

  def abort(): Unit = {
    SparkCDMLogger.log(Level.ERROR, "CSV Writer aborting.." + prefix + filePath, logger)
    writer.close()
  }

  def close (): Unit = {
    writer.close()
  }
}
