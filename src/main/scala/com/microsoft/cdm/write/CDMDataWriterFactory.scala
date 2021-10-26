package com.microsoft.cdm.write

import java.net.URLDecoder

import com.microsoft.cdm.utils.{Constants, DataConverter, FileFormatSettings, SparkSerializableConfiguration}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

/**
 * Factory class. Creates a CDMDataWriter instance for a single partition of data to write.
 * @param storage The storage account
 * @param container The container name in the storage account.
 * @param entity The name of the entity that will be written.
 * @param schema Spark schema
 * @param manifestPath path relative to storage and container where manifest will be written
 * @param useSubManifest indicates if subManifest can be used (Boolean)
 * @param cdmSchema cdmSchema
 * @param fileFormatSettings file settings including - header, delimiter, file type [parquet or csv]
 * @param jobId id of the write job.
 * @param compression compression codec name
 * @param serConf Spark serialization configuration
 */
@SerialVersionUID(100L)
class CDMDataWriterFactory(var storage: String,
                           var container: String,
                           var entity: String,
                           var schema: StructType,
                           var manifestPath: String,
                           var useSubManifest: Boolean,
                           var cdmSchema: List[Any],
                           val dataDir: String,
                           var fileFormatSettings: FileFormatSettings,
                           var jobId: String,
                           var compression: CompressionCodecName,
                           var serConf: SparkSerializableConfiguration) extends DataWriterFactory {

  // TODO: error handling. we're basically assuming successful writes. Need to add logic to remove/rewrite files on failure.

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    var path = manifestPath + entity + "/" + dataDir
    path = URLDecoder.decode(path, "UTF-8")
    val fileWriter = fileFormatSettings.fileFormat match{
      case "csv" => {
        val prefix = "https://" + storage + container
        val filename = entity + "-" + jobId + "-" +partitionId + ".csv"
        new CSVWriterConnector( prefix, path + "/" + filename, cdmSchema, serConf, fileFormatSettings)
      }
      case "parquet" => {
        val prefix ="https://" +storage + container
        val filename = entity + "-" + jobId + "-" + partitionId + compression.getExtension + ".parquet"
        new ParquetWriterConnector(prefix, path + "/" + filename, cdmSchema, compression, serConf)
      }
    }
    new CDMDataWriter(schema, fileWriter, new DataConverter())
  }

}
