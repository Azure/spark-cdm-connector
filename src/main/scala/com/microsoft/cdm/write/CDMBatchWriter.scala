package com.microsoft.cdm.write

import java.io.IOException

import com.microsoft.cdm.log.SparkCDMLogger
import com.microsoft.cdm.utils.{CDMDataFormat, CDMDecimalType, CDMModelWriter, CDMTokenProvider, Constants, DataConverter, Messages, SchemaDiffOutput, SerializedABFSHadoopConf}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.{ArrayType, DecimalType, StringType, StructType}
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class CDMBatchWriter(jobId: String, writeMode: SaveMode, schema: StructType, cdmOptions: CDMWriteOptions) extends BatchWrite {

  val logger  = LoggerFactory.getLogger(classOf[CDMBatchWriter])

  val serializedHadoopConf = SerializedABFSHadoopConf.getConfiguration(cdmOptions.storage, cdmOptions.container, cdmOptions.authCreds, cdmOptions.conf)

  val tokenProvider =  if (cdmOptions.authCreds.appId.isEmpty) Some(new CDMTokenProvider(serializedHadoopConf, cdmOptions.storage)) else None

  val cdmModel = new CDMModelWriter(cdmOptions.storage,
                                    cdmOptions.container,
                                    cdmOptions.manifestPath,
                                    cdmOptions.manifestFileName,
                                    cdmOptions.manifestName,
                                    cdmOptions.entity,
                                    cdmOptions.useSubManifest,
                                    cdmOptions.entityDefinition,
                                    cdmOptions.entDefContAndPath,
                                    jobId,
                                    cdmOptions.fileFormatSettings,
                                    cdmOptions.authCreds, tokenProvider,
                                    cdmOptions.overrideConfigPath,
                                    cdmOptions.cdmSource,
                                    cdmOptions.entityDefinitionStorage,
                                    cdmOptions.maxCDMThreads)

  val cdmEntity = cdmModel.entityExists(cdmOptions.entity, serializedHadoopConf)

  /* This list is used track the list of partitions in order to know which files to delete in case of an abort */
  val partitionList = ListBuffer[FileCommitMessage]()

  if (Constants.PRODUCTION && cdmOptions.manifestFileName.equals(Constants.MODEL_JSON))  {
    throw new Exception("Writing model.json is not supported.")
  }

  def compare(cdmSchema: Iterable[Any], schema: StructType, path: ArrayBuffer[String]): SchemaDiffOutput = {
    if(cdmSchema.size != schema.fields.length) return SchemaDiffOutput(false, -1, path)
    val dv = new DataConverter;
    val arr = cdmSchema.toArray
    schema.fields.zipWithIndex.foreach{ case (field, i) =>
      path.append(field.name)
      if (field.dataType.isInstanceOf[StructType]) {
        val diff = compare(arr(i).asInstanceOf[Iterable[Any]], field.dataType.asInstanceOf[StructType], path)
        if(!diff.isSame)  return diff
      }
      else if(field.dataType.isInstanceOf[ArrayType]){
        val arrayElementType = field.dataType.asInstanceOf[ArrayType].elementType
        val diff = compare(arr(i).asInstanceOf[Iterable[Any]], arrayElementType.asInstanceOf[StructType], path)
        if(!diff.isSame)  return diff
      }
      else if (field.dataType.isInstanceOf[DecimalType]) {
        if(arr(i).isInstanceOf[CDMDecimalType]) {
          val cdmDecimal = arr(i).asInstanceOf[CDMDecimalType]
          val sparkDecimal = field.dataType.asInstanceOf[DecimalType]
          if(cdmDecimal.precision != sparkDecimal.precision || cdmDecimal.scale != sparkDecimal.scale) {
            return SchemaDiffOutput(false, i, path)
          }
        }else {
          return SchemaDiffOutput(false, i, path)
        }
      }
      else if (arr(i).equals("Guid")) {
        if (!field.dataType.equals(StringType)) {
          return SchemaDiffOutput(false, i, path)
        }
      }
      else {
        try {
          if (!dv.toSparkType(CDMDataFormat.withName(arr(i).toString), 0, 0).getClass.equals(field.dataType.getClass)) {
            return SchemaDiffOutput(false, i, path)
          }
        }
          /*NoSuchElementException will thrown when toSparkType functions doesn't get the specified CDMDataType. This can happen for array, structs and CDMDecimaltype */
        catch{
          case e : java.util.NoSuchElementException => {
            return SchemaDiffOutput(false, i, path)
          }
        }
      }
      path.remove(path.length - 1) //backtrack
    }
    SchemaDiffOutput(true, -1, path)
  }

  def printPath(path: ArrayBuffer[String]): String = {
    var result = new StringBuilder()
    for (i <- path) {
      result.append(i)
      result.append(" > ")
    }
    if (result.length > 0)  result.setLength(result.length - 3)
    result.toString()
  }

  def isNestedTypePresent() : Boolean = {
    val structFieldtype = schema.fields.find(each => (each.dataType.isInstanceOf[StructType] ||  each.dataType.isInstanceOf[ArrayType])).getOrElse(null)
    if(structFieldtype != null) true else false
  }
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {

    if(isNestedTypePresent() && "csv".equals(cdmOptions.fileFormatSettings.fileFormat)) {
      throw new IllegalArgumentException(Messages.nestedTypesNotSupported)
    }

    var cdmSchema: Iterable[Any] = null
    // Get the time taken to run this block of code in Kusto
    SparkCDMLogger.logEventToKustoForPerf(
      {
        cdmSchema = if (cdmEntity.entityDec != null) {
          if (writeMode == SaveMode.ErrorIfExists) {
            throw new IOException("Entity " + cdmOptions.entity + " exists with SaveMode.ErrorIfExists set")
          } else if (writeMode == SaveMode.Overwrite) {
            if (cdmOptions.entityDefinition == "") {
              // cdm schema will be processed from the dataframe schema in case of overwrite and entity exists
              cdmModel.getCDMSchemaFromStructType(schema)
            } else {
              cdmModel.getCDMSchemaTypesAsSeqFromPredefinedSource(cdmOptions.entityDefinition)
            }
            cdmModel.getCDMSchemaFromStructType(schema)
          } else {
            cdmModel.getCDMSchemaTypesAsSeq(cdmOptions.entity, serializedHadoopConf)
          }
        } else {
          if (cdmOptions.entityDefinition == "") {
            /* create types from scratch*/
            cdmModel.getCDMSchemaFromStructType(schema)
          } else {
            cdmModel.getCDMSchemaTypesAsSeqFromPredefinedSource(cdmOptions.entityDefinition)
          }
        }
      }, this.getClass.getName, Thread.currentThread.getStackTrace()(1).getMethodName, Level.DEBUG, "Get CDM Schema for entityDefinition - " + (if (cdmOptions.entityDefinition.isEmpty)  cdmOptions.entity else cdmOptions.entityDefinition), Some(logger))

    val resultPath = ArrayBuffer[String]()
    resultPath.append(cdmOptions.entity)
    val compareResult = compare(cdmSchema, schema, resultPath)
    val prettyPath =  printPath(compareResult.path)

    compareResult match {
      case SchemaDiffOutput(false, -1, path) => throw new Exception(String.format(Messages.mismatchedSizeSchema, prettyPath))
      case SchemaDiffOutput(false, i, path) => throw new Exception(String.format(Messages.invalidIndexSchema, prettyPath.substring(prettyPath.lastIndexOf(" ") + 1), prettyPath))
      case _ =>
    }

    new CDMDataWriterFactory(cdmOptions.storage,
                             cdmOptions.container,
                             cdmOptions.entity,
                             schema,
                             cdmOptions.manifestPath,
                             cdmOptions.useSubManifest,
                             cdmSchema.toList,

      cdmOptions.dataFolder, cdmOptions.fileFormatSettings, jobId, cdmOptions.compressionCodec, serializedHadoopConf)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    /*
    * The list of partitions to we will write to
    */
    messages.foreach { e =>
      val message = e.asInstanceOf[FileCommitMessage]
      partitionList.append(message)
    }

    /*
     * If we are using managed identities and databricks, verify that the token has not expired. If it has expired,
     * notify the user user with an exception
     */
    if (tokenProvider != None && !tokenProvider.get.isTokenValid()) {
      throw new Exception(Messages.managedIdentitiesDatabricksTimeout)
    }

    if (cdmEntity.entityDec != null) {
      //  val cdmEntity = cdmModel.getEntityDec(entity, serializedHadoopConf)
      writeMode match {
        case SaveMode.ErrorIfExists => throw new Exception("Entity " + cdmOptions.entity + "exists with SaveMode ErrorIf Exists")
        case SaveMode.Overwrite => {
          val fs= serializedHadoopConf.getFileSystem()
          val oldPartitions = cdmModel.getOldPartitions(fs, cdmEntity)   // Gets the old partitions that need to be deleted after overwritten entity is created.
          SparkCDMLogger.logEventToKustoForPerf(
            {
              if(cdmModel.createEntity(schema, partitionList, true, cdmOptions.dataFolder, serializedHadoopConf, cdmEntity) && cdmOptions.useSubManifest) {
                deleteOldSubManifest(fs)
              }
              deleteOldPartitionsFromDisk(cdmOptions.useSubManifest, fs, oldPartitions)
            }, this.getClass.getName, Thread.currentThread.getStackTrace()(1).getMethodName, Level.DEBUG, "SaveMode - Overwwite : delete partition and create entity.." + logInfo, Some(logger))


        }
        case SaveMode.Append => {
          SparkCDMLogger.logEventToKustoForPerf(
            {
              cdmModel.updateEntity(cdmEntity, partitionList, cdmOptions.dataFolder)
            }, this.getClass.getName, Thread.currentThread.getStackTrace()(1).getMethodName, Level.DEBUG, "SaveMode - Append : update entity.." + logInfo, Some(logger))
        }
        case _ => {
          SparkCDMLogger.logEventToKusto(this.getClass.getName, Thread.currentThread.getStackTrace()(1).getMethodName, Level.ERROR, "Other SaveMode." + writeMode, Some(logger))
        }
      }
    } else {
      SparkCDMLogger.logEventToKustoForPerf(
        {
          // Entity does not exist, so create it
          cdmModel.createEntity(schema, partitionList, false, cdmOptions.dataFolder, serializedHadoopConf, cdmEntity)
        }, this.getClass.getName, Thread.currentThread.getStackTrace()(1).getMethodName, Level.DEBUG, "Entity does not exist. Creating entity.." + logInfo, Some(logger))
    }
  }

  def logInfo() : String = {

    return "[Entity : " + cdmOptions.entity +
      ", Manifest Path : " + cdmOptions.storage + cdmOptions.container + cdmOptions.manifestPath +
      ", entDefContAndPath : " + cdmOptions.entDefContAndPath + cdmOptions.entityDefinition +"]";
  }

  // When spark job is aborted, delete the partitions that was created.
  def cleanupOnAbort() = {
    val fs= serializedHadoopConf.getFileSystem()
    var path: Path = null
    for (file <- partitionList) {
      path = new Path(cdmOptions.manifestPath + cdmOptions.entity + "/" + cdmOptions.dataFolder + "/" + file.name + file.extension)
      if (fs.exists(path)) {
        SparkCDMLogger.logEventToKusto(this.getClass.getName,
          Thread.currentThread.getStackTrace()(1).getMethodName,
          Level.ERROR, "CDMDataSourceWriter abort - Deleting partition- " + HadoopOutputFile.fromPath(path, serializedHadoopConf.value).toString,
          Some(logger))
        fs.delete(path)
      }
    }
    // Only in case of overwrite with submanifests
    path = new Path(String.format(Constants.SUBMANIFEST_WITH_OVERWRITTEN_PARTITIONS, cdmOptions.manifestPath + cdmOptions.entity + "/" + cdmOptions.entity))
    if(fs.exists(path)) {
      SparkCDMLogger.logEventToKusto(this.getClass.getName, Thread.currentThread.getStackTrace()(1).getMethodName, Level.ERROR, "CDMDataSourceWriter abort - Deleting submanifest with overwritten partition locations - " + HadoopOutputFile.fromPath(path, serializedHadoopConf.value).toString, Some(logger))
      fs.delete(path)
    }
  }


  def deleteOldPartitionsFromDisk(useSubManifest: Boolean, fs: FileSystem, oldPartitions: ArrayBuffer[Path]) = {
    for( oldPartition <- oldPartitions) {
      SparkCDMLogger.logEventToKusto(this.getClass.getName, Thread.currentThread.getStackTrace()(1).getMethodName, Level.INFO, "deleting partitions from disk-  " +  oldPartition.toString, Some(logger))
      fs.delete(oldPartition)
    }
  }

  def deleteOldSubManifest(fs: FileSystem) = {
    val oldPath = new Path(cdmOptions.manifestPath + cdmOptions.entity + "/" + cdmOptions.entity +".manifest.cdm.json")
    val newPath = new Path(String.format(Constants.SUBMANIFEST_WITH_OVERWRITTEN_PARTITIONS, cdmOptions.manifestPath + cdmOptions.entity + "/" + cdmOptions.entity))
    fs.delete(oldPath)
    SparkCDMLogger.log(Level.INFO, "Renaming " + newPath.getName + " to " + oldPath.getName , logger)
    fs.rename(newPath, oldPath)  // renames <entity>.rename.manifest.cdm.json to <entity>.manifest.cdm.json
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    cleanupOnAbort()
    SparkCDMLogger.logEventToKusto(this.getClass.getName, Thread.currentThread.getStackTrace()(1).getMethodName, Level.ERROR, "CDMBatchWriter abort " + logInfo, Some(logger))

  }
}
