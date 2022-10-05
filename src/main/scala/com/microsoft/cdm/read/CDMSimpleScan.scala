package com.microsoft.cdm.read

import com.microsoft.cdm.log.SparkCDMLogger
import com.microsoft.cdm.utils.{AuthCredential, CDMModelReader, CDMSource, CDMTokenProvider, Constants, DataConverter, Messages, SerializedABFSHadoopConf, SparkSerializableConfiguration}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.net.URLDecoder
import com.microsoft.commondatamodel.objectmodel.cdm.CdmTraitReference
import scala.collection.JavaConverters._


case class CDMPartition(partitionNumber: Int, header: Boolean=true) extends InputPartition

class CDMSimpleScan(val storage: String,
                    val container: String,
                    val manifestPath: String,
                    val manifestFileName: String,
                    val entityName: String,
                    val entDefContAndPath: String,
                    val authCredential: AuthCredential,
                    val conf:Configuration,
                    val dataConverter: DataConverter,
                    val cdmSource: CDMSource.Value,
                    val entityDefinitionStorage: String,
                    val maxCDMThreads: Int,
                    val mode: String) extends Scan with Batch{

  val logger  = LoggerFactory.getLogger(classOf[CDMSimpleScan])

 val serializedHadoopOConf= SerializedABFSHadoopConf.getConfiguration(storage, container, authCredential, conf)

  val tokenProvider =  if (authCredential.appId.isEmpty) Some(new CDMTokenProvider(serializedHadoopOConf, storage)) else None

  val cdmModel = new CDMModelReader(storage, container, manifestPath, manifestFileName, entityName, entDefContAndPath, authCredential, tokenProvider, cdmSource, entityDefinitionStorage,
    maxCDMThreads)

  //TODO: Make this a accessor class to retrieve tuple items
  var entity = cdmModel.entityDecHandleError(entityName, serializedHadoopOConf)

  override def readSchema() = {
    SparkCDMLogger.logEventToKustoForPerf({
      cdmModel.getSchema(entity.parentManifest, entity.entityDec)
    },this.getClass.getName, Thread.currentThread.getStackTrace()(1).getMethodName, Level.DEBUG, "Reading CDM entity and convert it to spark schema", Some(logger))
  }

  override def toBatch: Batch = this

  def getReader(fType: String, uriPath: String, filePath: String, schema: StructType, serializedHadoopConf: SparkSerializableConfiguration, delimiter: Char, hasHeader: Boolean): ReaderConnector ={
    return fType match {
      case   "is.partition.format.CSV" => new CSVReaderConnector(uriPath, filePath, serializedHadoopConf, delimiter, mode, schema, hasHeader)
      case   "is.partition.format.parquet" => new ParquetReaderConnector(uriPath, filePath, schema, serializedHadoopConf)
    }
  }
  override def planInputPartitions(): Array[InputPartition] = {
    /* Fetch the partitions and their names from the CDMModel*/
    val factoryList = new java.util.ArrayList[InputPartition]
    val man = entity.parentManifest
    val eDec = entity.entityDec

    // Calling fileStatusCheckAsync() on model.json breaks because the CDM library
    // assumes a "entity".cdm.json file exists
    if (manifestFileName != Constants.MODEL_JSON) {
      entity.entityDec.fileStatusCheckAsync().get()
    }

    for(partition <- eDec.getDataPartitions.asScala) {
      // the relative location of the path
      val loc = cdmModel.getRelPath(partition.getLocation)
      assert(!loc.startsWith("https:/"))
      val absPath = cdmModel.cdmCorpus.getStorage.createAbsoluteCorpusPath(loc,eDec)
      //The full path to data partition with the adapter stripped off
      val relPath= cdmModel.getRelPath(absPath)

      val uriPrefix = "https://"+storage+container

      // Decode strings because hadoop cannot parse URI-encoded strings
      val decodedFilePath = URLDecoder.decode(manifestPath + relPath, "UTF-8")

      //we track hasHeader and pass it in to the reader so that we know if the first line is a header row
      var hasHeader = false
      val schema = readSchema();
      var delimiter = Constants.DEFAULT_DELIMITER
      val fileReader = {

        if (partition.getExhibitsTraits.asScala.size > 0 &&
          partition.getExhibitsTraits.asScala.find(_.getNamedReference.startsWith("is.partition.format")) != None) {
          val traits = partition.getExhibitsTraits.asScala.find(_.getNamedReference.startsWith("is.partition.format")).get
          assert(traits.getNamedReference == "is.partition.format.CSV" ||
            traits.getNamedReference == "is.partition.format.parquet")
          // if arguments are defined, determine whether the the files have headers prepended to them
          val arguments = traits.asInstanceOf[CdmTraitReference].getArguments().asScala
          val headerArg = arguments.find(_.getName() == "columnHeaders")
          if (headerArg != None) {
            hasHeader = headerArg.get.getValue().toString.toBoolean
          }
          val delimiterArg = arguments.find(_.getName() == "delimiter")
          if (delimiterArg != None) {
            val strDelimiter = delimiterArg.get.getValue.toString;
            if(strDelimiter.length > 1) throw new IllegalArgumentException(String.format(Messages.invalidDelimiterCharacter, strDelimiter))
            delimiter = strDelimiter.charAt(0)
          }
          val reader = getReader(traits.getNamedReference, uriPrefix, decodedFilePath, schema, serializedHadoopOConf, delimiter, hasHeader)
          if (reader.isInstanceOf[ParquetReaderConnector] && Constants.PERMISSIVE.equalsIgnoreCase(mode)) {
            throw new IllegalArgumentException(String.format(Messages.invalidPermissiveMode))
          }
          reader
        } else {
          SparkCDMLogger.log(Level.DEBUG, "No Named Reference Trait \"is.partition.format\" (CSV/Parquet", logger)
          new CSVReaderConnector(uriPrefix, decodedFilePath, serializedHadoopOConf, delimiter, mode, schema, hasHeader)
        }
      }

      factoryList.add(new CDMInputPartition(storage, container, fileReader, hasHeader, schema, dataConverter, mode))
    }
    SparkCDMLogger.log(Level.DEBUG, "Count of partitions - "+eDec.getDataPartitions.size() + " Entity - " + eDec.getEntityName + " Manifest -"+ man.getManifestName, logger)
    factoryList.asScala.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new CDMPartitionReaderFactory( )
  }
}
