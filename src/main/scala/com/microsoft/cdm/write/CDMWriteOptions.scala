package com.microsoft.cdm.write

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.microsoft.cdm.utils.{CDMDataFolder, CDMOptions, Constants, FileFormatSettings, Messages}
import sys.process._
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CDMWriteOptions(options: CaseInsensitiveStringMap) extends CDMOptions(options) {

  Constants.MODE = "write"
  val manifestName = if(options.containsKey("manifestName")) options.get("manifestName") else "default"
  val useSubManifest = if(options.containsKey("useSubManifest")) options.get("useSubManifest").toBoolean else false
  val entDefIn = if (options.containsKey("entityDefinitionPath")) options.get("entityDefinitionPath") else ""
  val useCdmStandard = if (options.containsKey("useCdmStandardModelRoot")) options.get("useCdmStandardModelRoot").toBoolean else false
  val entDefModelRootIn= if (options.containsKey("entityDefinitionModelRoot")) options.get("entityDefinitionModelRoot") else ""
  val compressionFormat= if (options.containsKey("compression")) options.get("compression") else "snappy"
  val customDataFolderPattern = if (options.containsKey("dataFolderFormat")) options.get("dataFolderFormat") else ""
  var entityDefinitionStorage = if (options.containsKey("entityDefinitionStorage")) options.get("entityDefinitionStorage") else storage

  if (useCdmStandard &&  !entDefModelRootIn.isEmpty) {
    throw new Exception(Messages.invalidBothStandardAndEntityDefCont)
  }

  if (Constants.PRODUCTION && manifestFileName.equals(Constants.MODEL_JSON))  {
    throw new Exception("Writing model.json is not supported.")
  }

  var entDefContAndPath = getEntityDefinitionPath(useCdmStandard, container, manifestPath, entDefModelRootIn)
  val compressionCodec = getCompression(compressionFormat)

  import com.microsoft.cdm.utils.Environment
  import com.microsoft.cdm.utils.SparkPlatform

  if ((Environment.sparkPlatform eq SparkPlatform.DataBricks) && compressionFormat.equals("lzo")) checkLzo(compressionCodec)

  if (!useTokenAuth(options)) {
    if(!entityDefinitionStorage.equals(storage)) {
      throw new IllegalArgumentException(Messages.entityDefStorageAppCredError)
    }
  }

  // if there is no entity definition model root, use the output CDM storage account
  if(entDefModelRootIn.isEmpty) {
    entityDefinitionStorage = storage
  }

  val dataFolder =
    if (customDataFolderPattern == ""){
      CDMDataFolder.getDataFolderWithDate()
    } else {
      try{
        val dataFormatter = DateTimeFormatter.ofPattern(customDataFolderPattern)
        dataFormatter.format(LocalDateTime.now)
      } catch {
        case e: Exception => throw new IllegalArgumentException(String.format(Messages.incorrectDataFolderFormat, customDataFolderPattern))
      }
    }

  var entityDefinition = if (entDefIn.isEmpty || entDefIn.startsWith("/")) entDefIn else "/"+ entDefIn

  val delimiter = getDelimiterChar(options.get("delimiter"))
  val showHeader = if(options.containsKey("columnHeaders")) options.get("columnHeaders").toBoolean else true

  val fileFormatSettings = FileFormatSettings(fileFormatType, delimiter, showHeader)



  // Return the container + path to use for the CDM adapter. If useCdmStandard is set, set it to an empty string, which is the flag
  // in the CDMCommonModel to use the CdmStandard adapter
  private def getEntityDefinitionPath(useCdmStandard: Boolean, origContainer:String, origPath: String, entDefModelRootIn: String) = {

    val entDefPath = if(entDefModelRootIn.isEmpty || entDefModelRootIn.startsWith("/")) entDefModelRootIn else "/"+ entDefModelRootIn

    if (useCdmStandard) {
      ""
    } else {
      //If "entityDefnitionModelRoot" was empty, use the CDM metadata container -> which would be container plus the origPath
      if (entDefModelRootIn.isEmpty()) {
        origContainer + origPath.dropRight(1)
      } else {
        entDefPath
      }
    }
  }

  def getCompression(compressionFormat: String): CompressionCodecName = {
    try {
      CompressionCodecName.fromConf(compressionFormat)
    }catch {
      case e: IllegalArgumentException =>  throw new IllegalArgumentException(String.format(Messages.invalidCompressionFormat, compressionFormat))
    }
  }

  def checkLzo(compression: CompressionCodecName) = {
    try {
      val version = "which lzop".!!
      Class.forName(compression.getHadoopCompressionCodecClassName)
    }
    catch {
      case _ => throw new UnsupportedOperationException("Codec class " + compression.getHadoopCompressionCodecClassName + " is not available. " +
        "If running on databricks, please read: https://docs.databricks.com/data/data-sources/read-lzo.html ")
    }
  }

  def getDelimiterChar(value: String): Char = {
    val delimiter = if(value != null) value else ",";
    if (delimiter.length > 1) {
      throw new IllegalArgumentException(String.format(Messages.invalidDelimiterCharacter, delimiter))
    }
    delimiter.charAt(0)
  }

}
