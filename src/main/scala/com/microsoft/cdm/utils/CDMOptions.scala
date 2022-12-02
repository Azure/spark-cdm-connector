package com.microsoft.cdm.utils

import com.microsoft.cdm.log.SparkCDMLogger
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

class CDMOptions(options: CaseInsensitiveStringMap) {

  val logger  = LoggerFactory.getLogger(classOf[CDMOptions])

  var appId: String = ""
  var appKey: String = ""
  var tenantId : String = ""
  var sasToken: String = ""
  var auth: Auth = null

  val storage = getRequiredArgument(options, "storage")
  val entity= getRequiredArgument(options,"entity")
  val newManifestPath= getRequiredArgument(options,"manifestPath")

  val manipathPathInput =  getContainerManifestPathAndFile(newManifestPath)
  var manifestPath= manipathPathInput.manifestPath
  val manifestFileName = manipathPathInput.manifestFileName
  val container = manipathPathInput.container

  val maxCDMThreadsString = if (options.containsKey("maxCDMThreads")) options.get("maxCDMThreads") else "100"
  if (!isNumeric(maxCDMThreadsString)) throw new Exception(String.format("%s - %s", Messages.invalidThreadCount, maxCDMThreadsString))
  val maxCDMThreads = maxCDMThreadsString.toInt
  if (maxCDMThreads < 1 ) throw new Exception(String.format("%s - %s", Messages.invalidThreadCount, maxCDMThreadsString))

  val cdmSource =
    if (options.containsKey("cdmSource")) {
      val cdmSourceValue = options.get("cdmSource")
      CDMSource.getValue(cdmSourceValue)
    } else
      CDMSource.REFERENCED



  var conf : Configuration = SparkSession.builder.getOrCreate.sessionState.newHadoopConf()
  Environment.sparkPlatform = SparkPlatform.getPlatform(conf)
  if (getAuthType(options) == CdmAuthType.AppReg.toString()) {
    appId = getRequiredArgument(options,"appId")
    appKey = getRequiredArgument(options,"appKey")
    tenantId = getRequiredArgument(options,"tenantId")
    auth = AppRegAuth(appId, appKey, tenantId)
  } else if (getAuthType(options) == CdmAuthType.Sas.toString()) {
    sasToken = getRequiredArgument(options,"sasToken")
    auth = SasAuth(sasToken)
  } else if (getAuthType(options) == CdmAuthType.Token.toString()) {
    auth = TokenAuth()
  } else {
    if (Environment.sparkPlatform == SparkPlatform.Other){
      throw new Exception(Messages.managedIdentitiesSynapseDataBricksOnly)
    }
  }

  def isNumeric(input: String): Boolean = input.forall(_.isDigit)

  private def getRequiredArgument(options: CaseInsensitiveStringMap, arg: String): String = {
    val result = if (options.containsKey(arg)) options.get(arg) else  {
      throw new Exception(s"'$arg' is a required argument!")
    }
    result
  }

  def getAuthType(options: CaseInsensitiveStringMap): String = {
    val appIdPresent =  options.containsKey("appId")
    val appKeyPresent =  options.containsKey("appKey")
    val tenantIdPresent =  options.containsKey("tenantId")
    val sasTokenPresent =  options.containsKey("sasToken")
    val result = if (appIdPresent || appKeyPresent|| tenantIdPresent) {
      //make sure all creds are present
      if (!appIdPresent || !appKeyPresent || !tenantIdPresent) {
        throw new Exception("All creds must exist")
      }
      SparkCDMLogger.log(Level.INFO,"Using app registration for authentication", logger)
      CdmAuthType.AppReg.toString()
    } else if (sasTokenPresent) {
      SparkCDMLogger.log(Level.INFO,"Using SAS token for authentication", logger)
      CdmAuthType.Sas.toString()
    } else {
      SparkCDMLogger.log(Level.INFO, "Using managed identities for authentication", logger)
      CdmAuthType.Token.toString()
    }
    result
  }

  def checkValidFileName(manifestFileName: String) =  {
    if(manifestFileName != Constants.MODEL_JSON &&  !manifestFileName.contains(".manifest.cdm.json")) {
      throw new Exception(String.format("Invalid manifest filename provided - %s", manifestFileName))
    }
  }

  def getContainerManifestPathAndFile(manifestContainerPath: String) =  {

    var manifestContainerPathTemp = manifestContainerPath
    if(manifestContainerPath.startsWith("/") &&  manifestContainerPath.length > 1) {
      manifestContainerPathTemp =  manifestContainerPath.substring(1)
    }
    val manifestFileNameStartIndex = manifestContainerPathTemp.lastIndexOf("/") + 1
    val manifestFileName = manifestContainerPathTemp.substring(manifestFileNameStartIndex)

    checkValidFileName(manifestFileName)

    val containerEndIndex = manifestContainerPathTemp.indexOf("/")
    if(containerEndIndex == -1) {
      throw new Exception("Container is not specified in the manifestPath")
    }
    var container = manifestContainerPathTemp.substring(0, containerEndIndex)
    container = if(container.startsWith("/")) container else "/" + container

    var manifestPath = manifestContainerPathTemp.substring(containerEndIndex, manifestFileNameStartIndex)
    manifestPath = if(manifestPath.startsWith("/")) manifestPath else "/" + manifestPath

    ManifestPath(container, manifestPath, manifestFileName)
  }

  val fileFormatType = if(options.containsKey("format")) options.get("format") else "csv"
  val overrideConfigPathIn = if (options.containsKey("configPath")) options.get("configPath") else ""
  val overrideConfigPath =  if (overrideConfigPathIn.startsWith("/")) overrideConfigPathIn else "/" + overrideConfigPathIn


}
