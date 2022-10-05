package com.microsoft.cdm.utils

import com.microsoft.cdm.log.SparkCDMLogger
import java.util.Calendar

import com.microsoft.commondatamodel.objectmodel.cdm.{CdmCorpusDefinition, CdmEntityDeclarationDefinition, CdmEntityDefinition, CdmManifestDefinition}
import com.microsoft.commondatamodel.objectmodel.enums.CdmStatusLevel
import com.microsoft.commondatamodel.objectmodel.utilities.EventCallback
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

import util.control.Breaks._
import scala.collection.mutable.ArrayBuffer

case class CDMEntity(rootManifest: CdmManifestDefinition, parentManifest: CdmManifestDefinition, entityDec:CdmEntityDeclarationDefinition, var schema: StructType)
case class SchemaDiffOutput(isSame: Boolean, diffIndex: Int, path: ArrayBuffer[String])
case class CDMDecimalType(precision: Int, scale: Int)   {
  override def toString() : String = {
    this.getClass.getName + precision + scale;
  }
}
case class FileFormatSettings(fileFormat: String, delimiter: Char, showHeader: Boolean)
case class ManifestPath(container: String , manifestPath: String, manifestFileName: String)

/**
 * Enum containing possible data types for CDM data.
 */

object CDMDataType extends Enumeration {
  val byte, bigInteger, smallInteger, integer, date, dateTime, guid, float, string, double, decimal, boolean, time, entity= Value
}

object CDMDataFormat extends Enumeration {
  val Byte, Int64, Int32, Int16, Date, DateTime, Guid, Float, String, Double, Decimal, Boolean,  DateTimeOffset, Time, entity = Value
}


/**
 * Platform the connector is running on
 */
object SparkPlatform extends Enumeration {
  val DataBricks, Synapse, Local, Other = Value

  def getPlatform(conf: Configuration): Value = {
    val host = conf.get("spark.driver.host")
    // Use these conf settings to determine the platform
    if (conf.get("spark.databricks.preemption.enabled") != null) {
      SparkPlatform.DataBricks
    } else if (conf.get("spark.synapse.session.token") != null){
      SparkPlatform.Synapse
    } else if (host.equals("localhost")){
      SparkPlatform.Local
    } else {
      SparkPlatform.Other
    }
  }
}

// callback implementation to fetch Logs from CDM SDK
object CDMCallback extends EventCallback {
  val fromCDMSDK = "CDM-SDK Library"
  val logger  = LoggerFactory.getLogger(fromCDMSDK)

  override def apply(cdmStatusLevel: CdmStatusLevel, message: String): Unit = {
    // Dev debug
    // println(s"[${cdmStatusLevel}] ${message}")

    if(cdmStatusLevel == CdmStatusLevel.Error) {
      SparkCDMLogger.log(Level.ERROR, message, logger)
      SparkCDMLogger.logEventToKusto(fromCDMSDK, "", Level.ERROR, message)
      if (message.contains("saveDocumentAsAsync")) {
        throw new Exception(message)
      }
      if (message.contains("Adapter not found for the namespace") && Constants.MODE.equals("write")) {
        throw new Exception(String.format(Messages.overrideConfigJson, message))
      }
      if (Constants.MODE.equals("write")) {
        throw new Exception(message)
      }
    }
  }
}

// Singleton to retrieve the current date as a folder for partitions to be written to
object CDMDataFolder {
  def getDataFolderWithDate ():String = {
    val cal = Calendar.getInstance()
    val year=  cal.get(Calendar.YEAR)
    val month =  "%02d".format((cal.get(Calendar.MONTH ) + 1))
    val day =  "%02d".format(cal.get(Calendar.DATE))
    Constants.PARTITION_DIR_PATTERN.format(year, month, day)
  }
}

object CDMSource extends Enumeration {
  val REFERENCED, BUILTIN = Value

  def getValue(input: String) : CDMSource.Value ={
    var result : CDMSource.Value = null
    val cdmSource = CDMSource.values;
    breakable {
      for (cdm <- cdmSource) {
        if (cdm.toString.equalsIgnoreCase(input)) {
          result = cdm
          break
        }
      }
    }
    if(result == null) throw  new IllegalArgumentException(String.format(Messages.invalidCDMSourceName, input)) else result
  }
}



case class EntityNotFoundException(private val message: String = "") extends  Exception(message)
case class ManifestNotFoundException(private val message: String = "") extends  Exception(message)
