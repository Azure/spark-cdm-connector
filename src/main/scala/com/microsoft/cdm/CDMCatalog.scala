package com.microsoft.cdm

import java.util

import com.microsoft.cdm.log.SparkCDMLogger
import com.microsoft.cdm.utils.{CDMOptions, CdmAuthType, EntityNotFoundException, ManifestNotFoundException}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

class CDMCatalog extends CatalogPlugin with TableCatalog with SupportsNamespaces {

  val logger  = LoggerFactory.getLogger(classOf[CDMCatalog])
  var cdmOptions: CDMOptions = null;
  var tables: HadoopTables = null;

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    logger.info("Initializing CDM Catalog...")
    this.tables = new HadoopTables()
  }

  @throws(classOf[NoSuchTableException])
  override def loadTable(ident: Identifier): SparkTable  = {
    try {
      cdmOptions = ident.asInstanceOf[CDMIdentifier].cdmOptions
      val cdmEntity = tables.load(cdmOptions)
      new SparkTable(cdmEntity.schema, ident.asInstanceOf[CDMIdentifier].optionsAsHashMap)
    } catch {
      case e: EntityNotFoundException =>  throw new NoSuchTableException(e.getMessage)
      case e: ManifestNotFoundException => throw new NoSuchTableException(e.getMessage)
    }
  }

  @throws(classOf[TableAlreadyExistsException])
  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
     new SparkTable(schema, ident.asInstanceOf[CDMIdentifier].optionsAsHashMap) //make it write options
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("Not supported")
  }

  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException("Not supported")

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = throw new UnsupportedOperationException("Not supported")

  override def listNamespaces(): Array[Array[String]] = throw new UnsupportedOperationException("Not supported")

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = throw new UnsupportedOperationException("Not supported")

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = throw new UnsupportedOperationException("Not supported")

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = throw new UnsupportedOperationException("Not supported")

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = throw new UnsupportedOperationException("Not supported")

  override def dropNamespace(namespace: Array[String]): Boolean = throw new UnsupportedOperationException("Not supported")

  override def listTables(namespace: Array[String]): Array[Identifier] = throw new UnsupportedOperationException("Not supported")

  override def toString = s"${this.getClass.getCanonicalName}($name)"

  override def name(): String = "CDM"


  private def getRequiredArgument(options: CaseInsensitiveStringMap, arg: String): String = {
    val result = if (options.containsKey(arg)) options.get(arg) else  {
      throw new Exception(arg + "argument required")
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

}
