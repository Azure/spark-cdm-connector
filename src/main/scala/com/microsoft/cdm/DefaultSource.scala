package com.microsoft.cdm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsCatalogOptions, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory


class DefaultSource extends SupportsCatalogOptions{

    val logger  = LoggerFactory.getLogger(classOf[DefaultSource])

    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
       null
    }
    
    override def getTable(structType: StructType, transforms: Array[Transform], map: java.util.Map[String, String]): Table = {
        try{
        val caseInsensitiveStringMap = new CaseInsensitiveStringMap(map)
        val schema = if (structType != null) {
            structType
        } else {
            inferSchema(caseInsensitiveStringMap)
        }
        new SparkTable(schema, caseInsensitiveStringMap)
        } catch {
            case _ : Exception => {
                 null
            }
        }
    }

    override def supportsExternalMetadata(): Boolean = true

    def setupDefaultSparkCatalog(spark: SparkSession, options: CaseInsensitiveStringMap) = {
        spark.conf.set("spark.sql.catalog.cdm", "com.microsoft.cdm.CDMCatalog")
//        spark.conf.set("spark.sql.catalog.cdm.appId", options.get("appId"))
//        spark.conf.set("spark.sql.catalog.cdm.appKey", options.get("appKey"))
//        spark.conf.set("spark.sql.catalog.cdm.tenantId", options.get("tenantId"))
//        spark.conf.set("spark.sql.catalog.cdm.storage", options.get("storage"))
//        spark.conf.set("spark.sql.catalog.cdm.container", options.get("container"))
        spark.sessionState.catalogManager.catalog("cdm")
    }

    override def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = {

        val spark = SparkSession.active;
        setupDefaultSparkCatalog(spark, options);
        new CDMIdentifier(options)
    }

    override def extractCatalog(options: CaseInsensitiveStringMap): String = {
        "cdm"
    }

}