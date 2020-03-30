// Databricks notebook source
import com.microsoft.cdm.utils.{AADProvider, ADLGen2Provider}
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType, TimestampType}

val appid = ""
val appkey = ""
val tenantid = ""

val container = "/timdemo4"
val storageAccountName = "cdmdemo1.dfs.core.windows.net"


// COMMAND ----------


val timestamp = new java.sql.Timestamp(System.currentTimeMillis());
      val data = Seq(
        Row("commentidval", "articleIdVal", "titleval", "commentTextVal", timestamp, "createdByVal", timestamp,
          "modifiedBy", 1, "organizationIdVal","createdOnBehalfOfVal", "modifiedOnBehalfOfVal")
      )

      val schema = new StructType()
        .add(StructField("kbArticleCommentId", StringType, true))
        .add(StructField("kbArticleId", StringType, true))
        .add(StructField("title", StringType, true))
        .add(StructField("commentText", StringType, true))
        .add(StructField("createdOn", TimestampType, true))
        .add(StructField("createdBy", StringType, true))
        .add(StructField("modifiedOn", TimestampType, true))
        .add(StructField("modifiedBy", StringType, true))
        .add(StructField("versionNumber", IntegerType, true))
        .add(StructField("organizationId", StringType, true))
        .add(StructField("createdOnBehalfBy", StringType, true))
        .add(StructField("modifiedOnBehalfBy", StringType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 3), schema)
      df.printSchema()
      df.select("*").show()
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", container)
        .option("manifest", "root.manifest.cdm.json")
        .option("entity", "ArticleComment")
        .option("entityPath", "/core/applicationCommon/ArticleComment.cdm.json/ArticleComment")
        .option("appId", appid) .option("appKey", appkey) .option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()

      val newreadDF = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", container)
        .option("manifest", "root.manifest.cdm.json")
        .option("entity", "ArticleComment")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      newreadDF.select("*").show()
      

// COMMAND ----------

val origdf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", container)
        .option("manifest", "wwi.manifest.cdm.json")
        .option("entity", "SalesBuyingGroups")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      origdf.printSchema()
      origdf.select("*").show()

      origdf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", container)
        .option("manifest", "wwiout.manifest.cdm.json")
        .option("entity", "second")
        .option("appId", appid) .option("appKey", appkey) .option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()
      val readDF = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", container)
        .option("manifest", "wwiout.manifest.cdm.json")
        .option("entity", "second")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      readDF.printSchema()
      readDF.select("*").show()
