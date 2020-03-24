// Databricks notebook source

val appid = ""
val appkey = ""
val tenantid = ""


val container = "/output"
val storageAccountName = "tcbstoragecdm.dfs.core.windows.net"


// COMMAND ----------

// Demo creating entities and data in submanifests from predefined locations from overwrite
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}

 val data1 = Seq(
        Row(1, 1, 1, 1),
        Row(2, 2, 2, 2),
        Row(3, 3, 3, 3),
        Row(4, 4, 4, 4),
        Row(5, 5, 5, 5),
        Row(6, 6, 6, 6)
      )

      val data2 = Seq(
        Row(7, 7, 7, 7),
        Row(8, 8, 8, 8),
        Row(9, 9, 9, 9),
        Row(0, 0, 0, 0)
      )

      val schema = new StructType()
        .add(StructField("teamMembershipId", IntegerType, true))
        .add(StructField("systemUserId", IntegerType, true))
        .add(StructField("teamId", IntegerType, true))
        .add(StructField("versionNumber", IntegerType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data1, 3), schema)
      val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2, 2), schema)

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", container)
        .option("manifest", "root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("entityPath", "/core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
        .option("useSubManifest", true)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      df2.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", container)
        .option("manifest", "root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("entityPath", "/core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
        .option("useSubManifest", true)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()

      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", container)
        .option("manifest", "root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(readDf.collect.size == 4)
    

// COMMAND ----------

val entities = List("AWCustomer", "Person", "Customer")
entities.foreach { entity =>

  val origdf = spark.read.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("container", "/billdata")
          .option("manifest", "root.manifest.cdm.json")
          .option("entity", entity)
          .option("corpusPath", "/billmodels")
          .option("appId", appid)
          .option("appKey", appkey)
          .option("tenantId", tenantid)
          .load()
  origdf.printSchema()
  origdf.select("*").show()
}
