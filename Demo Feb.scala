// Databricks notebook source
val appid = ""
val appkey = ""
val tenantid = ""


val outputContainer = "/billoutput"
val storageAccount = "tcbstoragecdm.dfs.core.windows.net"


// COMMAND ----------

// MAGIC %md
// MAGIC ##Write output using parquet types

// COMMAND ----------

import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}

  val data = Seq(
    Row("tim", 1),
    Row("brad", 10)
  )

  val schema = new StructType()
    .add(StructField("name", StringType, true))
    .add(StructField("id", IntegerType, true))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 2), schema)

  //Create a new manifest and add the entity to it
  df.write.format("com.microsoft.cdm")
    .option("storage", storageAccount)
    .option("container", outputContainer)
    .option("manifest", "default.manifest.cdm.json")
    .option("entity", "TestEntity")
    .option("format", "parquet")
    .option("databricks", true)
    .option("appId", appid)
    .option("appKey", appkey)
    .option("tenantId", tenantid)
    .save()

  val readDf = spark.read.format("com.microsoft.cdm")
    .option("storage", storageAccount)
    .option("container", outputContainer)
    .option("manifest", "default.manifest.cdm.json")
    .option("entity", "TestEntity")
    .option("databricks", true)
    .option("appId", appid)
    .option("appKey", appkey)
    .option("tenantId", tenantid)
    .load()
  readDf.select("*").show(2)
  val res = df.select("name").collect()(0)
  assert(df.select("name").collect()(0).getString(0) == "tim")
  assert(df.select("id").collect()(1).getInt(0) == 10)


// COMMAND ----------

// MAGIC %md
// MAGIC ## Read a predfined entity (in sub-manifest format), add a column to the dataframe, and create a new entity that can be read by ADF

// COMMAND ----------


//Create a new manifest and add the entity to it
val df = spark.read.format("com.microsoft.cdm")
      .option("storage", storageAccount)
      .option("container", outputContainer+ "/root")
      .option("manifest", "root.manifest.cdm.json")
      .option("entity", "TeamMembership")
      .option("databricks", true)
      .option("appId", appid)
      .option("appKey", appkey)
      .option("tenantId", tenantid)
      .load()


val newColumn = df.col("teamMembershipId").as("teamMembershipIdNew")
val newdf = df.withColumn("teamMembershipIdNew", newColumn)
    //Create a new manifest and add the entity to it

// Note, this is an ADF-specific test, where there entity name must match the SchemaDocuments entity name, which
// is why the name of the entity remains the same
newdf.write.format("com.microsoft.cdm")
      .option("storage", storageAccount)
      .option("container", outputContainer + "/root2")
      .option("manifest", "root.manifest.cdm.json")
      .option("derivedFromEntity", "TeamMembership")
      .option("entity", "TeamMembership")
      .option("useSubManifest", true)
      .option("databricks", true)
      .option("appId", appid)
      .option("appKey", appkey)
      .option("tenantId", tenantid)
      .save()

val readDf = spark.read.format("com.microsoft.cdm")
      .option("storage", storageAccount)
      .option("container", outputContainer+ "/root2")
      .option("manifest", "root.manifest.cdm.json")
      .option("entity", "TeamMembership")
      .option("databricks", true)
      .option("appId", appid)
      .option("appKey", appkey)
      .option("tenantId", tenantid)
      .load()

assert(readDf.columns.size == df.columns.size + 1)

// COMMAND ----------

// MAGIC %md
// MAGIC Create CDM file with parquet-based data. Read/Write Test
