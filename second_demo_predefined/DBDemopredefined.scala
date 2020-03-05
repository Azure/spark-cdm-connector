// Databricks notebook source
import com.microsoft.cdm.utils.{AADProvider, ADLGen2Provider}
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}

val appid = ""
val appkey = ""
val tenantid = ""


val container = "/output"
val storage = "tcbstoragecdm.dfs.core.windows.net"


// COMMAND ----------


try {
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
    .option("storage", storage)
    .option("container", container)
    .option("manifest", "default.manifest.cdm.json")
    .option("entity", "TestEntity")
    .option("format", "parquet")
    .option("databricks", true)
    .option("appId", appid)
    .option("appKey", appkey)
    .option("tenantId", tenantid)
    .save()

  val readDf = spark.read.format("com.microsoft.cdm")
    .option("storage", "tcbstoragecdm.dfs.core.windows.net")
    .option("container", container)
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
} finally {
  

}
      

// COMMAND ----------

// Demo creating entities and data from predefined locations

val data = Seq(
  Row(1, 2, 3, 4),
  Row(5, 6, 7, 8)
)

val schema = new StructType()
  .add(StructField("teamMembershipId", IntegerType, true))
   .add(StructField("systemUserId", IntegerType, true))
   .add(StructField("teamId", IntegerType, true))
   .add(StructField("versinNumber", IntegerType, true))

val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)

// entityPath meanse we are going to build this entity from a predefined entity, not from scratch
// If corpusPath is provided, that will serve as our reference for CDM:/:
// * the first path is the container, the second is the directory for the CDM contents
// If corpusPath is not provided, we use the github CDM
df.write.format("com.microsoft.cdm")
  .option("storage", storage)
  .option("container", container)
  .option("manifest", "root.manifest.cdm.json")
  .option("entity", "TeamMembership")
  .option("entityPath", "/core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
  .option("databricks", true)
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .save()

df.write.format("com.microsoft.cdm")
  .option("storage", storage)
  .option("container", container)
  .option("manifest", "root.manifest.cdm.json")
  .option("entity", "KnowledgeArticleCategory")
  .option("entityPath", "/core/applicationCommon/KnowledgeArticleCategory.cdm.json/KnowledgeArticleCategory")
  .option("corpusPath", "/outputsubmanifest/example-public-standards")
  .option("databricks", true)
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .save()

val readDf = spark.read.format("com.microsoft.cdm")
  .option("storage", storage)
  .option("container", container)
  .option("manifest", "root.manifest.cdm.json")
  .option("entity", "TeamMembership")
  .option("databricks", true)
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .load()

val readDf2 = spark.read.format("com.microsoft.cdm")
  .option("storage", storage)
  .option("container", container)
  .option("manifest", "root.manifest.cdm.json")
  .option("entity", "KnowledgeArticleCategory")
  .option("databricks", true)
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .load()

readDf2.select("*").show()


// COMMAND ----------

// Demo creating entities and data in submanifests from predefined locations

val data = Seq(
  Row("1", "2", "3", "4"),
  Row("5", "6", "7", "8")
)
val schema = new StructType()
  .add(StructField("teamMembershipId", StringType, true))
  .add(StructField("systemUserId", StringType, true))
  .add(StructField("teamId", StringType, true))
  .add(StructField("versinNumber", StringType, true))

val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
      // entityPath means we are going to build this entity from a predefined entity, not from scratch
      // If corpusPath is provided, that will serve as our reference for CDM:/:
      // * the first path is the container, the second is the directory for the CDM contents
      // If corpusPath is not provided, we use the github CDM
      // No corpus path location, so the github adapter is used for the predefined entity
df.write.format("com.microsoft.cdm")
  .option("storage", storage)
  .option("container", container)
  .option("manifest", "root.manifest.cdm.json")
  .option("entity", "TeamMembership")
  .option("entityPath", "/core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
  .option("useSubManifest", true)
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .save()

df.write.format("com.microsoft.cdm")
  .option("storage", storage)
  .option("container", container)
  .option("manifest", "root.manifest.cdm.json")
  .option("entity", "KnowledgeArticleCategory")
  .option("entityPath", "/core/applicationCommon/KnowledgeArticleCategory.cdm.json/KnowledgeArticleCategory")
  .option("corpusPath", "/outputsubmanifest/example-public-standards")
  .option("databricks", true)
  .option("useSubManifest", true)
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .save()

val readDf = spark.read.format("com.microsoft.cdm")
  .option("storage", storage)
  .option("container", container)
  .option("manifest", "root.manifest.cdm.json")
  .option("entity", "TeamMembership")
  .option("databricks", true)
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .load()

val readDf2 = spark.read.format("com.microsoft.cdm")
  .option("storage", storage)
  .option("container", container)
  .option("manifest", "root.manifest.cdm.json")
  .option("entity", "KnowledgeArticleCategory")
  .option("databricks", true)
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .load()
readDf2.select("*").show()

