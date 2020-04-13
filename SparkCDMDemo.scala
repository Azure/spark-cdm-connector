// Databricks notebook source
import org.apache.spark.sql.types.{BooleanType, DateType, Decimal, DecimalType, DoubleType, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType, TimestampType}

val appid = ""
val appkey = ""
val tenantid = ""



val outputContainer = "<containerName>"
val storageAccountName = "<storageAccount>.dfs.core.windows.net"


// COMMAND ----------

//implicit case
import org.apache.spark.sql.types.{BooleanType, DateType, Decimal, DecimalType, DoubleType, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType, TimestampType}

//Parquet Demo with implicit Schema building
val date= java.sql.Date.valueOf("2015-03-31");
val timestamp = new java.sql.Timestamp(System.currentTimeMillis());
val data = Seq(
  Row("a", 1, true, 12.34,6L, date, Decimal(2.3), timestamp) ,
  Row("b", 2, false, 13.34,7L, date, Decimal(3.3), timestamp),
   Row("c", 3, true, 12.34,6L, date, Decimal(2.3), timestamp) ,
  Row("d", 4, false, 13.34,7L, date, Decimal(3.3), timestamp),
   Row("e", 5, true, 12.34,6L, date, Decimal(2.3), timestamp) ,
  Row("f", 6, false, 13.34,7L, date, Decimal(3.3), timestamp),
   Row("g", 7, true, 12.34,6L, date, Decimal(2.3), timestamp) ,
  Row("h", 8, false, 13.34,7L, date, Decimal(3.3), timestamp)
)

 val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))
        .add(StructField("dob", DateType, true))
        .add(StructField("weight",  DecimalType(28,1), true))
        .add(StructField("time", TimestampType, true))

val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

//Create a new manifest and add the entity to it
df.write.format("com.microsoft.cdm")
  .option("storage", storageAccountName)
  .option("container", outputContainer)
  .option("manifest", "/root/default.manifest.cdm.json")
  .option("entity", "TestEntity")
  .option("format", "parquet")
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .save()

// Append the same dataframe to the entity
df.write.format("com.microsoft.cdm")
  .option("storage", storageAccountName)
  .option("container", outputContainer)
  .option("manifest", "/root/default.manifest.cdm.json")
  .option("entity", "TestEntity")
  .option("format", "parquet")
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .mode(SaveMode.Append)
  .save()


val readDf = spark.read.format("com.microsoft.cdm")
  .option("storage", storageAccountName)
  .option("container", outputContainer)
  .option("manifest", "/root/default.manifest.cdm.json")
  .option("entity", "TestEntity")
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .load()

readDf.select("*").show()
      

// COMMAND ----------

//Predefined writes
val data = Seq(
        Row("1", "2", "3", 4), Row("4", "5", "6", 8),Row("7", "8", "9", 4),Row("10", "11", "12", 8),Row("13", "14", "15", 4))
val schema = new StructType()
        .add(StructField("teamMembershipId", StringType, true))
        .add(StructField("systemUserId", StringType, true))
        .add(StructField("teamId", StringType, true))
        .add(StructField("versinNumber", IntegerType, true))

val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", outputContainer)
        .option("manifest", "root2/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("entityDefinition", "core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
        .option("useSubManifest", true)
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()

df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", outputContainer)
        .option("manifest", "/root2/root.manifest.cdm.json")
        .option("entity", "KnowledgeArticleCategory")
        .option("entityDefinitionContainer", "outputsubmanifest")
        .option("entityDefinitionModelRoot", "example-public-standards")
        .option("entityDefinition", "/core/applicationCommon/KnowledgeArticleCategory.cdm.json/KnowledgeArticleCategory")
        .option("useSubManifest", true)
        .option("format", "parquet")
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()

val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", outputContainer)
        .option("manifest", "/root2/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .load()

val readDf2 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", outputContainer)
        .option("manifest", "/root2/root.manifest.cdm.json")
        .option("entity", "KnowledgeArticleCategory")
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .load()
readDf.select("*").show
      
