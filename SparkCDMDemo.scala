// Databricks notebook source
//import com.microsoft.cdm.utils.{AADProvider, ADLGen2Provider}
import org.apache.spark.sql.types.{BooleanType, DateType, Decimal, DecimalType, DoubleType, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType, TimestampType}


val appid = ""
val appkey = ""
val tenantid = ""

val outputContainer = "<container"
val storageAccountName = "<storage>.dfs.core.windows.net"


// COMMAND ----------

//implicit case
import org.apache.spark.sql.types.{BooleanType, DateType, Decimal, DecimalType, DoubleType, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType, TimestampType}

//Parquet Demo with implicit Schema building
val date= java.sql.Date.valueOf("2015-03-31");
val timestamp = new java.sql.Timestamp(System.currentTimeMillis());
val data = Seq(
   Row("a", 1, true, 12.34, 6L, date, timestamp, Decimal(1.4337879), Decimal(999.00), Decimal(18.8)),
   Row("b", 1, true, 12.34, 6L, date, timestamp, Decimal(1.4337879), Decimal(999.00), Decimal(18.8))
)

 val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))
        .add(StructField("dob", DateType, true))
        .add(StructField("time", TimestampType, true))
        .add(StructField("decimal1", DecimalType(15, 3), true))
        .add(StructField("decimal2", DecimalType(38, 7), true))
       .add(StructField("decimal3", DecimalType(5, 2), true))

val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

//Create a new manifest and add the entity to it with gzip'd parquet partitions
df.write.format("com.microsoft.cdm")
  .option("storage", storageAccountName)
  .option("container", outputContainer)
  .option("manifest", "/root/default.manifest.cdm.json")
  .option("entity", "TestEntity")
  .option("useCDMGithub", true)
  .option("format", "parquet")
  .option("compression", "gzip")
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .save()

// Append the same dataframe to the entity in CSV format
df.write.format("com.microsoft.cdm")
  .option("storage", storageAccountName)
  .option("container", outputContainer)
  .option("manifest", "/root/default.manifest.cdm.json")
  .option("entity", "TestEntity")
  .option("useCDMGithub", true)
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
  .option("useCDMGithub", true)
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .load()

readDf.select("*").show()
readDf.count()

// COMMAND ----------

//Predefined writes
val data = Seq(
        Row("1", "2", "3", 4L), Row("4", "5", "6", 8L),Row("7", "8", "9", 4L),Row("10", "11", "12", 8L),Row("13", "14", "15", 4L))
val schema = new StructType()
        .add(StructField("teamMembershipId", StringType, true))
        .add(StructField("systemUserId", StringType, true))
        .add(StructField("teamId", StringType, true))
        .add(StructField("versinNumber", LongType, true))

val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", outputContainer)
        .option("manifest", "root2/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("entityDefinition", "core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
        .option("useCDMGithub", true)
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
        .option("useCDMGithub", true)
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .load()

val readDf2 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", outputContainer)
        .option("manifest", "/root2/root.manifest.cdm.json")
        .option("entity", "KnowledgeArticleCategory")
        .option("useCDMGithub", true)
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .load()
readDf.select("*").show
      
