// Databricks notebook source
import org.apache.spark.sql.types.{BooleanType, DateType, Decimal, DecimalType, DoubleType, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType, TimestampType}

val appid = "<appId>"
val appkey = "<appKey>"
val tenantid = "<tenantId>"

val container = "<demoContainerName>"
val storageAccountName = "<storageAccount>.dfs.core.windows.net"


// COMMAND ----------

// Implicit write case
import org.apache.spark.sql.types.{BooleanType, DateType, Decimal, DecimalType, DoubleType, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType, TimestampType}

// Write a CDM entity with Parquet data files, entity definition is derived from the dataframe schema
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

// Creates the CDM manifest and adds the entity to it with gzip'd parquet partitions
// with both physical and logical entity definitions 
df.write.format("com.microsoft.cdm")
  .option("storage", storageAccountName)
  .option("container", container)
  .option("manifest", "/implicitTest/default.manifest.cdm.json")
  .option("entity", "TestEntity")
  .option("format", "parquet")
  .option("compression", "gzip")
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .mode(SaveMode.Append)
  .save()

// Append the same dataframe content to the entity in the default CSV format
df.write.format("com.microsoft.cdm")
  .option("storage", storageAccountName)
  .option("container", container)
  .option("manifest", "/implicitTest/default.manifest.cdm.json")
  .option("entity", "TestEntity")
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .mode(SaveMode.Append)
  .save()

val readDf = spark.read.format("com.microsoft.cdm")
  .option("storage", storageAccountName)
  .option("container", container)
  .option("manifest", "/implicitTest/default.manifest.cdm.json")
  .option("entity", "TestEntity")
  .option("appId", appid)
  .option("appKey", appkey)
  .option("tenantId", tenantid)
  .load()

readDf.select("*").show()


// COMMAND ----------

// Explicit write, creating an entity in a CDM folder based on a pre-defined model 

// Case 1: Using an entity definition defined in the CDM Github repo

val data = Seq(
        Row("1", "2", "3", 4L), Row("4", "5", "6", 8L),Row("7", "8", "9", 4L),Row("10", "11", "12", 8L),Row("13", "14", "15", 4L))
val schema = new StructType()
        .add(StructField("teamMembershipId", StringType, true))
        .add(StructField("systemUserId", StringType, true))
        .add(StructField("teamId", StringType, true))
        .add(StructField("versionNumber", LongType, true))


val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", container)
        .option("manifest", "explicitTest/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("entityDefinition", "core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
        .option("useCdmGithubModelRoot", true)  // sets the model root to the CDM GitHub schema documents folder
        .option("useSubManifest", true)
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()

val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("container", container)
        .option("manifest", "/explicitTest/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("useCDMGithub", true) // sets the modelroot alias to the CDM GitHub schema documents folder
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .load()
readDf.select("*").show()


// COMMAND ----------

// Explicit write, creating an entity in a CDM folder based on a pre-defined model 

// Case 2: Using an entity definition defined in a CDM model stored in ADLS

// UPLOAD CDM FILES FIRST
// To run this example, first create a /Models/Contacts folder to your demo container in ADLS gen2,
// then upload the provided Contacts.manifest.cdm.json, Person.cdm.json, Entity.cdm.json files

val birthdate= java.sql.Date.valueOf("1991-03-31");
val now = new java.sql.Timestamp(System.currentTimeMillis());
val data2 = Seq(
  Row(1,now,"Donna","Carreras",birthdate),
  Row(2,now,"Keith","Harris",birthdate),
  Row(2,now,"Carla","McGee",birthdate)
)

val schema2 = new StructType()
  .add(StructField("identifier", IntegerType))
  .add(StructField("createdTime", TimestampType))
  .add(StructField("firstName", StringType))
  .add(StructField("lastName", StringType))
  .add(StructField("birthDate", DateType))

// Create the dataframe that matches the CDM definition of the entity, Person
val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2, 1), schema2)
df2.write.format("com.microsoft.cdm")
    .option("storage", storageAccountName)
    .option("container", container)
    .option("manifest", "/Data/Contacts/root.manifest.cdm.json")
    .option("entity", "Person")
    .option("entityDefinitionContainer", container)
    .option("entityDefinitionModelRoot", "Models") 
    .option("entityDefinition", "/Contacts/Person.cdm.json/Person")   
    .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
    .mode(SaveMode.Overwrite)
    .save()

val readDf2 = spark.read.format("com.microsoft.cdm")
  .option("storage", storageAccountName)
  .option("container", container)
  .option("manifest", "/Data/Contacts/root.manifest.cdm.json")
  .option("entity", "Person")
  .option("entityDefinitionContainer", container)
  .option("entityDefinitionModelRoot", "Models")
  .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
  .load()
readDf2.select("*").show()
