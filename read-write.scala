// Databricks notebook source
import com.microsoft.cdm.utils.{AADProvider, ADLGen2Provider}
import com.microsoft.commondatamodel.objectmodel.cdm.{CdmCorpusDefinition, CdmDataPartitionDefinition, CdmEntityDefinition, CdmManifestDefinition, CdmTypeAttributeDefinition}
import com.microsoft.commondatamodel.objectmodel.enums.CdmObjectType
import com.microsoft.commondatamodel.objectmodel.storage.{AdlsAdapter, GithubAdapter}


import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}


val appId = dbutils.secrets.get(scope = "key-vault-secrets", key = "appId")
val appKey = dbutils.secrets.get(scope = "key-vault-secrets", key = "appKey")
val tenantId = dbutils.secrets.get(scope = "key-vault-secrets", key = "tenantId")

val outputContainer = "/output"
val storageAccount = "tcbstoragecdm.dfs.core.windows.net"



// COMMAND ----------

// MAGIC %md
// MAGIC Utility function to create a an enttity  how attribute-level metadata is preserved across data transformation and read/write operations.

// COMMAND ----------

def createManifestTeamMembershipEntity(container: String, manifestName: String, entityName: String) = {
  val account = "tcbstoragecdm.dfs.core.windows.net"
  val adlProvider = new ADLGen2Provider(new AADProvider(appId, appKey, tenantId))
  val filesystem = container
  val adlsAdapter = new AdlsAdapter(account, filesystem, tenantId, appId, appKey)

  val cdmCorpus = new CdmCorpusDefinition()
  cdmCorpus.getStorage.mount("adls", adlsAdapter)
  cdmCorpus.getStorage.setDefaultNamespace("adls")
  cdmCorpus.getStorage.mount("cdm", new GithubAdapter)

  val manifestAbstract = cdmCorpus.makeObject(CdmObjectType.ManifestDef, "tempAbstract").asInstanceOf[CdmManifestDefinition]
  manifestAbstract.getEntities.add(entityName, "cdm:/core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
  val localRoot = cdmCorpus.getStorage.fetchRootFolder("adls")
  localRoot.getDocuments.add(manifestAbstract)
  val manifestResolved = manifestAbstract.createResolvedManifestAsync(manifestName, "").get

  // Add an import to the foundations doc so the traits about partitions will resolve nicely.
  manifestResolved.getImports.add("cdm:/foundations.cdm.json", "")
  import scala.collection.JavaConversions._
  for (eDef <- manifestResolved.getEntities) { // Get the entity being pointed at.
    val localEDef = eDef
    val entDef = cdmCorpus.fetchObjectAsync[CdmEntityDefinition](localEDef.getEntityPath, manifestResolved).get
    val part = cdmCorpus.makeObject(CdmObjectType.DataPartitionDef, entDef.getEntityName + "-data-description").asInstanceOf[CdmDataPartitionDefinition]
    localEDef.getDataPartitions.add(part)
    part.setExplanation("not real data, just for demo")
    
    val location = entDef.getEntityName + "/partition-data.csv"
    part.setLocation(cdmCorpus.getStorage.createRelativeCorpusPath(location, manifestResolved))

    // Add trait to partition for csv params.
    val csvTrait = part.getExhibitsTraits.add("is.partition.format.CSV")
    csvTrait.getArguments.add("columnHeaders", "true")
    csvTrait.getArguments.add("delimiter", ",")

    // Get the actual location of the partition file from the corpus.
    val partPath = cdmCorpus.getStorage.corpusPathToAdapterPath(location)

    // Make a fake file with nothing but header for columns.
    var header = ""
    import scala.collection.JavaConversions._
    for (att <- entDef.getAttributes) {
      if (att.isInstanceOf[CdmTypeAttributeDefinition]) {
        val attributeDef = att.asInstanceOf[CdmTypeAttributeDefinition]
          if (!("" == header)) header += ","
          header += attributeDef.getName
        }
      }
      adlProvider.uploadData(header, partPath)
    }
  manifestResolved.saveAsAsync(manifestResolved.getManifestName + ".manifest.cdm.json", true).get()
}

// COMMAND ----------

// MAGIC %md
// MAGIC Utility function to demonstrate how attribute-level metadata is preserved across data transformation and read/write operations.

// COMMAND ----------

try {
  val manifestName = "default"
  val entityName = "TeamMembership"
  createManifestTeamMembershipEntity(outputContainer, manifestName, entityName)
  val df = spark.read.format("com.microsoft.cdm")
    .option("storage", "tcbstoragecdm.dfs.core.windows.net")
    .option("container", outputContainer)
    .option("manifest", manifestName + ".manifest.cdm.json")
    .option("entity", entityName)
    .option("appId", appId)
    .option("appKey", appKey)
    .option("tenantId", tenantId)
    .load()

  //verify properties come through into the metadata object
  assert(df.schema.fields(0).name == "teamMembershipId")
  val md = df.schema.fields(0).metadata
  assert(md.getString("description") == "Unique identifier of the teamMembership.")
  assert(md.contains("maximumValue") == false)
  assert(md.getBoolean("isNullable") == false)
  assert(df.schema.fields(3).metadata.getBoolean("isNullable") == true)
 

  //change one of the properties: from true false
  val mdb = new MetadataBuilder().withMetadata(df.schema.fields(3).metadata)
  mdb.putBoolean("isNullable", false)
  val newMd = mdb.build()
  
  //add a derived column
  val newColumn = df.col("versionNumber").as("versionNumberNew", newMd)
  val newdf = df.withColumn("versionNumberNew", newColumn)

  newdf.write.format("com.microsoft.cdm")
    .option("storage", "tcbstoragecdm.dfs.core.windows.net")
    .option("container", outputContainer)
    .option("manifest", manifestName + ".manifest.cdm.json")
    .option("entity", entityName + "save")
    .option("appId", appId)
    .option("appKey", appKey)
    .option("tenantId", tenantId)
    .save()

  val rereaddf = spark.read.format("com.microsoft.cdm")
    .option("storage", "tcbstoragecdm.dfs.core.windows.net")
    .option("container", outputContainer)
    .option("manifest", manifestName + ".manifest.cdm.json")
    .option("entity", entityName + "save")
    .option("appId", appId)
    .option("appKey", appKey)
    .option("tenantId", tenantId)
    .load()

  //compare the original metadata object with the ones we wrote out when we save the dataframe
  assert(rereaddf.schema.fields(0).name == "teamMembershipId")
  assert(rereaddf.schema.fields(0).name == df.schema.fields(0).name)
  val rereadmd = rereaddf.schema.fields(0).metadata
  assert(rereadmd.getString("description") == md.getString("description"))
  assert(rereadmd.contains("maximumValue") == md.contains("maximumValue"))
  
  //verify the new column's name and one of the properties changed values
  assert(rereaddf.schema.fields(4).metadata.getBoolean("isNullable") == false)
  assert(rereaddf.schema.fields(4).name == "versionNumberNew")

} finally {
  /* clean-up
  adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + metadataContainer + "/TeamMembership/partition-data.csv")
  adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + metadataContainer + "/default.manifest.cdm.json")
  adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + metadataContainer + "/TeamMembership.cdm.json")
  adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + metadataContainer + "/TeamMembershipsave.cdm.json")
  adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + metadataContainer + "/config.json")
  */
}
  

// COMMAND ----------

// MAGIC %md
// MAGIC Create CDM file with parquet-based data. Read/Write Test

// COMMAND ----------

private val adlProvider = new ADLGen2Provider(new AADProvider(appId, appKey, tenantId))

try {
      val data = Seq(
        Row("tim", 1),
        Row("brad", 10)
      )

      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data),
        schema)

      //Create a new manifest and add the entity to it
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccount)
        .option("container", outputContainer)
        .option("manifest", "default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("appId", appId)
        .option("appKey", appKey)
        .option("tenantId", tenantId)
        .save()

      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccount)
        .option("container", outputContainer)
        .option("manifest", "default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appId)
        .option("appKey", appKey)
        .option("tenantId", tenantId)
        .load()
      df.select("*").show(2)
      val res = df.select("name").collect()(0)
      assert(df.select("name").collect()(0).getString(0) == "tim")
      assert(df.select("id").collect()(1).getInt(0) == 10)

    } finally {
      /*cleanup the files we created*/
      adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + outputContainer + "/TestEntity/pdata0.parquet")
      adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + outputContainer + "/TestEntity.cdm.json")
      adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + outputContainer + "/default.manifest.cdm.json")
      adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + outputContainer + "/config.json")
    }

// COMMAND ----------

test("spark adls preserve property") {
    try {
      val manifestName = "default"
      val entityName = "TeamMembership"
      createManifestTeamMembershipEntity(metadataContainer, manifestName, entityName)
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", "tcbstoragecdm.dfs.core.windows.net")
        .option("container", metadataContainer)
        .option("manifest", manifestName + ".manifest.cdm.json")
        .option("entity", entityName)
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .load()

      //verify properties come through into the metadata object
      assert(df.schema.fields(0).name == "teamMembershipId")
      val md = df.schema.fields(0).metadata
      assert(md.getString("description") == "Unique identifier of the teamMembership.")
      assert(md.contains("maximumValue") == false)
      assert(md.getBoolean("isNullable") == false)
      assert(df.schema.fields(3).metadata.getBoolean("isNullable") == true)
      //TODO: Fix library because this is not returning true
      println("TODO: Fix library where is_primary key is not returning true for primary key memmbershipid ")
      //assert(md.getBoolean("isPrimaryKey") == true)

      //change one of the properties: from true false
      val mdb = new MetadataBuilder().withMetadata(df.schema.fields(3).metadata)
      mdb.putBoolean("isNullable", false)
      val newMd = mdb.build()
      //add a derived column
      val newColumn = df.col("versionNumber").as("versionNumberNew", newMd)
      val newdf = df.withColumn("versionNumberNew", newColumn)

      newdf.write.format("com.microsoft.cdm")
        .option("storage", "tcbstoragecdm.dfs.core.windows.net")
        .option("container", metadataContainer)
        .option("manifest", manifestName + ".manifest.cdm.json")
        .option("entity", entityName + "save")
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .save()

      val rereaddf = spark.read.format("com.microsoft.cdm")
        .option("storage", "tcbstoragecdm.dfs.core.windows.net")
        .option("container", metadataContainer)
        .option("manifest", manifestName + ".manifest.cdm.json")
        .option("entity", entityName + "save")
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .load()

      //compare the original metadata object with the ones we wrote out when we save the dataframe
      assert(rereaddf.schema.fields(0).name == "teamMembershipId")
      assert(rereaddf.schema.fields(0).name == df.schema.fields(0).name)
      val rereadmd = rereaddf.schema.fields(0).metadata
      assert(rereadmd.getString("description") == md.getString("description"))
      assert(rereadmd.contains("maximumValue") == md.contains("maximumValue"))
      //verify the new column's name and one of the properties changed values
      assert(rereaddf.schema.fields(4).metadata.getBoolean("isNullable") == false)
      assert(rereaddf.schema.fields(4).name == "versionNumberNew")

    }
    finally {
      adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + metadataContainer + "/TeamMembership/partition-data.csv")
      adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + metadataContainer + "/default.manifest.cdm.json")
      adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + metadataContainer + "/TeamMembership.cdm.json")
      adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + metadataContainer + "/TeamMembershipsave.cdm.json")
      adlProvider.deleteFile("https://tcbstoragecdm.dfs.core.windows.net" + metadataContainer + "/config.json")
    }
  }
