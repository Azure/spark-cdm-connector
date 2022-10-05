package com.microsoft.cdm.test


import java.sql.Timestamp
import java.time.{LocalDate, LocalTime, ZoneId}
import java.time.format.DateTimeFormatter

import com.microsoft.cdm.utils.Constants.DECIMAL_PRECISION
import org.apache.commons.io
import com.microsoft.cdm.utils.{AppRegAuth, Auth, CDMDataFolder, Constants, Messages, SerializedABFSHadoopConf}
import com.microsoft.commondatamodel.objectmodel.cdm.{CdmCorpusDefinition, CdmDataPartitionDefinition, CdmEntityDefinition, CdmManifestDeclarationDefinition, CdmManifestDefinition, CdmTraitReference, CdmTypeAttributeDefinition}
import com.microsoft.commondatamodel.objectmodel.enums.CdmObjectType
import com.microsoft.commondatamodel.objectmodel.storage.{AdlsAdapter, CdmStandardsAdapter}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.scalatest.FunSuite

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.io.Source
import scala.util.Random
import scala.util.Random.javaRandomToRandom

class CDMADLS extends FunSuite {
  private val appid = System.getenv("APPID")
  private val appkey = System.getenv("APPKEY")
  private val tenantid = System.getenv("TENANTID")
  private val storageAccountName = System.getenv(("ACCOUNT_URL"))
  private val sastokenatroot = System.getenv("SASTOKENROOT")
  private val sastokenatfolder = System.getenv("SASTOKENFOLDER")
  private val sastokencustomerfolder = System.getenv("SASTOKENCUSTOMERFOLDER")
  private val sastokentransactionfolder = System.getenv("SASTOKENTRANSACTIONFOLDER")
  private val storageAccountUrl = "https://" + storageAccountName;
  private val outputContainer = "output"

  private val metadataContainer = "testmetadata"
  private val newContainer = "wwi-new-model"
  private val oldModelContainer = "wwi-sales"
  private val patternContainer = "testpattern"

  Constants.PRODUCTION = false

  //val storage = "srichetastorage.dfs.core.windows.net"
  private val spark = SparkSession.builder()
    .master("local[2]") // If the number is changed, some of the unit tests will fail.
    .appName("Demo")
    .config("spark.driver.host", "localhost")
    .config("spark.ui.enabled", false)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  private val conf = SparkSession.builder.getOrCreate.sessionState.newHadoopConf
  private val testdata = new TestData(spark)


  /**
   * Cleanup after a test
   * @param container - the Container to delete from
   * @param path - the path directory to delete (recursively)
   */
  private def cleanup(container: String, path: String ): Unit = {
    val serConf = SerializedABFSHadoopConf.getConfiguration(storageAccountName, "/" + container, AppRegAuth(appid, appkey, tenantid), conf)
    val fs= serConf.getFileSystem()
    fs.delete(new Path(path), true)
  }

  test("tanmoy cx test case - also use it to test maxCDMThreads option") {
    val df = spark.read.format("com.microsoft.cdm")
      .option("storage", storageAccountName)
      .option("manifestPath", "tanmoy/ADBTest1_10_21_2020/model.json")
      .option("entity", "EmployeeData")
      .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
      .load()
    df.printSchema()
    df.select("*").show()

    try {
      val caught = intercept[Exception] {
        spark.read.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", "tanmoy/ADBTest1_10_21_2020/model.json")
          .option("entity", "EmployeeData")
          .option("maxCDMThreads", "foo")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .load()
      }
      assert(caught.getMessage == String.format("%s - %s", Messages.invalidThreadCount, "foo"))
      val caught2 = intercept[Exception] {
        spark.read.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", "tanmoy/ADBTest1_10_21_2020/model.json")
          .option("entity", "EmployeeData")
          .option("maxCDMThreads", "-1")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .load()
      }
      assert(caught2.getMessage == String.format("%s - %s", Messages.invalidThreadCount, "-1"))
    }
    catch {
      case e: Exception => println("Exception: " + e.printStackTrace())
        assert(false)
    }
  }

  test("spark adls wwi read all") {
    val entities = List("SalesBuyingGroups", "SalesCustomerCategories", "SalesCustomers", "SalesOrderLines",
      "SalesOrders", "WarehouseColors", "WarehousePackageTypes", "WarehouseStockItems")
    entities.foreach { entity =>
      println("Reading entity --- " + entity)
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", newContainer + "/wwi.manifest.cdm.json")
        .option("entity", entity)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      df.printSchema()
      df.select("*").show()
      entity match {
        case "SalesBuyingGroups" => assert(df.count() == 2)
        case "SalesCustomerCategories" => assert(df.count() == 8)
        case "SalesCustomers" => assert(df.count() == 663)
        case "SalesOrderLines" => assert(df.count() == 231412)
        case "SalesOrders" => assert(df.count() == 73595)
        case "WarehouseColors" => assert(df.count() == 36)
        case "WarehousePackageTypes" => assert(df.count() == 14)
        case "WarehouseStockItems" => assert(df.count() == 227)
      }
    }
  }

  test("Handle nulls in parquet and csv") {

    val df = testdata.prepareNullData()
    try {
      // 1. Parquet
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/nulldataparquet/root.manifest.cdm.json")
        .option("entity", "NullDataParquet")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()
      var readDF = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/nulldataparquet/root.manifest.cdm.json")
        .option("entity", "NullDataParquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      readDF.show()

      assert(readDF.except(df).count() == 0)

      // 2. CSV
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/nulldatacsv/root.manifest.cdm.json")
        .option("entity", "NullDataCSV")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()
      readDF = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/nulldatacsv/root.manifest.cdm.json")
        .option("entity", "NullDataCSV")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      readDF.show()

      assert(readDF.except(df).count() == 0)
    }
    catch {
      case e: Exception=> println("Exception: " + e.printStackTrace())
        assert(false)
    } finally {
      /*cleanup*/
      cleanup(outputContainer, "/")
    }
  }

  test("spark adls too many char per col") {
    try {
      val originalDF = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "rahul/toomanycharpercol/model.json")
        .option("entity", "contact")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      originalDF.printSchema()
      originalDF.select("*").show()

      originalDF.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "contact")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()
    }
    catch {
      case e: Exception => println("Exception: " + e.printStackTrace())
        assert(false)
    } finally {
      /*cleanup*/
      cleanup(outputContainer, "/")
    }
  }

  test("spark adls too many char per col with sasToken at root level") {
    try {
      val originalDF = spark.read.format("com.microsoft.cdm")
        .option("storage",storageAccountName)
        .option("manifestPath", "sateesh/toomanycharpercol/model.json")
        .option("entity", "contact")
        .option("sasToken", sastokenatroot)
        .load()
      originalDF.select("*").show()

      originalDF.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "contact")
        .option("sasToken", sastokenatroot)
        .save()
    } catch {
      case e: Exception=> println("Exception: " + e.printStackTrace())
        assert(false)
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test("spark adls too many char per col with sasToken at folder level") {
    try {
      val originalDF = spark.read.format("com.microsoft.cdm")
        .option("storage",storageAccountName)
        .option("manifestPath", "dynamicscontainer/sastokenfolder/model.json")
        .option("entity", "contact")
        .option("sasToken", sastokenatfolder)
        .load()
      originalDF.select("*").show()

      originalDF.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "dynamicscontainer/sastokenfolder/write/default.manifest.cdm.json")
        .option("entity", "contact")
        .option("sasToken", sastokenatfolder)
        .save()
    } catch {
      case e: Exception=> println("Exception: " + e.printStackTrace())
        assert(false)
    } finally {
      cleanup("dynamicscontainer", "/sastokenfolder/write/")
    }
  }

  test("spark adls with sas token join at folder level") {
    try {
      val originalDF = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "sateesh/Customer/Main.manifest.cdm.json")
        .option("entity", "CustTable")
        .option("sasToken", sastokencustomerfolder)
        .load()
      val originalDF1 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "sateesh/Transaction/Transaction.manifest.cdm.json")
        .option("entity", "CustTrans")
        .option("sasToken", sastokentransactionfolder)
        .load()
      originalDF1.join(originalDF, Seq("ACCOUNTNUM")).limit(1).show()
    } catch {
      case e: Exception=> println("Exception: " + e.printStackTrace())
        assert(false)
    }
  } 

  test("spark adls read rahul") {
    try {
      val originalDF = spark.read.format("com.microsoft.cdm")
        .option("storage",storageAccountName)
        .option("manifestPath", "rahul/contact/model.json")
        .option("entity", "contact")
        .option("appId", appid) .option("appKey", appkey) .option("tenantId", tenantid)
        .load()
      originalDF.select("*").show()
    }
    catch {
      case e: Exception=> println("Exception: " + e.printStackTrace())
        assert(false)
    }
  }

  test ("chevron") {
    try{
      val readDF = spark.read.format("com.microsoft.cdm")
        .option("storage",storageAccountName)
        .option("entity", "PITag")
        .option("manifestPath", "chevron/default.manifest.cdm.json")
        .option("appId", appid) .option("appKey", appkey) .option("tenantId", tenantid)
        .load()
      //readDF.printSchema()
      assert(readDF.count() == 2)
    } catch {
      case e: Exception=> println("Exception: " + e.printStackTrace())
        assert(false)
    }
  }

  test("spark adls read antares") {
    try {
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "antaresds/nfpdataflow/pbiflows/model.json")
        .option("entity", "cdstest")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      val row = df.head()
      assert(row.getLong(0) == 1)
      assert(row.getString(1) == "Reza")

      val originalDF = spark.read.format("com.microsoft.cdm")
        .option("storage",storageAccountName)
        .option("manifestPath", "antaresds/df-Customer/model.json")
        .option("entity", "Customer")
        .option("appId", appid) .option("appKey", appkey) .option("tenantId", tenantid)
        .load()
      originalDF.select("*").show()
    }
    catch {
      case e: Exception=> println("Exception: " + e.printStackTrace())
        assert(false)
    }

  }

  test("tison long overflow issue") {
    val formatString = "HH:mm:ss"
    val formatterTime = DateTimeFormatter.ofPattern(formatString)
    val ls = LocalTime.parse("00:00:00", formatterTime)
    val instant = ls.atDate(LocalDate.of(1, 1, 1)).atZone(ZoneId.systemDefault()).toInstant
    val timestamp = Timestamp.from(instant)
    val data = Seq(Row( timestamp))

    val schema = new StructType()
      .add(StructField("time", TimestampType, true))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.write.format("com.microsoft.cdm")
      .option("storage", storageAccountName)
      .option("manifestPath", outputContainer + "/manifest/location/default.manifest.cdm.json")
      .option("entity", "tempentity")
      .option("appId", appid)
      .option("appKey", appkey)
      .option("tenantId", tenantid)
      .option("format", "parquet")
      .option("compression", "gzip")
      .save()

    val readDF = (spark.read.format("com.microsoft.cdm")
      .option("storage", storageAccountName)
      .option("manifestPath", outputContainer + "/manifest/location/default.manifest.cdm.json")
      .option("entity", "tempentity")
      .option("appId", appid)
      .option("appKey", appkey)
      .option("tenantId", tenantid)
      .load())

    val diff = df.except(readDF)
    assert(diff.count() == 0)
  }

  test("spark adls dynamics read all") {
    val container = "/customerleads"
    val adlsAdapter = new AdlsAdapter(storageAccountName, container, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")
    val manifest = cdmCorpus.fetchObjectAsync("model.json").get().asInstanceOf[CdmManifestDefinition]
    for (entity <- manifest.getEntities.asScala) {
      // There are 240 entities, and we time-out when on integration tests. Using randomness
      // to prevent integration-test time-out
      val random = Random.nextInt(100)
      if (random == 0) {
        println("entity is " + entity.getEntityName)
        val df = spark.read.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", container + "/model.json")
          .option("entity", entity.getEntityName)
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .load()
        df.show()
      }
    }
  }

  test("spark adls power bi real all") {
    try {
      val container = "/power-platform-dataflows"
      val adlsAdapter = new AdlsAdapter(storageAccountName, container, tenantid, appid, appkey)
      val cdmCorpus = new CdmCorpusDefinition()
      cdmCorpus.getStorage.mount("adls", adlsAdapter)
      cdmCorpus.getStorage.setDefaultNamespace("adls")
      val manifest = cdmCorpus.fetchObjectAsync("powerbi/NorthWind/model.json").get().asInstanceOf[CdmManifestDefinition]
      for (entity <- manifest.getEntities.asScala) {
        val modelDf = spark.read.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", container + "/powerbi/NorthWind/model.json")
          .option("entity", entity.getEntityName)
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .load()

        modelDf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/"+ entity.getEntityName + "/root.manifest.cdm.json")
          .option("entity", entity.getEntityName)
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()

        val manifestDf = spark.read.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/"+ entity.getEntityName + "/root.manifest.cdm.json")
          .option("entity", entity.getEntityName)
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .load()

        manifestDf.show()
        val row0model = modelDf.head()
        val row0manifest = manifestDf.head()
        assert(modelDf.count == manifestDf.count())
        assert(modelDf.columns.length == manifestDf.columns.length)

        for (i <- 0 until modelDf.columns.length) {
          assert(row0model.get(i) == row0manifest.get(i))
        }
        cleanup(outputContainer, "/"+entity.getEntityName)
      }
    }catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }finally {
    }
  }

  test("spark tricklefeed athena read all") {
    val container = "/cdmdynamics"
    val adlsAdapter = new AdlsAdapter(storageAccountName, container, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")
    val manifest = cdmCorpus.fetchObjectAsync("model.json").get().asInstanceOf[CdmManifestDefinition]
    for (entity <- manifest.getEntities.asScala) {
      println("Reading entity is " + entity.getEntityName)
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", container + "/model.json")
        .option("entity", entity.getEntityName)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      df.show()
    }
  }

  test("spark adls wwi write entity new manifest") {
    try {
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", newContainer + "/wwi.manifest.cdm.json")
        .option("entity", "WarehousePackageTypes")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      assert(df.rdd.getNumPartitions == 1)
      df.printSchema()
      df.select("*").show(1)
      assert(df.rdd.getNumPartitions == 1)
      val df2 = df.repartition(2)
      val first = df2.orderBy("PackageTypeName").select("PackageTypeName").collect()(0).getString(0)

      //Create a new manifest and add the entity to it
      df2.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/copy-wwi.manifest.cdm.json")
        .option("manifestName", "AGoodManifestName")
        .option("entity", "NewWarehousePackageTypes")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()
      assert(df2.rdd.getNumPartitions == 2)

      //verify we we were able to set the manifest name for a new manifest
      val manifestURI = storageAccountUrl + "/" + outputContainer + "/copy-wwi.manifest.cdm.json"
      val serConf = SerializedABFSHadoopConf.getConfiguration(storageAccountName, "/" + outputContainer, AppRegAuth(appid, appkey, tenantid), conf)
      val fs= FileSystem.get(serConf.value)
      val path = new Path("/copy-wwi.manifest.cdm.json")
      var in = fs.open(path);
      var fileString = Source.fromInputStream(in).mkString
      in.close();
      assert(fileString.contains("\"manifestName\" : \"AGoodManifestName\","))
      assert(!fileString.contains("\"manifestName\" : \"ADifferentManifestName\","))

      //Add a second entity to the newly created manifest
      df2.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/copy-wwi.manifest.cdm.json")
        .option("manifestName", "ADifferentManifestName")
        .option("entity", "NewWarehousePackageTypesAppend")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      //verify we we were able to set the manifest name on an existing manifest
      in = fs.open(path);
      fileString = Source.fromInputStream(in).mkString
      in.close();
      assert(fileString.contains("\"manifestName\" : \"ADifferentManifestName\","))
      assert(!fileString.contains("\"manifestName\" : \"AGoodManifestName\","))

      val rereaddf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/copy-wwi.manifest.cdm.json")
        .option("entity", "NewWarehousePackageTypes")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      rereaddf.printSchema()
      rereaddf.select("*").show()
      assert(df.count() == 14)
      val second = rereaddf.orderBy("PackageTypeName").select("PackageTypeName").collect()(0).getString(0)
      assert(first == second)
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      /*cleanup*/
      cleanup(outputContainer, "/")
    }
  }

  test("Test Overwrite Partitions.partition patterns") {
    val df = testdata.prepareDataWithAllTypes()
    val serConf = SerializedABFSHadoopConf.getConfiguration(storageAccountName, "/" + outputContainer, AppRegAuth(appid, appkey, tenantid), conf)
    val fs = serConf.getFileSystem()

    //write without partition patterns
    df.write.format("com.microsoft.cdm")
      .option("storage", storageAccountName)
      .option("manifestPath", outputContainer + "/dataTypeImplicit/default.manifest.cdm.json")
      .option("entity", "DataTypeImplicit")
      .option("format", "parquet")
      .option("compression", "gzip")
      .option("dataFolderFormat", "'data'")
      .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
      .save()


    assert(fs.listStatus(new Path("/dataTypeImplicit/DataTypeImplicit/data/")).length == 2)

    //write with partition patterns
    df.write.format("com.microsoft.cdm")
      .option("storage", storageAccountName)
      .option("manifestPath", outputContainer + "/dataTypeImplicit/default.manifest.cdm.json")
      .option("entity", "DataTypeImplicit")
      .option("format", "csv")
      .option("compression", "gzip")
      .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
      .mode(SaveMode.Overwrite)
      .save()

    val dataFolder = CDMDataFolder.getDataFolderWithDate()
    // Verify the old parquet files are deleted
    assert(fs.listStatus(new Path("/dataTypeImplicit/DataTypeImplicit/"+ dataFolder + "/")).length == 2)
    val file = fs.listStatus(new Path("/dataTypeImplicit/DataTypeImplicit/"+ dataFolder + "/"))
    // Verify the new files are of csv format
    assert(io.FilenameUtils.getExtension(file.apply(0).getPath.getName) == "csv")
    assert(io.FilenameUtils.getExtension(file.apply(1).getPath.getName) == "csv")
  }

  test("spark adls wwi read old model") {
    val entities = List("Sales BuyingGroups", "Sales CustomerCategories", "Sales Customers", "Sales OrderLines",
      "Sales Orders", "Warehouse Colors", "Warehouse PackageTypes", "Warehouse StockItems")
    entities.foreach { entity =>
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", oldModelContainer +"/model.json")
        .option("entity", entity)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      df.printSchema()
      df.select("*").show()
      entity match {
        case "Sales BuyingGroups" => assert(df.count() == 2)
        case "Sales CustomerCategories" => assert(df.count() == 8)
        case "Sales Customers" => assert(df.count() == 663)
        case "Sales OrderLines" => assert(df.count() == 231412)
        case "Sales Orders" => assert(df.count() == 73595)
        case "Warehouse Colors" => assert(df.count() == 36)
        case "Warehouse PackageTypes" => assert(df.count() == 14)
        case "Warehouse StockItems" => assert(df.count() == 227)
      }
    }
  }

  test("spark adls wwi read entity old model") {
    try {
      println("read manifest")
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", oldModelContainer + "/model.json")
        .option("entity", "Warehouse PackageTypes")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      assert(df.rdd.getNumPartitions == 1)
      df.select("*").show(1)

      println("read entity - Warehouse PackageTypes")
      val packageTypesDF = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "wwi-sales1/model.json")
        .option("entity", "NewWarehouse PackageTypes")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(packageTypesDF.except(df).count() == 0)
      packageTypesDF.select("*").show()

      println("read entity - Warehouse PackageTypesAppend")
      val packageTypesAppendDF = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "wwi-sales1/model.json")
        .option("entity", "NewWarehouse PackageTypesAppend")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(packageTypesAppendDF.except(df).count() == 0)
      packageTypesAppendDF.select("*").show()

    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }
    finally {
      cleanup(outputContainer, "/")
    }
  }

  test("Guru bug") {
    val df = spark.read.format("com.microsoft.cdm")
      .option("storage", storageAccountName)
      .option("manifestPath", "guru/model.json")
      .option("entity", "contact")
      .option("mode", "failfast")
      .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
      .load()

    var caught = intercept[Exception] {
      df.show()
    }
    assert(caught.getCause.getMessage.equals(Messages.incompatibleFileWithDataframe))

  }

  test("Permissive mode") {
    /*
     * Test case 1. Bad mode name
     */
    var caught = intercept[Exception] {
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "permissive/model.json")
        .option("entity", "fewer")
        .option("mode", "failfaster")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      df.show()
    }
    assert(caught.getMessage.equals(Messages.invalidMode))

    /*
     * Test case 2. Fewer items in CSV file than in schema
     */
    var dfPermissive = spark.read.format("com.microsoft.cdm")
      .option("storage", storageAccountName)
      .option("manifestPath", "permissive/model.json")
      .option("entity", "fewer")
      .option("mode", "permissive")
      .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
      .load()


    var data = Seq(
      Row("california", "state", null)
    )
    var schema = new StructType()
      .add(StructField("loc", StringType, true))
      .add(StructField("name", StringType, true))
      .add(StructField("type", StringType, true))
    var dfCompare = spark.createDataFrame(spark.sparkContext.parallelize(data),
      schema)


    var diff = dfPermissive.except(dfCompare)
    assert(diff.count() == 0)

    caught = intercept[Exception] {
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "permissive/model.json")
        .option("entity", "fewer")
        .option("mode", "failfast")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      df.show()
    }
    assert(caught.getCause.getMessage.equals(Messages.incompatibleFileWithDataframe))

    /*
     * Test case 3. More items in CSV file than in schema
     */
    dfPermissive = spark.read.format("com.microsoft.cdm")
      .option("storage", storageAccountName)
      .option("manifestPath", "permissive/model.json")
      .option("entity", "more")
      .option("mode", "permissive")
      .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
      .load()

    //extra column in more.csv is chopped off
    data = Seq(
      Row("california", "state", "oval")
    )

    schema = new StructType()
      .add(StructField("loc", StringType, true))
      .add(StructField("name", StringType, true))
      .add(StructField("type", StringType, true))
    dfCompare = spark.createDataFrame(spark.sparkContext.parallelize(data),
      schema)

    diff = dfPermissive.except(dfCompare)
    assert(diff.count() == 0)

    caught = intercept[Exception] {
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "permissive/model.json")
        .option("entity", "more")
        .option("mode", "failfast")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      df.show()
    }
    assert(caught.getCause.getMessage.equals(Messages.incompatibleFileWithDataframe))

    /*
     * Test case 4. Uncastable item with
     */
    dfPermissive = spark.read.format("com.microsoft.cdm")
      .option("storage", storageAccountName)
      .option("manifestPath", "permissive/model.json")
      .option("entity", "wrongtype")
      .option("mode", "permissive")
      .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
      .load()

    data = Seq(
      Row("california", null, "oval")
    )

    schema = new StructType()
      .add(StructField("loc", StringType, true))
      .add(StructField("name", BooleanType, true))
      .add(StructField("type", StringType, true))
    dfCompare = spark.createDataFrame(spark.sparkContext.parallelize(data),
      schema)

    diff = dfPermissive.except(dfCompare)
    assert(diff.count() == 0)

    /*No failfast exception test here because the invalid roww will be turned to all nulls*/

    /*
     * Test case 5:
     * * parquet read with permissive on should fail
     * * malformed mode not allowed
     */
    val data5 = Seq( Row("tim"))
    val schema5 = new StructType() add(StructField("name", StringType, true))
    val df5 = spark.createDataFrame(spark.sparkContext.parallelize(data5), schema5)

    try {
      //Create a new manifest and add the entity to it with parquet files
      df5.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      caught = intercept[Exception] {
        val readdf = spark.read.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("mode", "permissive")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .load()
        readdf.count()
      }
      assert(caught.getMessage.equals(Messages.invalidPermissiveMode))

      caught = intercept[Exception] {
        val readdf = spark.read.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("mode", "dropmalformed")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .load()
        readdf.count()
      }
      assert(caught.getMessage.equals(Messages.dropMalformedNotSupported))
    } catch {
      case e: Exception => {
        println("Exception: " + e.printStackTrace())
        assert(false)
      }
    } finally {
      /*cleanup*/
      cleanup(outputContainer, "/")
    }

  }

  test("spark adls write clean old/new entity model") {
    try {
      val data = Seq(
        Row(8, "bat"),
        Row(64, "mouse")
      )

      val schema = new StructType()
        .add(StructField("number", IntegerType, true))
        .add(StructField("word", StringType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data),
        schema)

      //Create a new manifest and add the entity to it
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
        .option("entity", "TestEntityA")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      //Create a new manifest and add the entity to it
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/model.json")
        .option("entity", "TestEntityB")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()
      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
        .option("entity", "TestEntityA")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      val readDf2 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/model.json")
        .option("entity", "TestEntityB")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      assert(readDf.select("word").collect()(0).getString(0) ==
        readDf2.select("word").collect()(0).getString(0))
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      /*cleanup*/
      cleanup(outputContainer, "/")
    }
  }


  test("implicit spark adls write parquet cdm complex types") {
    try{
      val date= java.sql.Date.valueOf("2015-03-31")
      val timestamp = new java.sql.Timestamp(System.currentTimeMillis());
      val data = Seq(
        Row(13, Row("Str1", true, 12.34,6L, date, Decimal(2.3), timestamp,  Row("sub1", Row(Decimal(23.5))))) ,
        Row(24, Row("Str2", false, 12.34,6L, date, Decimal(2.3), timestamp, Row("sub2", Row(Decimal(17.6)))))
      )

      val schema = new StructType()
        .add(StructField("id", IntegerType, true))
        .add(StructField("details", new StructType()
          .add(StructField("name", StringType, true))
          .add(StructField("flag", BooleanType, true))
          .add(StructField("salary", DoubleType, true))
          .add(StructField("phone", LongType, true))
          .add(StructField("dob", DateType, true))
          .add(StructField("weight",  DecimalType(DECIMAL_PRECISION,1), true))
          .add(StructField("time", TimestampType, true))
          .add(StructField("subRow", new StructType()
            .add(StructField("name", StringType, true))
            .add(StructField("level3", new StructType()
              .add(StructField("time1", DecimalType(DECIMAL_PRECISION,1), true))
            )
            )
          )
          )))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      print(df.show(false));

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      readDf.show(false);
      assert(readDf.select("id").collect()(0).getInt(0) == 13)
      assert(readDf.select("details.name").collect()(0).getString(0)  == "Str1")
      assert(readDf.select("details.name").collect()(1).getString(0)  == "Str2")
      assert(readDf.select("details.salary").collect()(0).getDouble(0)  == 12.34)
      assert(readDf.select("details.dob").collect()(0).getDate(0).toString  == date.toString)
      assert(readDf.select("details.subRow.name").collect()(0).getString(0) == "sub1")
      assert(readDf.select("details.subRow.name").collect()(1).getString(0) == "sub2")
      assert(readDf.select("details.subRow").collect()(0).isInstanceOf[GenericRowWithSchema]);
    }
    catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }
    finally {
      cleanup(outputContainer, "/root")
    }
  }

  test("test customDataFolder") {
    try {
      val data = Seq(
        Row("tim", 1, true, 12.34, 6L),
        Row("tddim", 1, false, 13.34, 7L),
        Row("tddim", 1, false, 13.34, 7L)
      )

      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data),
        schema)


      val fileTypes=Vector("/rootmanifest/default.manifest.cdm.json" )
      val formats = Vector("'year'yyyy'/month'MM", "'year='yyyy'/month'MM")
      var counter = 0

      for (file <- fileTypes; format <- formats) {
        var mode = if(counter !=0 ) SaveMode.Overwrite else SaveMode.ErrorIfExists
        /* Write using a customer data Folder format */
        df.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + file)
          .option("entity", "TestEntity")
          .option("dataFolderFormat", format)
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .mode(mode)
          .save()

        counter +=1;

        val read = spark.read.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + file)
          .option("entity", "TestEntity")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .load()
        val diff = read.except(df)
        assert(diff.count() == 0)

        val badDataFormat = "WrongDataFormat"
        var caught = intercept[Exception] {
          df.write.format("com.microsoft.cdm")
            .option("storage", storageAccountName)
            .option("manifestPath", outputContainer + file)
            .option("entity", "TestEntity")
            .option("dataFolderFormat", "WrongDataFormat")
            .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
            .mode(SaveMode.Overwrite)
            .save()
        }
        assert(caught.getMessage.startsWith(String.format(Messages.incorrectDataFolderFormat, badDataFormat)))
      }
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test("spaces in folder names model.json") {
    try {
      val data = Seq(
        Row("tim", 1, true, 12.34, 6L),
        Row("tddim", 1, false, 13.34, 7L),
        Row("tddim", 1, false, 13.34, 7L)
      )

      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data),
        schema)

      val read = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath",   "folderwithspaces/root with spaces/model.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      val origH = df.head()
      val readH = read.head()
      assert(df.count == read.count())
      assert(df.columns.length == read.columns.length)

      for (i <- 0 until df.columns.length) {
        assert(origH.get(i) == readH.get(i))
      }
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test("spaces in folder names manifest.cdm.json") {
    try {
      val data = Seq(
        Row("tim", 1, true, 12.34, 6L),
        Row("tddim", 1, false, 13.34, 7L),
        Row("tddim", 1, false, 13.34, 7L)
      )

      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data),
        schema)

      //Create a new manifest and add the entity to it with parquet files
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer+ "/root with spaces/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // Overwrite entity with csv files
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer+ "/root with spaces/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()

      val read = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer+ "/root with spaces/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      val origH = df.head()
      val readH = read.head()
      assert(df.count == read.count())
      assert(df.columns.length == read.columns.length)

      for (i <- 0 until df.columns.length) {
        assert(origH.get(i) == readH.get(i))
      }
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }


  test("Invalid model") {
    try{
      val data = Seq(Row("str1"))
      val schema = new StructType() .add(StructField("name", StringType, true))

      var entDef = "core/applicationCommon/TeamIndividual.cdm.json/TeamInvidiual"
      var entDefParent = entDef.dropRight(entDef.length - entDef.lastIndexOf('/'))
      // Invalid entityDefinition - TeamIndividual doesn't exist
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema);
      var caught = intercept[Exception] {
        df.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
          .option("entityDefinitionPath", entDef)
          .option("useCdmStandardModelRoot", true)
          .option("entity", "TestEntity")
          .option("format", "parquet")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      assert(caught.getMessage.startsWith(String.format(Messages.entityDefinitionModelFileNotFound, "/" + entDefParent)))

      entDef = "/core/applicationCommon/TeamMembership.cdm.json/TeamMembership"
      entDefParent = entDef.dropRight(entDef.length - entDef.lastIndexOf('/'))
      // Invalid entityDefinition directory
      caught = intercept[Exception] {
        df.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
          .option("entityDefinitionPath", entDef)
          .option("entityDefinitionModelRoot", "baddir")
          .option("entity", "TestEntity")
          .option("format", "parquet")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      assert(caught.getMessage.startsWith(String.format(Messages.entityDefinitionModelFileNotFound, entDefParent)))
    } finally {

    }

  }


  test("Nested explicit Parquet with predefined,  submanifest entity ") {
    try {
      val df = testdata.prepareNestedData()
      // entity write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/Entity/default.manifest.cdm.json")
        .option("entity", "Person")
        .option("useCdmStandardModelRoot", true)
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // Predefined write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/predefined/default.manifest.cdm.json")
        .option("entity", "PersonPredefined")
        .option("entityDefinitionPath", "core/applicationCommon/Person.cdm.json/Person")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // with submanifest from scratch
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/submanifest/root.manifest.cdm.json")
        .option("entity", "PersonSubmanifest")
        .option("useSubManifest", true)
        .option("format", "parquet")
        .option("useCdmStandardModelRoot", true)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()


      val readEntity = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/Entity/default.manifest.cdm.json")
        .option("entity", "Person")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      val readPredefined = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/predefined/default.manifest.cdm.json")
        .option("entity", "PersonPredefined")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      val readSubmanifest = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/submanifest/root.manifest.cdm.json")
        .option("entity", "PersonSubmanifest")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      df.show(false)
      readEntity.show(false)
      readPredefined.show(false)
      readSubmanifest.show(false)

      assert(readEntity.select("id").collect()(0).getInt(0) == 13)
      assert(readEntity.select("details.name").collect()(0).getString(0)  == "Str1")
      assert(readEntity.select("details.name").collect()(1).getString(0)  == "Str2")
      assert(readPredefined.select("details.salary").collect()(0).getDouble(0)  == 12.34)
      assert(readPredefined.select("details.subRow.name").collect()(0).getString(0) == "sub1")
      assert(readSubmanifest.select("details.subRow.name").collect()(1).getString(0) == "sub2")
      assert(readSubmanifest.select("details.subRow").collect()(0).isInstanceOf[GenericRowWithSchema]);
    }catch{
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }
    finally {
      cleanup(outputContainer, "/")
    }
  }

  test("Managed Identities unsupported on Unit Tests") {
    val caught = intercept[Exception] {
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "doesntmatter/foo.manifest.cdm.json")
        .option("entity", "blah")
        .load()
    }
    assert(caught.getMessage == String.format(Messages.managedIdentitiesSynapseDataBricksOnly))
  }

  test("Invalid schema") {
    try{
      val date= java.sql.Date.valueOf("2015-03-31");
      val timestamp = new java.sql.Timestamp(System.currentTimeMillis());
      val data = Seq(
        Row("0", true, "0", 0L)
      )

      /* Forcing failure with systemUserId as a boolean, it should be a String */
      val schema = new StructType()
        .add(StructField("teamMembershipId", StringType, true))
        .add(StructField("systemUserId", BooleanType, true))
        .add(StructField("teamId", StringType, true))
        .add(StructField("versionNumber", LongType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema);

      var caught = intercept[Exception] {
        df.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("entityDefinitionPath", "core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
          .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
          .option("format", "parquet")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      assert(caught.getMessage == String.format(Messages.invalidIndexSchema, "systemUserId", "TestEntity > systemUserId"))

      val data1 = Seq(Row(1))
      val schema1 = new StructType()
        .add(StructField("teamMembershipId", IntegerType, true))
      val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data1), schema1)

      /*
       * Test 3
       */

      /* Create an entitiy with a single column */
      df1.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dir/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val data2 = Seq(Row(7, 7))
      val schema2 = new StructType()
        .add(StructField("teamMembershipId", IntegerType, true))
        .add(StructField("systemUserId", IntegerType, true))
      val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema2)

      /* Try to write data to the same entity (one column) with two columns of data */
      caught = intercept[Exception] {
        df2.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/dir/root.manifest.cdm.json")
          .option("entity", "TeamMembership")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .mode(SaveMode.Append)
          .save()
      }
      assert(caught.getMessage == String.format(Messages.mismatchedSizeSchema, "TeamMembership"))

      /**
       * Test 4 - arrays with mismatch in number of fields (nested entity)
       */
      val df3 = testdata.prepareNestedDataArrays()
      caught = intercept[Exception] {
        df3.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/dir/default.manifest.cdm.json")
          .option("entity", "ArrayMismatch")
          .option("entityDefinitionPath", "core/applicationCommon/ArrayMismatch.cdm.json/ArrayMismatch")
          .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
          .option("format", "parquet")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      assert(caught.getMessage == String.format(Messages.mismatchedSizeSchema, "ArrayMismatch > details > subRow > hit_songs"))

      /**
       * Test 5 - arrays with mismatch in data type (nested entity)
       */
      val df4 = testdata.prepareNestedDataArrays()
      caught = intercept[Exception] {
        df4.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/dir/default.manifest.cdm.json")
          .option("entity", "ArrayInvalidIndex")
          .option("entityDefinitionPath", "core/applicationCommon/ArrayInvalidIndex.cdm.json/ArrayInvalidIndex")
          .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
          .option("format", "parquet")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      assert(caught.getMessage == String.format(Messages.invalidIndexSchema, "number", "ArrayInvalidIndex > details > subRow > hit_songs > number"))

      /**
       * Test 5 - Implicit overwrite
       */
      val origdata = Seq(
        Row("0", "1w", "0", 0L)
      )

      val origschema = new StructType()
        .add(StructField("teamMembershipId", StringType, true))
        .add(StructField("systemUserId", StringType, true))
        .add(StructField("teamId", StringType, true))
        .add(StructField("versionNumber", LongType, true))

      val origdf = spark.createDataFrame(spark.sparkContext.parallelize(origdata), origschema);

      origdf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()

      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(readDf.schema.apply("systemUserId").dataType.isInstanceOf[BooleanType])

    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test("decimal model.json") {
    try {

      /* Test 1 */
      val rdf= spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "inputcdmdynamics/model.json")
        .option("entity", "contact")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      rdf.select("*").show()

      val rdf2 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "inputcdmdynamics/model.json")
        .option("entity", "account")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      rdf2.select("*").show()

      /* Test 2 */
      val data = Seq(Row(Decimal(1.4337879), Decimal(999.00), Decimal(18.8)))
      val schema = new StructType().add(StructField("decimal1", DecimalType(15, 3), true))
        .add(StructField("decimal2", DecimalType(38, 7), true))
        .add(StructField("decimal3", DecimalType(5, 2), true))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      // Write df as model.json parquet
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/parquet/model.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // Read
      val rd =   spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/parquet/model.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      val caught = intercept[Exception] {
        rd.select("*").show()
      }

      assert(caught.getCause.getMessage == "Parquet decimal metadata don't match the CDM decimal arguments for field \"decimal1\". Found Parquet decimal (15, 3)")

      // Write df as model.json csv
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/csv/model.json")
        .option("entity", "TestEntity")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()
      val readDF = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/csv/model.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(readDF.schema.fields(0).dataType.asInstanceOf[DecimalType].precision == Constants.CDM_DEFAULT_PRECISION)
      assert(readDF.schema.fields(1).dataType.asInstanceOf[DecimalType].precision == Constants.CDM_DEFAULT_PRECISION)
      assert(readDF.schema.fields(2).dataType.asInstanceOf[DecimalType].precision == Constants.CDM_DEFAULT_PRECISION)
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }finally {
      cleanup(outputContainer, "/")
    }
  }

  test("test decimal" ) {
    try {
      // 1. Test with precision  == scale
      var data = Seq(Row(Decimal(8989.334)))
      var schema = new StructType().add(StructField("decimal3", DecimalType(8, 8), true))
      var df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      val caught1 = intercept[Exception] {
        df.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("format", "parquet")
          .option("compression", "gzip")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      assert(caught1.getCause.getCause().getMessage == String.format(Messages.invalidDecimalFormat , new Integer(8), new Integer(8)))

      // 2. Test with other decimals in parquet and csv files
      data = Seq(Row(Decimal(1.4337879), Decimal(999.00), Decimal(18.8)))
      schema = new StructType().add(StructField("decimal1", DecimalType(15, 3), true))
        .add(StructField("decimal2", DecimalType(38, 7), true))
        .add(StructField("decimal3", DecimalType(5, 2), true))
      df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // write parquet
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root2/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // write csv
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root2/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Append)
        .save()

      //read parquet
      val readDF= spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root2/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()


      readDF.select("*").show()
      val rows = readDF.head(2)
      val row0 = rows.apply(0)
      val row1 = rows.apply(1)
      assert(row0.getDecimal(0).toString == Decimal(1.4337879, 15, 3).toString())
      assert(row0.getDecimal(1).toString == Decimal(999.00, 38, 7).toString())
      assert(row0.getDecimal(2).toString == Decimal(18.8, 5, 2).toString())
      assert(row1.getDecimal(0).toString == Decimal(1.4337879, 15, 3).toString())
      assert(row1.getDecimal(1).toString == Decimal(999.00, 38, 7).toString())
      assert(row1.getDecimal(2).toString == Decimal(18.8, 5, 2).toString())

    }
    catch{
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }
    finally {
      cleanup(outputContainer, "/")
    }
  }

  test("lzo compression") {
    try {
      val date = java.sql.Date.valueOf("2015-03-31");
      val timestamp = new java.sql.Timestamp(System.currentTimeMillis());
      val data = Seq(
        Row("tim", 1, true, 12.34, 6L, date, Decimal(999.00), timestamp, 2f),
        Row("tddim", 1, false, 13.34, 7L, date, Decimal(3.3), timestamp, 3.59f),
        Row("tddim", 1, false, 13.34, 7L, date, Decimal(3.3), timestamp, 3.59f)
      )
      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))
        .add(StructField("dob", DateType, true))
        .add(StructField("weight", DecimalType(Constants.DECIMAL_PRECISION, 2), true))
        .add(StructField("time", TimestampType, true))
        .add(StructField("float", FloatType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/lzo/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("compression", "lzo")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/lzo/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      val row = readDf.head()
      assert(row.getString(0) == "tim")
      assert(row.getInt(1) == 1)
      assert(row.getBoolean(2) == true)
      assert(row.getDouble(3) == 12.34)
      assert(row.getLong(4) == 6)
      assert(row.getDate(5).toString() == date.toString)
      assert(row.getDecimal(6).toString == "999.00")
      assert(row.getFloat(8) == 2f)
    }
    catch{
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }
    finally {
      cleanup(outputContainer, "/lzo")
    }
  }

  test("spark adls write all types parquet and csv") {
    try {
      val date= java.sql.Date.valueOf("2015-03-31");

      val formatString = "HH:mm:ss.SSSSSS"
      val formatterTime = DateTimeFormatter.ofPattern(formatString)
      val ls = LocalTime.parse("08:00:00.123456", formatterTime)
      val instant = ls.atDate(LocalDate.of(1970, 1, 1)).atZone(ZoneId.systemDefault()).toInstant
      val timestamp = Timestamp.from(instant)

      val localTime= instant.atZone(ZoneId.systemDefault()).toLocalTime
      val timeWithoutDate = localTime.format(DateTimeFormatter.ofPattern(formatString)).toString

      val ls2 = LocalTime.parse(timeWithoutDate, formatterTime)
      val instant2 = ls2.atDate(LocalDate.of(1970, 1, 1)).atZone(ZoneId.systemDefault()).toInstant
      val timestampWithDefaultDate= Timestamp.from(instant2)

      val byteVal = 2.toByte
      val shortVal = 129.toShort
      val data = Seq(
        Row("tim", 1, true, 12.34,6L, date, Decimal(999.00), timestamp, 2f, byteVal, shortVal, timestamp),
        Row("tddim", 1, false, 13.34,7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal, timestamp),
        Row("tddim", 1, false, 13.34,7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal, timestamp),
        Row("tddim", 1, false, 13.34,7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal, timestamp),
        Row("tim", 1, true, 12.34,6L, date, Decimal(2.3), timestamp, 3.59f, byteVal, shortVal, timestamp),
        Row("tddim", 1, false, 13.34,7L, date, Decimal(3.3), timestamp, 3590.9f, byteVal, shortVal, timestamp),
        Row("tddim", 1, false, 13.34,7L, date, Decimal(3.3), timestamp, 359.8f, byteVal, shortVal, timestamp),
        Row("tddim", 1, false, 13.34,7L, date, Decimal(3.3), timestamp, 3.593f, byteVal, shortVal, timestamp),
        Row("tddim", 1, false, 13.34,7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal, timestamp),
        Row("tddim", 1, false, 13.34,7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal, timestamp),
        Row("tddim", 1, false, 13.34,7L, date, Decimal(3.3), timestamp, 332.33f, byteVal, shortVal, timestamp),
        Row("tddim", 1, false, 13.34,7L, date, Decimal(3.3), timestamp, 3.53232f, byteVal, shortVal, timestamp)
      )

      val md = new MetadataBuilder().putString(Constants.MD_DATATYPE_OVERRIDE, Constants.MD_DATATYPE_OVERRIDE_TIME).build()

      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))
        .add(StructField("dob", DateType, true))
        .add(StructField("weight",  DecimalType(Constants.DECIMAL_PRECISION,7), true))
        .add(StructField("datetime", TimestampType, true))
        .add(StructField("float", FloatType, true))
        .add(StructField("byte", ByteType, true))
        .add(StructField("short", ShortType, true))
        .add(StructField("timeOnly", TimestampType, true, md))

      /*
       * The partitioning needs to be 1 because we are testing against the first row in each partition. We are
       * performing three writes, each which generate the same partition data.
       */
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data),
        schema)

      //Create a new manifest and add the entity to it
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Append)
        .save()

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Append)
        .save()

      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      readDf.show(false)
      val rows = readDf.head(36)
      val row0 = rows.apply(0)
      val row1 = rows.apply(12)
      val row2 = rows.apply(24)
      assert(readDf.count == 36)

      for (row <- Seq(row0, row1, row2)){
        assert(row.getString(0) == "tim")
        assert(row.getInt(1) == 1)
        assert(row.getBoolean(2) == true)
        assert(row.getDouble(3) == 12.34)
        assert(row.getLong(4) == 6)
        assert(row.getDate(5).toString() == date.toString)
        assert(row.getDecimal(6).toString == "999.0000000")
        assert(row.getFloat(8) == 2f)
        assert(row.getByte(9) == 2.toByte)
        assert(row.getShort(10) == 129.toShort)
        assert(row.getTimestamp(11) == timestampWithDefaultDate)
      }
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      /*cleanup*/
      cleanup(outputContainer, "/root")
    }
  }

  test("Nested and CSV") {
    val structdf = testdata.prepareNestedData();
    val arrayadf = testdata.prepareNestedDataArrays()

    var caught = intercept[Exception] {
      structdf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
        .option("entity", "structdf")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()
    }

    assert(caught.getMessage == Messages.nestedTypesNotSupported)

    caught = intercept[Exception] {
      arrayadf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
        .option("entity", "arrayadf")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()
    }

    assert(caught.getMessage == Messages.nestedTypesNotSupported)
  }

  test("Null and  Empty Arrays Parquet") {
    try {
      val df = testdata.prepareNullAndEmptyArrays()
      df.show(false)

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/null/default.manifest.cdm.json")
        .option("entity", "NullAndEmpty")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readdf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/null/default.manifest.cdm.json")
        .option("entity", "NullAndEmpty")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      readdf.show(false)
      assert(readdf.count() == df.count())
    }catch{
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }finally {
      cleanup(outputContainer, "/null")
    }
  }

  test("Test CSV Header and Delimiter option") {

    try {
      val df = testdata.prepareDataWithAllTypes()

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/csvHeader/default.manifest.cdm.json")
        .option("entity", "CSVHeaderTest")
        .option("delimiter", '\u0001')
        .option("columnHeaders", false)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // Append data with default delimiter - ',' and columnHeaders - true
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/csvHeader/default.manifest.cdm.json")
        .option("entity", "CSVHeaderTest")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Append)
        .save()

      val readdf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/csvHeader/default.manifest.cdm.json")
        .option("entity", "CSVHeaderTest")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      val diff = df.except(readdf)
      assert(readdf.count == 24)
      assert(diff.count() == 0)

      // negetive test - invalid Delimiter char
      var caught = intercept[Exception] {
        df.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/csvHeader/default.manifest.cdm.json")
          .option("entity", "CSVHeaderTest")
          .option("delimiter", "sr")
          .option("columnHeaders", true)
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .mode(SaveMode.Overwrite)
          .save()
      }
      assert(caught.getMessage == String.format(Messages.invalidDelimiterCharacter, "sr"))

      // negetive test - invalid Delimiter (out of char range)
      caught = intercept[Exception] {
        df.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/csvHeader/default.manifest.cdm.json")
          .option("entity", "CSVHeaderTest")
          .option("delimiter", 894389489)
          .option("columnHeaders", true)
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .mode(SaveMode.Overwrite)
          .save()
      }
      assert(caught.getMessage == String.format(Messages.invalidDelimiterCharacter, new Integer(894389489)))
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/csvHeader")
    }
  }

  test("microseconds") {
    try{
      val timestamp1 =   Timestamp.valueOf("2015-03-31 14:02:20");
      val timestamp2 =   Timestamp.valueOf("2015-03-31 02:02:20.7");
      val timestamp3 =   Timestamp.valueOf("2015-03-31 02:02:20.32");
      val timestamp4 =   Timestamp.valueOf("2015-03-31 02:02:20.323");
      val timestamp5 =   Timestamp.valueOf("2015-03-31 23:02:20.4566");
      val timestamp6 =   Timestamp.valueOf("2015-03-31 02:02:20.90900");
      val timestamp7 =   Timestamp.valueOf("2015-03-31 02:02:20.879898");
      val timestamp8 =   Timestamp.valueOf("2015-03-31 09:02:20.879898999");



      val data = Seq(
        Row(timestamp1, timestamp2, timestamp3, timestamp4, timestamp5, timestamp6, timestamp7,  timestamp8)
      )

      val schema = new StructType()
        .add(StructField("time1", TimestampType, true))
        .add(StructField("time2", TimestampType, true))
        .add(StructField("time3", TimestampType, true))
        .add(StructField("time4", TimestampType, true))
        .add(StructField("time5", TimestampType, true))
        .add(StructField("time6", TimestampType, true))
        .add(StructField("time7", TimestampType, true))
        .add(StructField("time8", TimestampType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.show(false)
      // Implicit Parquet Write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/time/default.manifest.cdm.json")
        .option("entity", "Time")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readParquet = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/time/default.manifest.cdm.json")
        .option("entity", "Time")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      readParquet.show(false)

      // Implicit csv Write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/time1/default.manifest.cdm.json")
        .option("entity", "Time1")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readCSV = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/time1/default.manifest.cdm.json")
        .option("entity", "Time1")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      readCSV.show(false)

      val rowDf = df.head
      val row1 = readCSV.head
      val row2 =  readParquet.head

      for (row <- Seq(row1, row2)){
        assert(row.getTimestamp(0) == rowDf.getTimestamp(0))
        assert(row.getTimestamp(1) == rowDf.getTimestamp(1))
        assert(row.getTimestamp(2) == rowDf.getTimestamp(2))
        assert(row.getTimestamp(3) == rowDf.getTimestamp(3))
        assert(row.getTimestamp(4) == rowDf.getTimestamp(4))
        assert(row.getTimestamp(5) == rowDf.getTimestamp(5))
        assert(row.getTimestamp(6) == rowDf.getTimestamp(6))
        assert(row.getTimestamp(7) == rowDf.getTimestamp(7))
      }
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }

  }

  test("delete partitions on abort without submanifest") {
    try {
      val data = Seq(Row("a", 1, true), Row("b", 1, false), Row("c", 1, false), Row("d", 1, false), Row("e", 1, false))

      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))

      var df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      Constants.EXCEPTION_TEST = true
      //Test 1 - abort when entity created for first time
      var caught = intercept[Exception] {
        df.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer +  "/abort/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("format", "parquet")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      val serConf = SerializedABFSHadoopConf.getConfiguration(storageAccountName, "/" + outputContainer, AppRegAuth(appid, appkey, tenantid), conf)
      val fs = FileSystem.get(serConf.value)
      val dataFolder = CDMDataFolder.getDataFolderWithDate()
      assert(fs.listStatus(new Path("/abort/TestEntity/"+dataFolder + "/")).length == 0)
      Constants.EXCEPTION_TEST = false

      // SuccessfuLL write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      var read = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      read.show(false)
      Constants.EXCEPTION_TEST = true

      // Append test with csv files- fail on abort
      val appenddf = spark.createDataFrame(spark.sparkContext.parallelize(data, 3), schema)
      intercept[Exception] {
        appenddf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .mode(SaveMode.Append)
          .save()
      }

      read = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      read.show(false)

      assert(df.except(read).count() == 0)
      assert(fs.listStatus(new Path("/abort/TestEntity/" + dataFolder +"/")).length == 2)

      // Overwrite test with csv files- fail on abort
      val overwritedf = spark.createDataFrame(spark.sparkContext.parallelize(data, 5), schema)
      intercept[Exception] {
        overwritedf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .mode(SaveMode.Overwrite)
          .save()
      }

      read = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      read.show(false)
      assert(df.except(read).count() == 0)
      assert(fs.listStatus(new Path("/abort/TestEntity/" + dataFolder +"/")).length == 2)
    } catch {
      case e: Exception => {
        println("Exception: " + e.printStackTrace())
        assert(false)
      }
    } finally {
      cleanup(outputContainer, "/abort")
      Constants.EXCEPTION_TEST = false
    }
  }

  test("delete partitions on abort with submanifests") {
    try {
      val data = Seq(Row("a", 1, true), Row("b", 1, false), Row("c", 1, false), Row("d", 1, false), Row("e", 1, false))

      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))

      var df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      Constants.EXCEPTION_TEST = true
      //Test 1 - abort when entity created for first time
      var caught = intercept[Exception] {
        df.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("useSubManifest", "true")
          .option("format", "parquet")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      val serConf = SerializedABFSHadoopConf.getConfiguration(storageAccountName, "/" + outputContainer, AppRegAuth(appid, appkey, tenantid), conf)
      val fs = FileSystem.get(serConf.value)
      val dataFolder = CDMDataFolder.getDataFolderWithDate()
      assert(fs.listStatus(new Path("/abort/TestEntity/" + dataFolder + "/")).length == 0)
      Constants.EXCEPTION_TEST = false

      // SuccessfuLL write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("useSubManifest", "true")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      var read = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer +  "/abort/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      read.show(false)
      Constants.EXCEPTION_TEST = true

      // Append test with csv files- fail on abort
      val appenddf = spark.createDataFrame(spark.sparkContext.parallelize(data, 3), schema)
      intercept[Exception] {
        appenddf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("useSubManifest", "true")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .mode(SaveMode.Append)
          .save()
      }

      read = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      read.show(false)

      assert(df.except(read).count() == 0)
      assert(fs.listStatus(new Path("/abort/TestEntity/" + dataFolder + "/")).length == 2)

      // Overwrite test with csv files- fail on abort
      val overwritedf = spark.createDataFrame(spark.sparkContext.parallelize(data, 5), schema)
      intercept[Exception] {
        overwritedf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("useSubManifest", "true")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .mode(SaveMode.Overwrite)
          .save()
      }

      read = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/abort/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      read.show(false)
      assert(df.except(read).count() == 0)
      assert(fs.listStatus(new Path("/abort/TestEntity/" + dataFolder + "/")).length == 2)
    } catch {
      case e: Exception => {
        println("Exception: " + e.printStackTrace())
        assert(false)
      }
    } finally {
      cleanup(outputContainer, "/abort")
      Constants.EXCEPTION_TEST = false
    }
  }

  test("config.json test") {
    try {
      val timestamp1 = new java.sql.Timestamp(System.currentTimeMillis());
      val timestamp2 = new java.sql.Timestamp(System.currentTimeMillis());
      val cdata = Seq(
        Row( timestamp1, timestamp2,1, "A", Decimal(33.5)),
        Row( timestamp1, timestamp2, 2, "B", Decimal(42.1)),
        Row( timestamp1, timestamp2, 3, "C", Decimal(7.90))
      )

      val cschema = new StructType()
        .add(StructField("ValidFrom", TimestampType, true))
        .add(StructField("ValidTo", TimestampType, true))
        .add(StructField("CustomerId", IntegerType, true))
        .add(StructField("CustomerName", StringType, true))
        .add(StructField("CreditLimit", DecimalType(18, 2), true))

      val customerdf = spark.createDataFrame(spark.sparkContext.parallelize(cdata), cschema)

      customerdf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/customer/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("entityDefinitionPath", "Customer.cdm.json/Customer")
        .option("entityDefinitionModelRoot", "billalias")
        .option("configPath", "/config")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      customerdf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/customerImplicit/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val custRead = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/customer/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("configPath", "/config")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      custRead.show()

      val pdata = Seq(
        Row( timestamp1, timestamp2,1, "A"),
        Row( timestamp1, timestamp2, 2, "B"),
        Row( timestamp1, timestamp2, 3, "C")
      )

      val pschema = new StructType()
        .add(StructField("ValidFrom", TimestampType, true))
        .add(StructField("ValidTo", TimestampType, true))
        .add(StructField("CustomerId", IntegerType, true))
        .add(StructField("CustomerName", StringType, true))

      val persondf = spark.createDataFrame(spark.sparkContext.parallelize(pdata), pschema)

      persondf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/person/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("entityDefinitionPath", "Person.cdm.json/Person")
        .option("entityDefinitionModelRoot", "billalias")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val personRead = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/person/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(custRead.except(customerdf).count() == 0)
      assert(personRead.except(persondf).count() == 0)

      // Negative test - invalid config.json path
      val invalidConfigPath = "/config1"
      var caught = intercept[RuntimeException] {
        customerdf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/configTest/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("entityDefinitionPath", "Customer.cdm.json/Customer")
          .option("entityDefinitionModelRoot", "billalias")
          .option("configPath", invalidConfigPath)
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      assert(caught.getMessage == String.format(Messages.configJsonPathNotFound, invalidConfigPath))

      // Negative test - arbitrary namespace + config path option not provided on write
      val caught2 = intercept[java.util.concurrent.ExecutionException] {
        customerdf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("entityDefinitionPath", "Customer.cdm.json/Customer")
          .option("entityDefinitionModelRoot", "billaliaswithoutconfig")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      assert(caught2.getMessage == String.format(Messages.overrideConfigJson, "java.lang.Exception: StorageManager | Adapter not found for the namespace 'core' | fetchAdapter"))

      // Negative test - arbitrary namespace + incorrect location of namespace
      val caught3 = intercept[java.util.concurrent.ExecutionException] {
        customerdf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("entityDefinitionPath", "Customer.cdm.json/Customer")
          .option("entityDefinitionModelRoot", "billaliaaswronglocation")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      assert(caught3.getCause.getMessage.startsWith( "PersistenceLayer | Could not read '/TrackedEntity.cdm.json' from the 'core' namespace."))

    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test("config.json test with sastoken") {
    try {
      val timestamp1 = new java.sql.Timestamp(System.currentTimeMillis());
      val timestamp2 = new java.sql.Timestamp(System.currentTimeMillis());
      val cdata = Seq(
        Row( timestamp1, timestamp2,1, "A", Decimal(33.5)),
        Row( timestamp1, timestamp2, 2, "B", Decimal(42.1)),
        Row( timestamp1, timestamp2, 3, "C", Decimal(7.90))
      )

      val cschema = new StructType()
        .add(StructField("ValidFrom", TimestampType, true))
        .add(StructField("ValidTo", TimestampType, true))
        .add(StructField("CustomerId", IntegerType, true))
        .add(StructField("CustomerName", StringType, true))
        .add(StructField("CreditLimit", DecimalType(18, 2), true))

      val customerdf = spark.createDataFrame(spark.sparkContext.parallelize(cdata), cschema)

      customerdf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/customer/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("entityDefinitionPath", "Customer.cdm.json/Customer")
        .option("entityDefinitionModelRoot", "billalias")
        .option("configPath", "/config")
        .option("format", "parquet")
        .option("sasToken", sastokenatroot)
        .save()

      customerdf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/customerImplicit/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("format", "parquet")
        .option("sasToken", sastokenatroot)
        .save()

      val custRead = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/customer/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("configPath", "/config")
        .option("sasToken", sastokenatroot)
        .load()

      val pdata = Seq(
        Row( timestamp1, timestamp2,1, "A"),
        Row( timestamp1, timestamp2, 2, "B"),
        Row( timestamp1, timestamp2, 3, "C")
      )

      val pschema = new StructType()
        .add(StructField("ValidFrom", TimestampType, true))
        .add(StructField("ValidTo", TimestampType, true))
        .add(StructField("CustomerId", IntegerType, true))
        .add(StructField("CustomerName", StringType, true))

      val persondf = spark.createDataFrame(spark.sparkContext.parallelize(pdata), pschema)

      persondf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/person/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("entityDefinitionPath", "Person.cdm.json/Person")
        .option("entityDefinitionModelRoot", "billalias")
        .option("sasToken", sastokenatroot)
        .save()

      val personRead = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/person/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("sasToken", sastokenatroot)
        .load()

      assert(custRead.except(customerdf).count() == 0)
      assert(personRead.except(persondf).count() == 0)

      // Negative test - invalid config.json path
      val invalidConfigPath = "/config1"
      var caught = intercept[RuntimeException] {
        customerdf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/configTest/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("entityDefinitionPath", "Customer.cdm.json/Customer")
          .option("entityDefinitionModelRoot", "billalias")
          .option("configPath", invalidConfigPath)
          .option("sasToken", sastokenatroot)
          .save()
      }
      assert(caught.getMessage == String.format(Messages.configJsonPathNotFound, invalidConfigPath))

      // Negative test - arbitrary namespace + config path option not provided on write
      val caught2 = intercept[java.util.concurrent.ExecutionException] {
        customerdf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("entityDefinitionPath", "Customer.cdm.json/Customer")
          .option("entityDefinitionModelRoot", "billaliaswithoutconfig")
          .option("sasToken", sastokenatroot)
          .save()
      }
      assert(caught2.getMessage == String.format(Messages.overrideConfigJson, "java.lang.Exception: StorageManager | Adapter not found for the namespace 'core' | fetchAdapter"))

      // Negative test - arbitrary namespace + incorrect location of namespace
      val caught3 = intercept[java.util.concurrent.ExecutionException] {
        customerdf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
          .option("entity", "TestEntity")
          .option("entityDefinitionPath", "Customer.cdm.json/Customer")
          .option("entityDefinitionModelRoot", "billaliaaswronglocation")
          .option("sasToken", sastokenatroot)
          .save()
      }
      assert(caught3.getCause.getMessage.startsWith( "PersistenceLayer | Could not read '/TrackedEntity.cdm.json' from the 'core' namespace."))
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test("negative test - entityDefinitionStorage with App Creds") {
    val df = testdata.prepareDataWithAllTypes()
    val caught = intercept[java.lang.IllegalArgumentException] {
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeExplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeExplicit")
        .option("entityDefinitionPath", "core/applicationCommon/DataType.cdm.json/DataType")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("entityDefinitionStorage", "abc.dfs.core.windows.net")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()
    }
    assert(caught.getMessage.equals(Messages.entityDefStorageAppCredError))
  }

  test("negative test - entityDefinitionStorage with sastoken") {
    val df = testdata.prepareDataWithAllTypes()
    val caught = intercept[java.lang.IllegalArgumentException] {
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeExplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeExplicit")
        .option("entityDefinitionPath", "core/applicationCommon/DataType.cdm.json/DataType")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("entityDefinitionStorage", "abc.dfs.core.windows.net")
        .option("sasToken", sastokenatroot)
        .save()
    }
    assert(caught.getMessage.equals(Messages.entityDefStorageAppCredError))
  }

  test("Switch to DataTypes") {
    try{
      val date = java.sql.Date.valueOf("2015-03-31");
      val timestamp =  new Timestamp(System.currentTimeMillis());
      val byteVal = 2.toByte
      val shortVal = 129.toShort
      val data = Seq(
        Row("tim", 1, true, 12.34, 6L, date, Decimal(999.00), timestamp, 2f, byteVal, shortVal),
        Row("tddim", 1, false, 13.34, 7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal),
        Row("tddim", 1, false, 13.34, 7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal)
      )

      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))
        .add(StructField("dob", DateType, true))
        .add(StructField("weight", DecimalType(Constants.DECIMAL_PRECISION, 2), true))
        .add(StructField("time", TimestampType, true))
        .add(StructField("float", FloatType, true))
        .add(StructField("byte", ByteType, true))
        .add(StructField("short", ShortType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.show(false)
      // Implicit Parquet Write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeImplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeImplicit")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readImplicit = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeImplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeImplicit")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      readImplicit.show(false)

      // Explicit csv write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeExplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeExplicit")
        .option("entityDefinitionPath", "core/applicationCommon/DataType.cdm.json/DataType")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readExplicit = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeExplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeExplicit")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      readExplicit.show(false)

      // Implicit Overwrite
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeImplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeImplicit")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()

      val readOverwrite = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeImplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeImplicit")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      readOverwrite.show(false);

      // Test dateTimeOffset csv
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/offset/default.manifest.cdm.json")
        .option("entity", "Offset")
        .option("entityDefinitionPath", "core/applicationCommon/DateTimeOffset.cdm.json/DateTimeOffset")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()


      val readOff = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/offset/default.manifest.cdm.json")
        .option("entity", "Offset")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      readOff.show(false);

      val rowDf = df.head()
      val rowsExplicit = readExplicit.head(2)
      val row0 =  rowsExplicit.apply(0)
      val rowsImplicit = readImplicit.head(2)
      val row1 =  rowsImplicit.apply(0)
      val rowsOverwrite = readOverwrite.head(2)
      val row2 =  rowsOverwrite.apply(0)
      val rowsOffset = readOff.head(2)
      val row3 =  rowsOffset.apply(0)

      for (row <- Seq(row0, row1, row2, row3)){
        assert(row.getString(0) == rowDf.getString(0))
        assert(row.getInt(1) == rowDf.getInt(1))
        assert(row.getBoolean(2) == rowDf.getBoolean(2))
        assert(row.getDouble(3) == rowDf.getDouble(3))
        assert(row.getLong(4) == rowDf.getLong(4))
        assert(row.getDate(5) == rowDf.getDate(5))
        assert(row.getDecimal(6) == rowDf.getDecimal(6))
        assert(row.getTimestamp(7) == rowDf.getTimestamp(7))
        assert(row.getFloat(8) == rowDf.getFloat(8))
      }

    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test("Switch to DataTypes with sastoken") {
    try{
      val date = java.sql.Date.valueOf("2015-03-31");
      val timestamp =  new Timestamp(System.currentTimeMillis());
      val byteVal = 2.toByte
      val shortVal = 129.toShort
      val data = Seq(
        Row("tim", 1, true, 12.34, 6L, date, Decimal(999.00), timestamp, 2f, byteVal, shortVal),
        Row("tddim", 1, false, 13.34, 7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal),
        Row("tddim", 1, false, 13.34, 7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal)
      )

      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))
        .add(StructField("dob", DateType, true))
        .add(StructField("weight", DecimalType(Constants.DECIMAL_PRECISION, 2), true))
        .add(StructField("time", TimestampType, true))
        .add(StructField("float", FloatType, true))
        .add(StructField("byte", ByteType, true))
        .add(StructField("short", ShortType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.show(false)
      // Implicit Parquet Write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeImplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeImplicit")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("sasToken", sastokenatroot)
        .save()

      val readImplicit = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeImplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeImplicit")
        .option("sasToken", sastokenatroot)
        .load()
      readImplicit.show(false)

      // Explicit csv write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeExplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeExplicit")
        .option("entityDefinitionPath", "core/applicationCommon/DataType.cdm.json/DataType")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("sasToken", sastokenatroot)
        .save()

      val readExplicit = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeExplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeExplicit")
        .option("sasToken", sastokenatroot)
        .load()

      readExplicit.show(false)

      // Implicit Overwrite
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeImplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeImplicit")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("sasToken", sastokenatroot)
        .mode(SaveMode.Overwrite)
        .save()

      val readOverwrite = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeImplicit/default.manifest.cdm.json")
        .option("entity", "DataTypeImplicit")
        .option("sasToken", sastokenatroot)
        .load()

      readOverwrite.show(false);

      // Test dateTimeOffset csv
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/offset/default.manifest.cdm.json")
        .option("entity", "Offset")
        .option("entityDefinitionPath", "core/applicationCommon/DateTimeOffset.cdm.json/DateTimeOffset")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("sasToken", sastokenatroot)
        .save()


      val readOff = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/offset/default.manifest.cdm.json")
        .option("entity", "Offset")
        .option("sasToken", sastokenatroot)
        .load()

      readOff.show(false);

      val rowDf = df.head()
      val rowsExplicit = readExplicit.head(2)
      val row0 =  rowsExplicit.apply(0)
      val rowsImplicit = readImplicit.head(2)
      val row1 =  rowsImplicit.apply(0)
      val rowsOverwrite = readOverwrite.head(2)
      val row2 =  rowsOverwrite.apply(0)
      val rowsOffset = readOff.head(2)
      val row3 =  rowsOffset.apply(0)

      for (row <- Seq(row0, row1, row2, row3)){
        assert(row.getString(0) == rowDf.getString(0))
        assert(row.getInt(1) == rowDf.getInt(1))
        assert(row.getBoolean(2) == rowDf.getBoolean(2))
        assert(row.getDouble(3) == rowDf.getDouble(3))
        assert(row.getLong(4) == rowDf.getLong(4))
        assert(row.getDate(5) == rowDf.getDate(5))
        assert(row.getDecimal(6) == rowDf.getDecimal(6))
        assert(row.getTimestamp(7) == rowDf.getTimestamp(7))
        assert(row.getFloat(8) == rowDf.getFloat(8))
      }
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test ("CDMSource option") {
    try {
      val df = testdata.prepareDataWithAllTypes();
      val adapter = new AdlsAdapter(storageAccountName, "/" + outputContainer, tenantid, appid, appkey);
      val corpus = new CdmCorpusDefinition
      corpus.getStorage.mount("test", adapter)

      // cdmSource - builtin
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/cdmSourceBuiltIn/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("cdmSource", "BuiltIN")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // cdmSource - Referenced
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/cdmSourceReferenced/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("cdmSource", "Referenced")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // verify the cdm namespace is CdmStandards
      var config = adapter.readAsync("/cdmSourceReferenced/config.json").get()
      corpus.getStorage.mountFromConfig(config);
      assert(corpus.getStorage.fetchAdapter("cdm").isInstanceOf[CdmStandardsAdapter])

      // cdmSource - not specified
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/noCdmSource/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // verify the cdm namespace is CdmStandards
      config = adapter.readAsync("noCdmSource/config.json").get()
      corpus.getStorage.mountFromConfig(config);
      assert(corpus.getStorage.fetchAdapter("cdm").isInstanceOf[CdmStandardsAdapter])

      //  cdmSource - override with configPath
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/cdmSourceConfig/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .option("cdmSource", "Referenced")
        .option("configPath", "/config")
        .save()

      config = adapter.readAsync("cdmSourceConfig/config.json").get()
      corpus.getStorage.mountFromConfig(config);
      assert(corpus.getStorage.fetchAdapter("cdm").isInstanceOf[AdlsAdapter])

      val read1 =  spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/cdmSourceBuiltIn/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      val read2 =  spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/cdmSourceReferenced/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      val read3 =  spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/noCdmSource/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      val read4 =  spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/cdmSourceConfig/default.manifest.cdm.json")
        .option("entity", "TestEntity")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(read1.except(df).count() == 0)
      assert(read2.except(df).count() == 0)
      assert(read3.except(df).count() == 0)
      assert(read4.except(df).count() == 0)

    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }
    finally  {
      cleanup(outputContainer, "/")
    }
  }

  test("Implicit & Explicit Nested parquet arrays") {
    try {
      val df = testdata.prepareNestedDataArrays()
      df.show(false)

      // Implicit Write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/arrayImplicit/default.manifest.cdm.json")
        .option("entity", "ArrayImplicit")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()
      val readImplicit = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/arrayImplicit/default.manifest.cdm.json")
        .option("entity", "ArrayImplicit")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      readImplicit.show(false)

      // Explicit write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/arrayExplicit/default.manifest.cdm.json")
        .option("entity", "ArrayExplicit")
        .option("entityDefinitionPath", "core/applicationCommon/Array.cdm.json/Array")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readExplicit = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/arrayExplicit/default.manifest.cdm.json")
        .option("entity", "ArrayExplicit")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      readExplicit.show(false)

      // Implicit Append
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/arrayImplicit/default.manifest.cdm.json")
        .option("entity", "ArrayImplicit")
        .option("format", "parquet")
        .option("compression", "gzip")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Append)
        .save()

      val readOverwrite = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/arrayImplicit/default.manifest.cdm.json")
        .option("entity", "ArrayImplicit")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      readOverwrite.show(false);

      val rowsExplicit = readExplicit.head(2)
      val row0 = rowsExplicit.apply(0).getStruct(1).getStruct(7).getList(2)
      val rowsImplicit = readImplicit.head(2)
      val row1 = rowsImplicit.apply(1).getStruct(1).getStruct(7).getList(2)

      assert(row0.size() == 2)
      assert(row1.size() == 4)
      for (row <- Seq(row0)) {
        assert(row.get(0).toString == "[RowOneArray1,1,1970-01-01 10:09:08.0]")

        assert(row.get(1).toString == "[RowOneArray2,2,1970-01-01 10:09:08.0]")
      }

      for (row <- Seq(row1)) {
        assert(row.get(0).toString == "[RowTwoArray1,3,1970-01-01 10:09:08.0]")
        assert(row.get(1).toString == "[RowTwoArray2,4,1970-01-01 10:09:08.0]")
        assert(row.get(2).toString == "[RowTwoArray3,5,1970-01-01 10:09:08.0]")
        assert(row.get(3).toString == "[RowTwoArray4,6,1970-01-01 10:09:08.0]")
      }
      assert(readOverwrite.count() == 4)
    }
    catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }
    finally {
      cleanup(outputContainer, "/")
    }
  }

  /**
   * Test append:
   *  1. read a dataframe and write it to the temp output container
   *  2. append a row to the dataframe
   *  3. read the dataframe back (verify it has 15 rows now)
   *  4. Verify appending workd
   *  5. append a row to the dataframe wih ErrorIfExists savemode, which will
   * throw an exception
   */
  test("spark adls savemodeappend") {
    try {
      //1. read original dataframe
      val origdf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", newContainer + "/wwi.manifest.cdm.json")
        .option("entity", "WarehousePackageTypes")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      //2. write dataframe to temporary location
      origdf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/copy-wwi.manifest.cdm.json")
        .option("entity", "NewWarehousePackageTypes")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      //3. append row and write dataframe (SaveMode.Append)
      val unassignedBuyingGroupDf = spark.sql("select 15, 'Container', 1, current_timestamp(), to_timestamp(\"07-01-2019 12 01 19 406\" ,\"MM-dd-yyyy HH mm ss SSS\")")

      unassignedBuyingGroupDf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/copy-wwi.manifest.cdm.json")
        .option("entity", "NewWarehousePackageTypes")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Append)
        .save()
      unassignedBuyingGroupDf.select("*").show()

      //4. Make sure appending worked
      val readdf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/copy-wwi.manifest.cdm.json")
        .option("entity", "NewWarehousePackageTypes")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      assert(origdf.count() + unassignedBuyingGroupDf.count() == readdf.count())

      //5.Try to save without SaveMode.Append
      /* This will throw an Exception */
      var caught = intercept[Exception] {
        unassignedBuyingGroupDf.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/copy-wwi.manifest.cdm.json")
          .option("entity", "NewWarehousePackageTypes")
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      assert(caught.getMessage == "Table `"+ storageAccountName+ "`./output.`copy-wwi.manifest.cdm.json`.NewWarehousePackageTypes already exists")
    }
    catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }
    finally {
      /*cleanup*/
      cleanup(outputContainer, "/")
    }
  }

  test("spark adls read bill ADF data ") {
    try {
      val entities = List("AWCustomer", "Person", "Customer")
      entities.foreach { entity =>
        //1. read original dataframe
        val origdf = spark.read.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", "billdata/root.manifest.cdm.json")
          .option("entity", entity)
          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .load()
        origdf.printSchema()
        origdf.select("*").show()
      }
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
    }
  }

  test("spark adls explicit bill models") {
    val billContainer = "billsamplemodel"
    try {
      val timestamp = new java.sql.Timestamp(System.currentTimeMillis());
      val date= java.sql.Date.valueOf("2018-01-31")
      val data = Seq(
        Row(1,  timestamp, "Jake", "Bisson", date)
      )

      val schema = new StructType()
        .add(StructField("identifier", IntegerType))
        .add(StructField("createdTime", TimestampType))
        .add(StructField("firstName", StringType))
        .add(StructField("lastName", StringType))
        .add(StructField("birthDate", DateType))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
      df.printSchema()
      df.select("*").show()
      println("Performing first write")
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", billContainer + "/data2/root.manifest.cdm.json")
        .option("entity", "Person")
        .option("entityDefinitionPath", "/Contacts/Person.cdm.json/Person")
        .option("entityDefinitionModelRoot", billContainer + "/Models")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      println("Performing append")
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", billContainer + "/data2/root.manifest.cdm.json")
        .option("entity", "Person")
        .option("entityDefinitionPath", "/Contacts/Person.cdm.json/Person")
        .option("entityDefinitionModelRoot", billContainer + "/Models")
        .mode(SaveMode.Append)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      println("Performing read")

      val readdf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", billContainer + "/data2/root.manifest.cdm.json")
        .option("entity", "Person")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      readdf.printSchema()
      readdf.select("*").show()
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(billContainer, "/data2")
    }
  }

  test("spark adls read submanifest regular and glob") {
    try {
      //read an ADF output with a regular expreession
      val origdf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", patternContainer + "/root/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      assert(origdf.count() == 9)

      //read an ADF output with a glob pattern
      val globPattern= spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", patternContainer + "/glob_pattern/root.manifest.cdm.json")
        .option("entity", "TeamMembership" )
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      val gres = globPattern.first()
      assert(gres.getString(0) == "a")
      assert(gres.getString(1) == "b")
      assert(gres.getString(2) == "c")
      assert(gres.getLong(3) == 1L)
    }
    catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    }finally {
    }
  }

  test("spark adls negative lookup") {
    // Search for entities that do not exist
    val entityName = "notanentity"
    var caught = intercept[NoSuchTableException] {
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", newContainer + "/wwi.manifest.cdm.json")
        .option("entity", entityName)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
    }
    assert(caught.getMessage == "Entity " +  entityName + " not found in manifest - wwi.manifest.cdm.json")


    caught = intercept[NoSuchTableException] {
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", oldModelContainer + "/model.json")
        .option("entity", entityName)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
    }
    assert(caught.getMessage == "Entity " +  entityName + " not found in manifest - model.json")

    // Search for an entity that does not exist in a sub manifest
    caught = intercept[NoSuchTableException] {
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", patternContainer + "/root/root.manifest.cdm.json")
        .option("entity", entityName)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
    }
    assert(caught.getMessage == "Entity " +  entityName + " not found in manifest - root.manifest.cdm.json")

    // Search for entities that do not exist
    val manifestName = "wwi-nonexisting.manifest.cdm.json"
    caught = intercept[NoSuchTableException] {
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", newContainer + "/" + manifestName)
        .option("entity", entityName)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
    }
    assert(caught.getMessage == "Manifest doesn't exist: " + manifestName)
  }


  test("spark adls preserve property") {
    try {
      val manifestName = "default"
      val entityName = "TeamMembership"
      createManifestTeamMembershipEntity(metadataContainer, manifestName, entityName)
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", metadataContainer + "/" + manifestName + ".manifest.cdm.json")
        .option("entity", entityName)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
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
        .option("storage", storageAccountName)
        .option("manifestPath", metadataContainer + "/" + manifestName + ".manifest.cdm.json")
        .option("entity", entityName + "save")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val rereaddf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", metadataContainer + "/" + manifestName + ".manifest.cdm.json")
        .option("entity", entityName + "save")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      //compare the original metadata object with the ones we wrote out when we save the dataframe
      assert(rereaddf.schema.fields(0).name == "teamMembershipId")
      assert(rereaddf.schema.fields(0).name == df.schema.fields(0).name)
      //verify the new column's name
      assert(rereaddf.schema.fields(4).name == "versionNumberNew")

    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(metadataContainer, "/")
    }
  }


  test("create submanifest scratch") {
    val entity = "TestEntity"
    val entity2 = "TestEntity2"
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
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", entity)
        .option("useSubManifest", true)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      //Create a new manifest and add the entity to it
      val readDF = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", entity)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      readDF.select("*").show()
      assert(readDF.count == data.size)

      //Create a new manifest and add the entity to it
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", entity2)
        .option("useSubManifest", true)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      //Create a new manifest and add the entity to it
      val readDF2 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", entity2)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      assert(readDF2.count == data.size)
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      /* cleanup*/
      cleanup(outputContainer, "/")
    }
  }


  test("Add implicit entity to a manifest that already has entity created explicitly ") {
    try {
      val date = java.sql.Date.valueOf("2015-03-31");
      val timestamp = new Timestamp(System.currentTimeMillis());
      val byteVal = 2.toByte
      val shortVal = 129.toShort
      val data = Seq(
        Row("tim", 1, true, 12.34, 6L, date, Decimal(999.00), timestamp, 2f, byteVal, shortVal),
        Row("tddim", 1, false, 13.34, 7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal),
        Row("tddim", 1, false, 13.34, 7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal)
      )

      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))
        .add(StructField("dob", DateType, true))
        .add(StructField("weight", DecimalType(Constants.DECIMAL_PRECISION, 2), true))
        .add(StructField("time", TimestampType, true))
        .add(StructField("float", FloatType, true))
        .add(StructField("byte", ByteType, true))
        .add(StructField("short", ShortType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Explicit csv write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "DataTypeExplicit")
        .option("entityDefinitionPath", "core/applicationCommon/DataType.cdm.json/DataType")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val df1 = testdata.prepareDataWithAllTypes()
      // Implicit csv write to same manifest
      df1.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "A")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // Read entity created explicitly
      val readDF1 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "DataTypeExplicit")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      // Read entit created implicitly
      val readDF2 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root/default.manifest.cdm.json")
        .option("entity", "A")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(readDF1.except(df).count() == 0)
      assert(readDF2.except(df1).count() == 0)
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/root")
    }
  }


  test("Add CDM entity to existing CDM folder one level up") {
    try {
      val date = java.sql.Date.valueOf("2015-03-31");
      val timestamp =  new Timestamp(System.currentTimeMillis());
      val byteVal = 2.toByte
      val shortVal = 129.toShort
      val data = Seq(
        Row("tim", 1, true, 12.34, 6L, date, Decimal(999.00), timestamp, 2f, byteVal, shortVal),
        Row("tddim", 1, false, 13.34, 7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal),
        Row("tddim", 1, false, 13.34, 7L, date, Decimal(3.3), timestamp, 3.59f, byteVal, shortVal)
      )

      val schema = new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("id", IntegerType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))
        .add(StructField("dob", DateType, true))
        .add(StructField("weight", DecimalType(Constants.DECIMAL_PRECISION, 2), true))
        .add(StructField("time", TimestampType, true))
        .add(StructField("float", FloatType, true))
        .add(StructField("byte", ByteType, true))
        .add(StructField("short", ShortType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Explicit csv write
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
        .option("entity", "DataTypeExplicit")
        .option("entityDefinitionPath", "core/applicationCommon/DataType.cdm.json/DataType")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val df1 = testdata.prepareDataWithAllTypes()
      df1.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeExplicit/default.manifest.cdm.json")
        .option("entity", "B")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readDF1 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dataTypeExplicit/default.manifest.cdm.json")
        .option("entity", "B")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      val readDF2 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/default.manifest.cdm.json")
        .option("entity", "DataTypeExplicit")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(readDF1.except(df1).count() ==0)
      assert(readDF2.except(df).count() ==0)

    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test("append column to standard schema entity for ADF") {

    val outputSubContainer = "outputsubmanifest"
    try {
      createStandardSchemaEntityAsSubManifest(outputSubContainer, "/root")

      //Create a new manifest and add the entity to it
      val df = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputSubContainer + "/root/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      df.select("*").show()

      val newColumn = df.col("teamMembershipId").as("teamMembershipIdNew")
      val newdf = df.withColumn("teamMembershipIdNew", newColumn)
      //Create a new manifest and add the entity to it

      // Note, this is an ADF-specific test, where there entity name must match the SchemaDocuments entity name, which
      // is why the name of the entity remains the same
      newdf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputSubContainer + "/root2/root.manifest.cdm.json")
        .option("derivedFromEntity", "TeamMembership")
        .option("entity", "TeamMembership")
        .option("useSubManifest", true)
        .option("useCdmStandardModelRoot", true)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputSubContainer + "/root2/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(readDf.columns.size == df.columns.size + 1)
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputSubContainer, "/root")
      cleanup(outputSubContainer, "/root2")
    }
  }

  test("Predefined entity creation at root level") {

    try {
      val data = Seq(
        Row("1", "2", "3", 4L),
        Row("5", "6", "7", 8L)
      )

      val schema = new StructType()
        .add(StructField("teamMembershipId", StringType, true))
        .add(StructField("systemUserId", StringType, true))
        .add(StructField("teamId", StringType, true))
        .add(StructField("versinNumber", LongType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)

      // Two Negative tests: useCdmStandardModelRoot and entityDefinition<ModelRoot|Container> cannot be used together
      var caught = intercept[Exception] {
        df.write.format("com.microsoft.cdm")
          .option("storage", storageAccountName)
          .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
          .option("entity", "TeamMembership")
          .option("entityDefinitionPath", "core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
          .option("entityDefinitionModelRoot", "outputsubmanifest")
          .option("useCdmStandardModelRoot", true)

          .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
          .save()
      }
      assert(caught.getMessage == Messages.invalidBothStandardAndEntityDefCont)

     //  Using github for CDM entities is explicit
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("useCdmStandardModelRoot", true)
        .option("entityDefinitionPath", "core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // Specify entityDefinitionModelRoot explicity and use default
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "KnowledgeArticleCategory")
        .option("entityDefinitionPath", "core/applicationCommon/KnowledgeArticleCategory.cdm.json/KnowledgeArticleCategory")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()
      // Perform the same write as above, but use the default "/" for a non-specified entityDefinitionModelRoot
      // The corpus in outputsubmanifest is at "/" and "/example-public-standards"
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "KnowledgeArticleCategory")
        .option("entityDefinitionPath", "core/applicationCommon/KnowledgeArticleCategory.cdm.json/KnowledgeArticleCategory")
        .option("entityDefinitionModelRoot", "outputsubmanifest")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Append)
        .save()

      // Default entityDefinitionModelRoot
      // Default entityDefinitionModelRoot (defaults to "manifestPath" param)
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "outputsubmanifest/rootPredefined/root.manifest.cdm.json")
        .option("entity", "KnowledgeArticleCategory")
        .option("entityDefinitionPath", "core/applicationCommon/KnowledgeArticleCategory.cdm.json/KnowledgeArticleCategory")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      // Perform the same write as above, but use the same modelRoot, which is also one-level lower
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "/outputsubmanifest/rootPredefined/root.manifest.cdm.json")
        .option("entity", "KnowledgeArticleCategory")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Append)
        .save()

      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      // No useCDM and use entityDefinitionModelRoot's default "/"
      val readDf2 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "KnowledgeArticleCategory")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      // No useCDM and use entityDefinitionModelRoot's default "/" entityModelRoot's default "manifestPah" param
      val readDf3 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", "outputsubmanifest/rootPredefined/root.manifest.cdm.json")
        .option("entity", "KnowledgeArticleCategory")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(readDf.collect.size == 2)
      assert(readDf2.collect.size == 4)
      assert(readDf3.collect.size == 4)
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
      cleanup("outputsubmanifest", "/rootPredefined/config.json")
      cleanup("outputsubmanifest", "/rootPredefined/root.manifest.cdm.json")
      cleanup("outputsubmanifest", "/rootPredefined/KnowledgeArticleCategory")
    }
  }

  test("Predefined entity creation with submanifests") {

    try {
      val data = Seq(
        Row("1", "2", "3", 4L),
        Row("5", "6", "7", 8L)
      )
      val schema = new StructType()
        .add(StructField("teamMembershipId", StringType, true))
        .add(StructField("systemUserId", StringType, true))
        .add(StructField("teamId", StringType, true))
        .add(StructField("versinNumber", LongType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("entityDefinitionPath", "core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
        .option("useCdmStandardModelRoot", true)
        .option("useSubManifest", true)
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .save()

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "KnowledgeArticleCategory")
        .option("entityDefinitionPath", "core/applicationCommon/KnowledgeArticleCategory.cdm.json/KnowledgeArticleCategory")
        .option("entityDefinitionModelRoot", "outputsubmanifest/example-public-standards")
        .option("useSubManifest", true)
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .save()

      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .load()

      val readDf2 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "KnowledgeArticleCategory")
        .option("appId", appid)
        .option("appKey", appkey)
        .option("tenantId", tenantid)
        .load()

      assert(readDf.collect.size == 2)
      assert(readDf2.collect.size == 2)
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test("SaveMode: scratch Overwrite") {
    try {
      val data1 = Seq(
        Row("1", "1", "1", 1L),
        Row("2", "2", "2", 2L),
        Row("3", "3", "3", 3L),
        Row("4", "4", "4", 4L),
        Row("5", "5", "5", 5L),
        Row("6", "6", "6", 6L)
      )

      val data2 = Seq(
        Row("7", "7", "7", 7L),
        Row("8", "8", "8", 8L),
        Row("9", "9", "9", 9L),
        Row("0", "0", "0", 0L)
      )

      val schema = new StructType()
        .add(StructField("teamMembershipId", StringType, true))
        .add(StructField("systemUserId", StringType, true))
        .add(StructField("teamId", StringType, true))
        .add(StructField("versionNumber", LongType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data1, 3), schema)
      val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2, 2), schema)

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dir/bar/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("useSubManifest", true)
        .option("format","parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      df2.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dir/root.manifest.cdm.json")
        .option("useSubManifest", true)
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dir/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(readDf.collect.size == 4)

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dir2/dir3/root2.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      df2.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dir2/dir3/root2.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid) .option("appKey", appkey) .option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()

      val readDf2 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/dir2/dir3/root2.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      assert(readDf2.collect.size == 4)
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test("SaveMode: predefined Overwrite") {
    try {
      val data1 = Seq(
        Row("1", "1", "1", 1L),
        Row("2", "2", "2", 2L),
        Row("3", "3", "3", 3L),
        Row("4", "4", "4", 4L),
        Row("5", "5", "5", 5L),
        Row("6", "6", "6", 6L)
      )

      val data2 = Seq(
        Row("7", "7", "7", 7L),
        Row("8", "8", "8", 8L),
        Row("9", "9", "9", 9L),
        Row("0", "0", "0", 0L)
      )

      val schema = new StructType()
        .add(StructField("teamMembershipId", StringType, true))
        .add(StructField("systemUserId", StringType, true))
        .add(StructField("teamId", StringType, true))
        .add(StructField("versionNumber", LongType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data1, 3), schema)
      val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2, 2), schema)

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("entityDefinitionPath", "core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
        .option("useCdmStandardModelRoot", true)
        .option("useSubManifest", true)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      df2.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("entityDefinitionPath", "core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
        .option("useCdmStandardModelRoot", true)
        .option("useSubManifest", true)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()

      val readDf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      assert(readDf.collect.size == 4)

      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root2.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("entityDefinitionPath", "core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
        .option("useCdmStandardModelRoot", true)
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      df2.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root2.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("entityDefinitionPath", "/core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
        .option("useCdmStandardModelRoot", true)
        .option("appId", appid) .option("appKey", appkey) .option("tenantId", tenantid)
        .mode(SaveMode.Overwrite)
        .save()

      val readDf2 = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root2.manifest.cdm.json")
        .option("entity", "TeamMembership")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      assert(readDf2.collect.size == 4)
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  test("spark CSV read compatibility") {
    val readDateContainer= "testreaddate"
    try {
      val timestamp = new java.sql.Timestamp(System.currentTimeMillis());
      val data = Seq(
        Row("commentidval", "articleIdVal", "titleval", "commentTextVal", timestamp, "createdByVal", timestamp,
          "modifiedBy", 1L, "organizationIdVal","createdOnBehalfOfVal", "modifiedOnBehalfOfVal")
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
        .add(StructField("versionNumber", LongType, true))
        .add(StructField("organizationId", StringType, true))
        .add(StructField("createdOnBehalfBy", StringType, true))
        .add(StructField("modifiedOnBehalfBy", StringType, true))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 3), schema)
      df.select("*").show()
      df.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "ArticleComment")
        .option("entityDefinitionPath", "/core/applicationCommon/ArticleComment.cdm.json/ArticleComment")
        .option("useCdmStandardModelRoot", true)
        .option("appId", appid) .option("appKey", appkey) .option("tenantId", tenantid)
        .save()

      val newreadDF = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/root.manifest.cdm.json")
        .option("entity", "ArticleComment")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      newreadDF.select("*").show()
      val dfres = df.first()
      val newres = newreadDF.first()
      assert(dfres.getString(0) == newres.getString(0))
      assert(dfres.getString(1) == newres.getString(1))
      assert(dfres.getString(2) == newres.getString(2))
      assert(dfres.getString(3) == newres.getString(3))
      assert(dfres.getTimestamp(4) == newres.getTimestamp(4))
      assert(dfres.getString(5) == newres.getString(5))
      assert(dfres.getTimestamp(6) == newres.getTimestamp(6))
      assert(dfres.getString(7) == newres.getString(7))
      assert(dfres.getLong(8) == newres.getLong(8))
      assert(dfres.getString(9) == newres.getString(9))
      assert(dfres.getString(10) == newres.getString(10))

      val origdf = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", readDateContainer + "/wwi.manifest.cdm.json")
        .option("entity", "SalesBuyingGroups")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()
      origdf.select("*").show(false)

      /* Write and read CSV */
      origdf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/wwiout.manifest.cdm.json")
        .option("entity", "second")
        .option("appId", appid) .option("appKey", appkey) .option("tenantId", tenantid)
        .save()

      val readCSV = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/wwiout.manifest.cdm.json")
        .option("entity", "second")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      readCSV.printSchema()
      readCSV.select("*").show(false)

      /* Write and read Parquet */
      origdf.write.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/wwiparquet.manifest.cdm.json")
        .option("entity", "third")
        .option("format", "parquet")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .save()

      val readParquet = spark.read.format("com.microsoft.cdm")
        .option("storage", storageAccountName)
        .option("manifestPath", outputContainer + "/wwiparquet.manifest.cdm.json")
        .option("entity", "third")
        .option("appId", appid).option("appKey", appkey).option("tenantId", tenantid)
        .load()

      readParquet.select("*").show(false)

      val origdfres = origdf.first()
      val readDFres= readCSV.first()
      val readParquetres= readParquet.first()


      for (row <- Seq(readDFres, readParquetres)){
        assert(origdfres.getDate(0) == row.getDate(0))
        assert(origdfres.getDate(1) == row.getDate(1))
        assert(origdfres.getTimestamp(2) == row.getTimestamp(2))
        assert(origdfres.getTimestamp(3) == row.getTimestamp(3))
        assert(origdfres.getTimestamp(4) == row.getTimestamp(4))
        assert(origdfres.getTimestamp(5) == row.getTimestamp(5))
        assert(origdfres.getTimestamp(6) == row.getTimestamp(6))
        assert(origdfres.getTimestamp(7) == row.getTimestamp(7))
        assert(origdfres.getTimestamp(8) == row.getTimestamp(8))
        assert(origdfres.getTimestamp(9) == row.getTimestamp(9))
        assert(origdfres.getTimestamp(10) == row.getTimestamp(10))
        assert(origdfres.getTimestamp(11) == row.getTimestamp(11))
        assert(origdfres.getTimestamp(12) == row.getTimestamp(12))
        assert(origdfres.getTimestamp(13) == row.getTimestamp(13))
        assert(origdfres.getTimestamp(14) == row.getTimestamp(14))
        assert(origdfres.getTimestamp(15) == row.getTimestamp(15))
        assert(origdfres.getTimestamp(16) == row.getTimestamp(16))
        assert(origdfres.getTimestamp(17) == row.getTimestamp(17))
        assert(origdfres.getTimestamp(18) == row.getTimestamp(18))
        assert(origdfres.getTimestamp(19) == row.getTimestamp(19))
      }
    } catch {
      case e: Exception=> {
        println("Exception: " + e.printStackTrace())
        assert(false)
      };
    } finally {
      cleanup(outputContainer, "/")
    }
  }

  /*
   * Utility functions
   */
  def createManifestTeamMembershipEntity(containerIn: String, manifestName: String, entityName: String) = {
    val container = "/" + containerIn

    val cdmCorpus = new CdmCorpusDefinition()

    val adlsAdapter = new AdlsAdapter(storageAccountName, container, tenantid, appid, appkey)
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")
    cdmCorpus.getStorage.mount("cdm", new CdmStandardsAdapter())

    val manifestAbstract = cdmCorpus.makeObject(CdmObjectType.ManifestDef, "tempAbstract").asInstanceOf[CdmManifestDefinition]
    manifestAbstract.getEntities.add(entityName, "cdm:/core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
    val localRoot = cdmCorpus.getStorage.fetchRootFolder("adls")
    localRoot.getDocuments.add(manifestAbstract)
    println("Resolve the placeholder")
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

      val location = "adls:/" + entDef.getEntityName + "/partition-data.csv"
      part.setLocation(cdmCorpus.getStorage.createRelativeCorpusPath(location, manifestResolved))

      // Add trait to partition for csv params.
      val csvTrait = part.getExhibitsTraits.add("is.partition.format.CSV")
      csvTrait.asInstanceOf[CdmTraitReference].getArguments.add("columnHeaders", "true")
      csvTrait.asInstanceOf[CdmTraitReference].getArguments.add("delimiter", ",")

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
      val serConf = SerializedABFSHadoopConf.getConfiguration(storageAccountName, container, AppRegAuth(appid, appkey, tenantid), conf)
      val path = new Path("/"+part.getLocation)
      val fs = path.getFileSystem(serConf.value)
      val outputStream: FSDataOutputStream = fs.create(path)
      outputStream.writeBytes(header)
      outputStream.close()
    }
    manifestResolved.saveAsAsync(manifestResolved.getManifestName + ".manifest.cdm.json", true).get()
  }

  def createStandardSchemaEntityAsSubManifest(containerIn:String, rpath:String): Unit = {
    val rootPath= "/" + containerIn + rpath

    val adlsAdapter = new AdlsAdapter(storageAccountName, rootPath, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")
    cdmCorpus.getStorage.mount("cdm", new CdmStandardsAdapter())

    val entityName = "TeamMembership"
    val subManifest= cdmCorpus.makeObject(CdmObjectType.ManifestDef, entityName).asInstanceOf[CdmManifestDefinition]
    subManifest.getEntities.add(entityName, "cdm:/core/applicationCommon/TeamMembership.cdm.json/TeamMembership")
    val folder= cdmCorpus.getStorage.fetchRootFolder("adls")
    val subFolder = folder.getChildFolders.add(entityName)
    subFolder.getDocuments.add(subManifest)
    val manifestResolved = subManifest.createResolvedManifestAsync(entityName, "").get

    manifestResolved.getImports.add("cdm:/foundations.cdm.json", "")
    val entityDecl = manifestResolved.getEntities.asScala.find(_.getEntityName == entityName).get
    val entDef = cdmCorpus.fetchObjectAsync[CdmEntityDefinition](entityDecl.getEntityPath, manifestResolved).get

    val part = cdmCorpus.makeObject(CdmObjectType.DataPartitionDef, entDef.getEntityName + "-partition").asInstanceOf[CdmDataPartitionDefinition]
    entityDecl.getDataPartitions.add(part)
    part.setExplanation("sample")

    val location = "adls:/" + entDef.getEntityName + "/output/partition-data.csv"
    part.setLocation(cdmCorpus.getStorage.createRelativeCorpusPath(location, manifestResolved))

    // Add trait to partition cfor csv params.
    val csvTrait = part.getExhibitsTraits.add("is.partition.format.CSV")
    csvTrait.asInstanceOf[CdmTraitReference].getArguments.add("columnHeaders", "true")
    csvTrait.asInstanceOf[CdmTraitReference].getArguments.add("delimiter", ",")


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

    val serConf = SerializedABFSHadoopConf.getConfiguration(storageAccountName, "/" + containerIn, AppRegAuth(appid, appkey, tenantid), conf)
    val path = new Path(rpath + "/" + entDef.getEntityName + "/" + part.getLocation)
    val fs = path.getFileSystem(serConf.value)
    val outputStream: FSDataOutputStream = fs.create(path)
    outputStream.writeBytes(header+"\n")
    outputStream.writeBytes("1,2,3,4")
    outputStream.close()

    // create the manifest from scratch and add it to the root folder
    val rootManifest = cdmCorpus.makeObject(CdmObjectType.ManifestDef, "root").asInstanceOf[CdmManifestDefinition]
    folder.getDocuments.add(rootManifest)
    addSubManifestToRootManifestAndSave(cdmCorpus, entityName, rootManifest, subManifest)

    val readManifest= cdmCorpus.fetchObjectAsync("root.manifest.cdm.json").get().asInstanceOf[CdmManifestDefinition]
    assert(readManifest.getSubManifests.asScala.size == 1)
    val subManifestDecl = readManifest.getSubManifests.asScala.find(_.getManifestName== entityName).get
    val subManDef = cdmCorpus.fetchObjectAsync(subManifestDecl.getDefinition, readManifest).join.asInstanceOf[CdmManifestDefinition]
    assert(subManDef.getEntities.size() == 1)
  }

  //add the submanifest to the root manifest
  def addSubManifestToRootManifestAndSave(cdmCorpus: CdmCorpusDefinition, entityName:String, rootManifest:CdmManifestDefinition, subManifest:CdmManifestDefinition): Unit = {
    val subManifestDecl2 = cdmCorpus.makeObject(CdmObjectType.ManifestDeclarationDef, entityName, false).asInstanceOf[CdmManifestDeclarationDefinition]
    val subManifestPath2 = cdmCorpus.getStorage.createRelativeCorpusPath(subManifest.getAtCorpusPath, rootManifest)
    subManifestDecl2.setDefinition(subManifestPath2)
    rootManifest.getSubManifests.add(subManifestDecl2)
    rootManifest.saveAsAsync("root.manifest.cdm.json", true).get
  }



}
