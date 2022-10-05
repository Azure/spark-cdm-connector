package com.microsoft.cdm.test

import java.util
import java.util.Arrays

import com.microsoft.cdm.DefaultSource
import com.microsoft.cdm.utils.StructTypeMetadata._
import com.microsoft.cdm.utils.{AppRegAuth, Auth, CDMDataFormat, CDMDataType, Constants, SerializedABFSHadoopConf}
import com.microsoft.commondatamodel.objectmodel.cdm._
import com.microsoft.commondatamodel.objectmodel.enums.{CdmDataFormat, CdmObjectType}
import com.microsoft.commondatamodel.objectmodel.persistence.cdmfolder.TypeAttributePersistence
import com.microsoft.commondatamodel.objectmodel.resolvedmodel.ResolveContext
import com.microsoft.commondatamodel.objectmodel.storage.{AdlsAdapter, GithubAdapter, LocalAdapter, ResourceAdapter}
import com.microsoft.commondatamodel.objectmodel.utilities.{AttributeResolutionDirectiveSet, JMapper, ResolveOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructType}
import org.scalatest.{FunSuite, PrivateMethodTester}

import scala.util.parsing.json._
import scala.collection.JavaConverters._

class CDMUnitTests extends FunSuite with PrivateMethodTester  {

  private val appid = System.getenv("APPID")
  private val appkey = System.getenv("APPKEY")
  private val tenantid = System.getenv("TENANTID")
  val storage = System.getenv("ACCOUNT_URL")

  test("Logical Import required after resolution"){
    /* Createa an entity where the logical entity was in a separate location from the cdm corpus */
    val manifestFileName = "default.manifest.cdm.json"
    val cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("cdm", new GithubAdapter)

    val localModelRoot= new AdlsAdapter(storage, "/bill/Models",tenantid,appid,appkey)
    cdmCorpus.getStorage.mount("modelRoot", localModelRoot)

    val localDataRoot = new AdlsAdapter(storage, "../bill/data",tenantid,appid,appkey)
    cdmCorpus.getStorage.mount("adls", localDataRoot)

    cdmCorpus.getStorage.setDefaultNamespace("adls")

    val folder = cdmCorpus.getStorage.fetchRootFolder("adls")
    val entitySubFolder = folder.getChildFolders.add("Person")

    val eDef = cdmCorpus.fetchObjectAsync("modelRoot:" + "/Contacts/Person.cdm.json/Person").get.asInstanceOf[CdmEntityDefinition]
    val entDefResolved = eDef.createResolvedEntityAsync("Person", new ResolveOptions(eDef), entitySubFolder).get

    val manifest = cdmCorpus.makeObject(CdmObjectType.ManifestDef, "default").asInstanceOf[CdmManifestDefinition]
    manifest.getImports.add("cdm:/foundations.cdm.json", null)
    folder.getDocuments.add(manifest, manifestFileName)

    val doc = entDefResolved.getInDocument()
    doc.getImports.add("cdm:/foundations.cdm.json")

    manifest.getEntities.add(entDefResolved)
    manifest.saveAsAsync(manifestFileName, true).get


    /*This resolved entity still requires the modelRoot for resolution. Why*/
    for (i <- Seq("first", "second")) {
      val readCdmCorpus = new CdmCorpusDefinition()
      readCdmCorpus.getStorage.mount("cdm", new GithubAdapter)


      if (i.equals("first")) {
        println("***\nIncluding modelRoot from Write\n***\n")
        val localModelRoot2 = new AdlsAdapter(storage,"/bill/Models",tenantid,appid,appkey)
        readCdmCorpus.getStorage.mount("modelRoot", localModelRoot2)
      } else {
        println("***\nNot including modelRoot from Write\n***\n")
      }


      val localDataRootRead = new AdlsAdapter(storage, "/bill/data",tenantid,appid,appkey)
      readCdmCorpus.getStorage.mount("adls", localDataRootRead)
      println("***\nFetching entity\n***\n")

      val fetchManifest= cdmCorpus.fetchObjectAsync(manifestFileName, null, true).get().asInstanceOf[CdmManifestDefinition]
      val entity = manifest.getEntities().asScala.find(_.getEntityName == "Person").get
      val relPath = entity.getEntityPath
      readCdmCorpus.fetchObjectAsync(relPath,fetchManifest, true).get().asInstanceOf[CdmEntityDefinition]

    }
  }

  test("read from Config") {
    val container = "/output"
    val adlsAdapter = new AdlsAdapter(storage, container, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()

    cdmCorpus.getStorage.setDefaultNamespace("adls")
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    val result = cdmCorpus.getStorage.fetchAdapter("adls").readAsync("adls:/nestedImplicit/config.json").get()
    val json = JSON.parseFull(result)
    print(json)
  }

  test("Create a resolved entity from a referenced cdm entity"){
    val container = "/output"
    val adlsAdapter = new AdlsAdapter(storage, container, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()

    cdmCorpus.getStorage.setDefaultNamespace("adls")
    cdmCorpus.getStorage.mount("cdm", new ResourceAdapter)

    val eDef = cdmCorpus.fetchObjectAsync("/core/applicationCommon/ArticleComment.cdm.json/ArticleComment").get.asInstanceOf[CdmEntityDefinition]
    val a = eDef.createResolvedEntityAsync("foo").get
    assert(a.getAttributes.asScala.size == 12)
    a.getAttributes.asScala.map(attr => {
      val attrDef = attr.asInstanceOf[CdmTypeAttributeDefinition]
      println(CDMDataFormat.withName(attrDef.fetchDataFormat().name()))
    })
  }

  test("Preserve entity-level metdata") {

    //HashMap is null
    val nullSchema= new StructType()
    assert(nullSchema.getMetadata("entity1") == Metadata.empty)

    val schema = new StructType()
    schema.setMetadata("entity1", new MetadataBuilder().putBoolean("key",true).build())
    val md1= schema.getMetadata("entity1")

    val schema2 = new StructType()
    schema2.setMetadata("entity2", new MetadataBuilder().putBoolean("key",false).build())
    val md2= schema2.getMetadata("entity2")

    val mdOrig= schema.getMetadata("entity1")

    val schemaOrigCopy = schema
    val mdOrigCopy =schemaOrigCopy.getMetadata("entity1")

    // Verify metadata is preserved across new instances and copies of a StructType
    // It's the key (i.e., "entity1") that determines which metadat object is returned
    assert(md1.getBoolean("key") == mdOrig.getBoolean("key") == mdOrigCopy.getBoolean("key") == true)
    assert(md1.getBoolean("key") != md2.getBoolean("key") )

    // No entity is found, so neither is Metadata
    val mdNotExist=schemaOrigCopy.getMetadata("not exist")
    assert(mdNotExist == Metadata.empty)

    // we can overwrite it
    schema.setMetadata("entity1", new MetadataBuilder().putBoolean("key",false).build())
    assert(schema.getMetadata("entity1").getBoolean("key") == false)
  }

  test ("create standard schema-based entity sub manifest") {
    val container = "/globaltestfeb6/root"

    val conf = new Configuration()
    val adlsAdapter = new AdlsAdapter(storage, container, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")
    cdmCorpus.getStorage.mount("cdm", new GithubAdapter)

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

    val location = "adls:/" + entDef.getEntityName + "/data/partition-data.csv"
    part.setLocation(cdmCorpus.getStorage.createRelativeCorpusPath(location, manifestResolved))

    // Add trait to partition for csv params.
    val csvTrait = part.getExhibitsTraits.add("is.partition.format.CSV")
    csvTrait.asInstanceOf[CdmTraitReference].getArguments.add("columnHeaders", "true")
    csvTrait.asInstanceOf[CdmTraitReference].getArguments.add("delimiter", ",")

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
    val serConf = SerializedABFSHadoopConf.getConfiguration(storage, "/globaltestfeb6" , AppRegAuth(appid, appkey, tenantid), conf)
    val path = new Path("/root/"+entDef.getEntityName+"/"+part.getLocation)
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

  test ("create/add scratch sub manifest") {
    val container = "/output"
    val adlsAdapter = new AdlsAdapter(storage, container, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")
    cdmCorpus.getStorage.mount("cdm", new GithubAdapter)

    val entityName = "ATestEntity"
    val subManifest = createSubmanifest(cdmCorpus, entityName)
    // create the manifest from scratch and add it to the root folder
    val rootManifest = cdmCorpus.makeObject(CdmObjectType.ManifestDef, "root").asInstanceOf[CdmManifestDefinition]
    val folder = cdmCorpus.getStorage.fetchRootFolder("adls")
    folder.getDocuments.add(rootManifest)
    addSubManifestToRootManifestAndSave(cdmCorpus, entityName, rootManifest, subManifest)

    val entityName2 = "SecondEntity"
    val subManifest2 = createSubmanifest(cdmCorpus, entityName2)
    val readRootManifest = cdmCorpus.fetchObjectAsync("root.manifest.cdm.json").get().asInstanceOf[CdmManifestDefinition]
    addSubManifestToRootManifestAndSave(cdmCorpus, entityName2, readRootManifest, subManifest2)

    val readManifest= cdmCorpus.fetchObjectAsync("root.manifest.cdm.json").get().asInstanceOf[CdmManifestDefinition]
    assert(readManifest.getSubManifests.asScala.size == 2)

  }

  test("check data format for LongType") {
    val container = "/output"
    val adlsAdapter = new AdlsAdapter(storage, container, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("local", new LocalAdapter("../../data"))
    cdmCorpus.getStorage.setDefaultNamespace("local")
    val attrDef = cdmCorpus.makeObject(CdmObjectType.TypeAttributeDef, "Long").asInstanceOf[CdmTypeAttributeDefinition]
    attrDef.updateDataFormat(CdmDataFormat.valueOf("Int64"));
    assert("Int64" == attrDef.fetchDataFormat().toString)
  }

  test("create resolved entities with resolution guidance") {
    val container = "/output"
    val adlsAdapter = new AdlsAdapter(storage, container, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("cdm", new GithubAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")
    cdmCorpus.getStorage.mount("adls",adlsAdapter)
    //cdmCorpus.setDefaultResolutionDirectives(new AttributeResolutionDirectiveSet())
    val folder = cdmCorpus.getStorage.fetchRootFolder("adls")
    val ent = cdmCorpus.fetchObjectAsync("adls:/logical-test.cdm.json/Person").get().asInstanceOf[CdmEntityDefinition]
    val entDefResolved = ent.createResolvedEntityAsync("PersonResolved").get

    val manifestFileName = "default.manifest.cdm.json"
    val manifest = cdmCorpus.makeObject(CdmObjectType.ManifestDef, manifestFileName).asInstanceOf[CdmManifestDefinition]
    manifest.getImports.add("cdm:/foundations.cdm.json", null)
    folder.getDocuments.add(manifest, manifestFileName)
    manifest.getEntities.add(entDefResolved)
    manifest.setManifestName("default")
    if(manifest.saveAsAsync(manifestFileName, true).get()) {
      // assert(entDefResolved.getAttributes.size() == ent.getAttributes.size())
      print("Saved")
    }
  }
  test("datatypes") {
    val container = "/output"
    val adlsAdapter = new AdlsAdapter(storage, container, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")

    // Create a attribute and add it to the entity
    val attrDef = cdmCorpus.makeObject(CdmObjectType.TypeAttributeDef, "name").asInstanceOf[CdmTypeAttributeDefinition]
    attrDef.asInstanceOf[CdmTypeAttributeDefinition].setDataType(cdmCorpus.makeRef(CdmObjectType.DataTypeRef, "string", true))
    val docAbs = cdmCorpus.makeObject(CdmObjectType.DocumentDef,  "logical-test.cdm.json").asInstanceOf[CdmDocumentDefinition]
    docAbs.getImports.add("cdm:/foundations.cdm.json", null)
    val folder = cdmCorpus.getStorage.fetchRootFolder("adls")
    folder.getDocuments.add(docAbs,docAbs.getName)
    val entDef = docAbs.getDefinitions.add(CdmObjectType.EntityDef, "logical-test").asInstanceOf[CdmEntityDefinition]
    entDef.asInstanceOf[CdmEntityDefinition].getAttributes().add(attrDef)

    // Resolve the entity
    val resolveOpts = new ResolveOptions(docAbs)
    resolveOpts.setDirectives(new AttributeResolutionDirectiveSet(new util.HashSet[String](Arrays.asList("referenceOnly", "normalized"))))
    val entDefResolved = entDef.createResolvedEntityAsync("resolved", resolveOpts).get

    // Verify the datatype for resolved and unresolved entity
    assert(entDefResolved.getAttributes.get(0).asInstanceOf[CdmTypeAttributeDefinition].getDataType == null)
    assert(entDef.getAttributes.get(0).asInstanceOf[CdmTypeAttributeDefinition].getDataType.getNamedReference == "string")
  }

  test("arrays") {
    val container = "/output1"
    val adlsAdapter = new AdlsAdapter(storage, container, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")
    cdmCorpus.getStorage.mount("cdm", new GithubAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")


    val entDefPre = cdmCorpus.fetchObjectAsync("adls:/logical-test.cdm.json/Business").get.asInstanceOf[CdmEntityDefinition]
    val entDefResolved = entDefPre.createResolvedEntityAsync("test1").get
    entDefResolved.getInDocument.saveAsAsync("test.cdm.json").get()
  }

  test("set secret after fetching config.json") {
    val container = "/output"
    val cdmCorpus = new CdmCorpusDefinition()
    val adlsAdapter = new AdlsAdapter(storage, container, tenantid, appid, appkey)
    cdmCorpus.getStorage.mount("adls", adlsAdapter)

    val config =cdmCorpus.getStorage.fetchAdapter("adls").readAsync("config/config.json").get()
    cdmCorpus.getStorage.mountFromConfig(config)

    val fooAdapter = cdmCorpus.getStorage().fetchAdapter("foo").asInstanceOf[AdlsAdapter];
    fooAdapter.setSecret(appkey)

    val entDefPre = cdmCorpus.fetchObjectAsync("adls:/BadNamespace.cdm.json/BadNamespace").get.asInstanceOf[CdmEntityDefinition]
    val entRes = entDefPre.createResolvedEntityAsync("temporary").get()
    entRes.getInDocument.saveAsAsync("new4.cdm.json").get()
    assert(entRes.getAttributes.get(0).asInstanceOf[CdmTypeAttributeDefinition].fetchDataFormat() == CdmDataFormat.String)
  }

  test("create resolved entities") {
    val container = "/output"
    val adlsAdapter = new AdlsAdapter(storage, container, tenantid, appid, appkey)
    val cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("adls1", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls1")
    cdmCorpus.getStorage.mount("cdm", new GithubAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls1")

    val docAbs = cdmCorpus.makeObject(CdmObjectType.DocumentDef,  "logical-test.cdm.json").asInstanceOf[CdmDocumentDefinition]
    docAbs.getImports.add("cdm:/foundations.cdm.json", null)
    val folder = cdmCorpus.getStorage.fetchRootFolder("adls1")
    folder.getDocuments.add(docAbs,docAbs.getName)
    val entDef = docAbs.getDefinitions.add(CdmObjectType.EntityDef, "logical-test").asInstanceOf[CdmEntityDefinition]

    val attNew = cdmCorpus.makeObject(CdmObjectType.TypeAttributeDef, "Name").asInstanceOf[CdmTypeAttributeDefinition]
    attNew.updateDataFormat(CdmDataFormat.valueOf("String"))
    entDef.getAttributes.add(attNew)

    val attrEnt1 = cdmCorpus.makeObject(CdmObjectType.EntityAttributeDef).asInstanceOf[CdmEntityAttributeDefinition]
    val entRef1 =  cdmCorpus.makeObject(CdmObjectType.EntityRef).asInstanceOf[CdmEntityReference]
    attrEnt1.asInstanceOf[CdmEntityAttributeDefinition].setName("level1")
    entRef1.setSimpleNamedReference(true)
    entRef1.setNamedReference("level1");
    attrEnt1.asInstanceOf[CdmEntityAttributeDefinition].setEntity(entRef1)
    val entity1  = docAbs.getDefinitions.add(CdmObjectType.EntityDef, "level1").asInstanceOf[CdmEntityDefinition]
    val attNew1 = cdmCorpus.makeObject(CdmObjectType.TypeAttributeDef, "Flag").asInstanceOf[CdmTypeAttributeDefinition]
    attNew1.updateDataFormat(CdmDataFormat.valueOf("Boolean"))
    entity1.getAttributes.add(attNew1)

    val attrEnt2 = cdmCorpus.makeObject(CdmObjectType.EntityAttributeDef).asInstanceOf[CdmEntityAttributeDefinition]
    val entRef2 =  cdmCorpus.makeObject(CdmObjectType.EntityRef).asInstanceOf[CdmEntityReference]
    attrEnt2.asInstanceOf[CdmEntityAttributeDefinition].setName("level2")
    entRef2.setSimpleNamedReference(true)
    entRef2.setNamedReference("level2");
    attrEnt2.asInstanceOf[CdmEntityAttributeDefinition].setEntity(entRef2)
    val entity2  = docAbs.getDefinitions.add(CdmObjectType.EntityDef, "level2").asInstanceOf[CdmEntityDefinition]
    val attNew2 = cdmCorpus.makeObject(CdmObjectType.TypeAttributeDef, "Salary").asInstanceOf[CdmTypeAttributeDefinition]
    attNew2.updateDataFormat(CdmDataFormat.valueOf("Double"))
    entity2.getAttributes.add(attNew2)


    val attrEnt3 = cdmCorpus.makeObject(CdmObjectType.EntityAttributeDef).asInstanceOf[CdmEntityAttributeDefinition]
    val entRef3 =  cdmCorpus.makeObject(CdmObjectType.EntityRef).asInstanceOf[CdmEntityReference]
    attrEnt3.asInstanceOf[CdmEntityAttributeDefinition].setName("level3")
    entRef3.setSimpleNamedReference(true)
    entRef3.setNamedReference("level3");
    attrEnt3.asInstanceOf[CdmEntityAttributeDefinition].setEntity(entRef3)
    val entity3  = docAbs.getDefinitions.add(CdmObjectType.EntityDef, "level3").asInstanceOf[CdmEntityDefinition]
    val attNew3 = cdmCorpus.makeObject(CdmObjectType.TypeAttributeDef, "ID").asInstanceOf[CdmTypeAttributeDefinition]
    attNew3.updateDataFormat(CdmDataFormat.valueOf("Int32"))
    entity3.getAttributes.add(attNew3)

    entDef.getAttributes.add(attrEnt1);
    entity1.getAttributes.add(attrEnt2);
    entity2.getAttributes.add(attrEnt3);


    val manifestFileName = "default.manifest.cdm.json"
    val manifest = cdmCorpus.makeObject(CdmObjectType.ManifestDef, manifestFileName).asInstanceOf[CdmManifestDefinition]
    manifest.getImports.add("cdm:/foundations.cdm.json", null)
    folder.getDocuments.add(manifest, manifestFileName)

    val r = new ResolveOptions(docAbs)
    r.setDirectives(new AttributeResolutionDirectiveSet(new util.HashSet[String](Arrays.asList("structured","noMaxDepth"))))
    val entDefResolved = entDef.createResolvedEntityAsync("test", r, manifest.getInDocument.getFolder).get

    manifest.getEntities.add(entDefResolved)
    manifest.setManifestName("default")
    if(manifest.saveAsAsync(manifestFileName, true).get()) {
      val doc = cdmCorpus.fetchObjectAsync("test.cdm.json").get().asInstanceOf[CdmDocumentDefinition];
      val attGrp = doc.getDefinitions.get(0).asInstanceOf[CdmEntityDefinition].getAttributes.get(1)
      val attGrpDef1 = attGrp.asInstanceOf[CdmAttributeGroupReference].getExplicitReference.asInstanceOf[CdmAttributeGroupDefinition];
      val attGrpDef2 = attGrpDef1.getMembers.get(1).asInstanceOf[CdmAttributeGroupReference].getExplicitReference.asInstanceOf[CdmAttributeGroupDefinition]
      assert(attGrpDef2.getMembers.size() == 2)

    }
  }

  //add the submanifest to the root manifest
  def addSubManifestToRootManifestAndSave(cdmCorpus: CdmCorpusDefinition, entityName:String, rootManifest:CdmManifestDefinition, subManifest:CdmManifestDefinition): Unit = {
    val subManifestDecl2 = cdmCorpus.makeObject(CdmObjectType.ManifestDeclarationDef, entityName, false).asInstanceOf[CdmManifestDeclarationDefinition]
    val subManifestPath2 = cdmCorpus.getStorage.createRelativeCorpusPath(subManifest.getAtCorpusPath, rootManifest)
    subManifestDecl2.setDefinition(subManifestPath2)
    rootManifest.getSubManifests.add(subManifestDecl2)
    rootManifest.saveAsAsync("root.manifest.cdm.json", true).get
  }

  // create an entity and put it in a submanifest
  def createSubmanifest(cdmCorpus: CdmCorpusDefinition, entityName:String): CdmManifestDefinition = {
    // Create the entity
    val docAbs = cdmCorpus.makeObject(CdmObjectType.DocumentDef,  entityName +".cdm.json").asInstanceOf[CdmDocumentDefinition]
    docAbs.getImports.add("cdm:/foundations.cdm.json", null)
    val folder = cdmCorpus.getStorage.fetchRootFolder("adls")
    // Place the "$entityName".cdm.json file in a folder with the name "$entityName"
    val subFolder = folder.getChildFolders.add(entityName)
    subFolder.getDocuments.add(docAbs, docAbs.getName)

    // Create an attribute for this entity
    val entDef = docAbs.getDefinitions.add(CdmObjectType.EntityDef, entityName).asInstanceOf[CdmEntityDefinition]
    val attNew = cdmCorpus.makeObject(CdmObjectType.TypeAttributeDef, "Name").asInstanceOf[CdmTypeAttributeDefinition]
    attNew.updateDataFormat(CdmDataFormat.valueOf("String"))
    entDef.getAttributes.add(attNew)

    // Create a partition for this entity with some traits
    val partition = cdmCorpus.makeObject(CdmObjectType.DataPartitionDef, "New Partition", false).asInstanceOf[CdmDataPartitionDefinition]
    partition.setLocation("data" + "/foo.csv")
    partition.getArguments.clear()
    val csvTrait = partition.getExhibitsTraits.add("is.partition.format.CSV")
    csvTrait.asInstanceOf[CdmTraitReference].getArguments.add("delimiter", ",")
    csvTrait.asInstanceOf[CdmTraitReference].getArguments.add("columnHeaders", "true")

    // Create the submanifest for this entity
    val subManifest = cdmCorpus.makeObject(CdmObjectType.ManifestDef, "default").asInstanceOf[CdmManifestDefinition]
    subManifest.getImports.add("cdm:/foundations.cdm.json", null)

    // Also place Place the "$entityName".manifest.cdm.json file in a folder with the name "$entityName"
    subFolder.getDocuments.add(subManifest, entityName+".manifest.cdm.json")

    // Add the Entity definition to the submanifest and the partition to the entity declaration
    subManifest.getEntities.add(entDef)
    val entDecl = subManifest.getEntities.asScala.find(_.getEntityName == entDef.getEntityName).get
    entDecl.getDataPartitions.add(partition)

    subManifest
  }

  test ("manifestPath split") {
    val p1 = new DefaultSource()
    val getRel = PrivateMethod[String]('getManifestPath)
    val getFileName = PrivateMethod[String]('getManifestFilename)
    var validPath = "/root/default.manifest.cdm.json"
    var expectFileName = "default.manifest.cdm.json"
    var expectDir = "/root/"
    assert(expectDir == (p1 invokePrivate getRel(validPath)))
    assert(expectFileName == (p1 invokePrivate getFileName(validPath)))

    validPath = "root/default.manifest.cdm.json"
    expectFileName = "default.manifest.cdm.json"
    expectDir = "/root/"
    assert(expectDir == (p1 invokePrivate getRel(validPath)))
    assert(expectFileName == (p1 invokePrivate getFileName(validPath)))

    validPath = "/default.manifest.cdm.json"
    expectFileName = "default.manifest.cdm.json"
    expectDir = "/"
    assert(expectDir == (p1 invokePrivate getRel(validPath)))
    assert(expectFileName == (p1 invokePrivate getFileName(validPath)))

    validPath = "default.manifest.cdm.json"
    expectFileName = "default.manifest.cdm.json"
    expectDir = "/"
    assert(expectDir == (p1 invokePrivate getRel(validPath)))
    assert(expectFileName == (p1 invokePrivate getFileName(validPath)))
  }

  test ("read model.json non root path") {

    var adlsAdapter = new AdlsAdapter(storage, "/antaresds/nfpdataflow/pbiflows", tenantid, appid, appkey)
    var cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")
    cdmCorpus.getStorage.mount("cdm", new GithubAdapter)

    /* Read cX data */
    var manifest = cdmCorpus.fetchObjectAsync("model.json").get().asInstanceOf[CdmManifestDefinition]
    var entity = manifest.getEntities().asScala.find(_.getEntityName == "cdstest")
    var eDec = entity.get

    for(partition <- eDec.getDataPartitions.asScala) {
      println("Partition location in model.json from cds dataset: " + partition.getLocation)
      val absPath = cdmCorpus.getStorage.createAbsoluteCorpusPath(partition.getLocation,eDec)
      println("absolute path is " + absPath + "\n")
    }

  }

  test ("find files doesn't work with partitions patterns and spaces in path") {

    /*
     * Entity with two partitions using partition pattern. Copied manifest/entity/partitions to directory
     * "rootnospaces" to demonstrate the issue is that when root has spaces, CDM is not finding the partitions
     */
    var adlsAdapter = new AdlsAdapter(storage, "/patternspacebug/root%20with%20spaces", tenantid, appid, appkey)
    var cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")
    cdmCorpus.getStorage.mount("cdm", new GithubAdapter)

    var manifest = cdmCorpus.fetchObjectAsync("default.manifest.cdm.json").get().asInstanceOf[CdmManifestDefinition]
    var entity = manifest.getEntities().asScala.find(_.getEntityName == "TestEntity")
    var eDec = entity.get

    eDec.fileStatusCheckAsync().get()

    for(partition <- eDec.getDataPartitions.asScala) {
      val absPath = cdmCorpus.getStorage.createAbsoluteCorpusPath(partition.getLocation,eDec)
      println("partition found with partition in root path" + absPath)
    }

    adlsAdapter = new AdlsAdapter(storage, "/patternspacebug/rootnospaces", tenantid, appid, appkey)
    cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount("adls", adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace("adls")
    cdmCorpus.getStorage.mount("cdm", new GithubAdapter)

    manifest = cdmCorpus.fetchObjectAsync("default.manifest.cdm.json").get().asInstanceOf[CdmManifestDefinition]
    entity = manifest.getEntities().asScala.find(_.getEntityName == "TestEntity")
    eDec = entity.get

    eDec.fileStatusCheckAsync().get()

    for(partition <- eDec.getDataPartitions.asScala) {
      val absPath = cdmCorpus.getStorage.createAbsoluteCorpusPath(partition.getLocation,eDec)
      println("partition found without spaces in root path" + absPath)
    }
    println("done")
  }

  test("predefined") {
    var adlsAdapter = new AdlsAdapter(storage, "/outputsubmanifest/example-public-standards", tenantid, appid, appkey)
    var cdmCorpus = new CdmCorpusDefinition()
    cdmCorpus.getStorage.mount(Constants.SPARK_MODELROOT_NAMESPACE, adlsAdapter)
    cdmCorpus.getStorage.setDefaultNamespace(Constants.SPARK_MODELROOT_NAMESPACE)

    val eDef = cdmCorpus.fetchObjectAsync(Constants.SPARK_MODELROOT_NAMESPACE + ":" + "/core/applicationCommon/KnowledgeArticleCategory.cdm.json/KnowledgeArticleCategory").get.asInstanceOf[CdmEntityDefinition]
    val resolvedDef = eDef.createResolvedEntityAsync("temporary").get
    println(resolvedDef.getAttributes.size())
  }
}
