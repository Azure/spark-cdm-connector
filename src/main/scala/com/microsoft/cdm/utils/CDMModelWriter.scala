package com.microsoft.cdm.utils

import java.util.Arrays

import com.microsoft.cdm.log.SparkCDMLogger
import com.microsoft.cdm.write.FileCommitMessage
import com.microsoft.commondatamodel.objectmodel.cdm.{CdmCollection, CdmDataPartitionDefinition, _}
import com.microsoft.commondatamodel.objectmodel.enums.{CdmObjectType, CdmPropertyName}
import com.microsoft.commondatamodel.objectmodel.persistence.cdmfolder.TypeAttributePersistence
import com.microsoft.commondatamodel.objectmodel.resolvedmodel.ResolveContext
import com.microsoft.commondatamodel.objectmodel.utilities.{JMapper, ResolveOptions}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.spark.sql.types.{ArrayType, DecimalType, Metadata, StructType}
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


class CDMModelWriter(val storage: String,
                     val container: String,
                     val manifestPath: String,
                     val manifestFileName: String,
                     val manifestName: String,
                     val entityName: String,
                     val useSubManifest: Boolean,
                     val entDefinition: String,
                     val entDefContAndPath: String,
                     val jobId: String,
                     val fileFormatSettings: FileFormatSettings,
                     val authCredential: AuthCredential,
                     val tokenProvider: Option[CDMTokenProvider],
                     val overrideConfigPath : String,
                     cdmSource: CDMSource.Value,
                     val entityDefinitionStorage: String,
                     val maxCDMThreads: Int) extends CDMModelCommon (storage, container, manifestPath, manifestFileName,
  entityName, entDefinition, entDefContAndPath, authCredential,
  tokenProvider, overrideConfigPath, cdmSource, entityDefinitionStorage,
  maxCDMThreads) {


  logger  = LoggerFactory.getLogger(classOf[CDMModelWriter])
  def insertPropertiesFromMetadata(attrDef: CdmTypeAttributeDefinition, md: Metadata) = {

    // There Metadata iteration, so use the fields we know about
    // TODO: We can return json as a string, then parse that, allowing for arbitrary properties
    for (property <- CdmPropertyName.values) {
      property match {
        case CdmPropertyName.CDM_SCHEMAS => null
        case CdmPropertyName.DATA_FORMAT => null // already set format from field type
        case CdmPropertyName.DEFAULT => {
          if (md.contains(CdmPropertyName.DEFAULT.toString)) {
            val value = md.getString(CdmPropertyName.DEFAULT.toString)
            attrDef.updateDefaultValue(value)
          }
        }
        case CdmPropertyName.DESCRIPTION => {
          if (md.contains(CdmPropertyName.DESCRIPTION.toString)) {
            val value = md.getString(CdmPropertyName.DESCRIPTION.toString)
            attrDef.updateDescription(value)
          }
        }
        case CdmPropertyName.DISPLAY_NAME=> {
          if (md.contains(CdmPropertyName.DISPLAY_NAME.toString)) {
            val value = md.getString(CdmPropertyName.DISPLAY_NAME.toString)
            attrDef.setDisplayName(value)
          }
        }
        case CdmPropertyName.IS_NULLABLE=>  {
          if (md.contains(CdmPropertyName.IS_NULLABLE.toString)) {
            val value = md.getBoolean(CdmPropertyName.IS_NULLABLE.toString)
            attrDef.updateIsNullable(value)
          }
        }
        case CdmPropertyName.IS_PRIMARY_KEY => {
          //TODO: There is no updateIsPrimaryKey
          if (md.contains(CdmPropertyName.IS_PRIMARY_KEY.toString)) {
            val value = md.getBoolean(CdmPropertyName.IS_PRIMARY_KEY.toString)
            //attrDef.updateIsPrimaryKey(value)
          }
        }
        case CdmPropertyName.IS_READ_ONLY=> {
          if (md.contains(CdmPropertyName.IS_READ_ONLY.toString)) {
            val value = md.getBoolean(CdmPropertyName.IS_READ_ONLY.toString)
            attrDef.updateIsReadOnly(value)
          }
        }
        case CdmPropertyName.MAXIMUM_LENGTH=> {
          if (md.contains(CdmPropertyName.MAXIMUM_LENGTH.toString)) {
            val value = md.getLong(CdmPropertyName.MAXIMUM_LENGTH.toString).toInt
            attrDef.updateMaximumLength(value)
          }
        }
        case CdmPropertyName.MAXIMUM_VALUE=> {
          if (md.contains(CdmPropertyName.MAXIMUM_VALUE.toString)) {
            val value = md.getString(CdmPropertyName.MAXIMUM_VALUE.toString)
            attrDef.updateMaximumValue(value)
          }
        }
        case CdmPropertyName.MINIMUM_VALUE=> {
          if (md.contains(CdmPropertyName.MINIMUM_VALUE.toString)) {
            val value = md.getString(CdmPropertyName.MINIMUM_VALUE.toString)
            attrDef.updateMinimumValue(value)
          }
        }
        case CdmPropertyName.PRIMARY_KEY=>  null
        case CdmPropertyName.SOURCE_NAME=> {
          if (md.contains(CdmPropertyName.SOURCE_NAME.toString)) {
            val value = md.getString(CdmPropertyName.SOURCE_NAME.toString)
            attrDef.updateSourceName(value)
          }
        }
        case CdmPropertyName.SOURCE_ORDERING=> {
          if (md.contains(CdmPropertyName.SOURCE_ORDERING.toString)) {
            val value = md.getLong(CdmPropertyName.SOURCE_ORDERING.toString).toInt
            attrDef.updateSourceOrderingToTrait(value)
          }
        }
        case CdmPropertyName.VALUE_CONSTRAINED_TO_LIST=> {
          if (md.contains(CdmPropertyName.VALUE_CONSTRAINED_TO_LIST.toString)) {
            val value = md.getBoolean(CdmPropertyName.VALUE_CONSTRAINED_TO_LIST.toString)
            attrDef.updateValueConstrainedToList(value)
          }
        }
        case CdmPropertyName.VERSION=>  null
      }
    }
  }

  def deleteSubManifestEntity(fetchManifest: CdmManifestDefinition): Unit = {
    val subDec = fetchManifest.getSubManifests.asScala.find(_.getManifestName == entityName).get
    /* TODO: add back when overwriting an entity declaration in a submanifest works with just root manifest save */
    /*
    val subManDef = cdmCorpus.fetchObjectAsync(subDec.getDefinition, fetchManifest).join.asInstanceOf[CdmManifestDefinition]
    val folder= cdmCorpus.getStorage.fetchRootFolder("adls")
    val subFolder = folder.getChildFolders.asScala.find(_.getName == entityName).get
    subFolder.getDocuments.remove(subManDef)
    folder.getChildFolders.remove(entityName)
    val entity = subManDef.getEntities().asScala.find(_.getEntityName == entityName).get
    subManDef.getEntities.remove(entity)
     */
    fetchManifest.getSubManifests.remove(subDec)
  }

  // Delete an  entity from the the root manifest
  def deleteEntity(fetchManifest: CdmManifestDefinition): Unit = {
    val entity = fetchManifest.getEntities().asScala.find(_.getEntityName == entityName).get
    fetchManifest.getEntities.remove(entity)
    SparkCDMLogger.log(Level.INFO, "Deleted entity from manifest- "+ entity, logger)
  }


  // Create a Predefined entity (from an entitiy definition)
  def createEntityPredefined(partitionList: ListBuffer[FileCommitMessage],
                             fetchManifest: CdmManifestDefinition,
                             dataFolder:String): Unit = {
    val folder = cdmCorpus.getStorage.fetchRootFolder(Constants.SPARK_NAMESPACE)
    val entitySubFolder = folder.getChildFolders.add(entityName)

    val eDef = cdmCorpus.fetchObjectAsync(Constants.SPARK_MODELROOT_NAMESPACE + ":"+ entDefinition).get.asInstanceOf[CdmEntityDefinition]
    val entDefResolved = eDef.createResolvedEntityAsync(entityName, new ResolveOptions(eDef), entitySubFolder).get

    val doc = entDefResolved.getInDocument()
    doc.getImports.add("cdm:/foundations.cdm.json")

    fetchManifest.getEntities.add(entDefResolved)
    val entDecl = fetchManifest.getEntities.asScala.find(_.getEntityName == entDefResolved.getEntityName).get
    addPartitions(entDecl, partitionList, dataFolder)
  }

  // Create an entity from scratch (schema from dataframe)
  def createEntityScratch(schema:StructType,partitionList: ListBuffer[FileCommitMessage],
                          fetchManifest: CdmManifestDefinition,
                          dataFolder:String): Unit = {

    val docAbs = cdmCorpus.makeObject(CdmObjectType.DocumentDef, entityName + ".cdm.json").asInstanceOf[CdmDocumentDefinition]
    docAbs.getImports.add("cdm:/foundations.cdm.json", null)

    val folder = cdmCorpus.getStorage.fetchRootFolder(Constants.SPARK_NAMESPACE)

    val entitySubFolder = folder.getChildFolders.add(entityName)

    val logicalEntityFolder = entitySubFolder.getChildFolders.add(Constants.LOGICAL_ENTITY_DIR)
    logicalEntityFolder.getDocuments.add(docAbs, docAbs.getName)

    val entDef= docAbs.getDefinitions.add(CdmObjectType.EntityDef, entityName).asInstanceOf[CdmEntityDefinition]
    addAttributes(docAbs, entDef, schema)

    val resolveOpts = new ResolveOptions(docAbs)
    val entDefResolved = entDef.createResolvedEntityAsync(entityName, resolveOpts, entitySubFolder).get

    fetchManifest.getEntities.add(entDefResolved)
    val newEntityDecl = fetchManifest.getEntities.asScala.find(_.getEntityName == entDefResolved.getEntityName).get
    addPartitions(newEntityDecl, partitionList, dataFolder)
  }

  def addAttributes(doc: CdmDocumentDefinition, entDef: CdmObjectDefinitionBase, schema:StructType): Unit = {
    schema.foreach {field =>
      var attrDef : CdmAttributeItem = null;
      // when we encounter a structType in the iteration, it means it is a nested entity..
      if (field.dataType.isInstanceOf[StructType] || field.dataType.isInstanceOf[ArrayType]) {
        // create entity reference
        attrDef = cdmCorpus.makeObject(CdmObjectType.EntityAttributeDef).asInstanceOf[CdmEntityAttributeDefinition]
        val entRef = cdmCorpus.makeObject(CdmObjectType.EntityRef).asInstanceOf[CdmEntityReference]
        attrDef.asInstanceOf[CdmEntityAttributeDefinition].setName(field.name)
        entRef.setNamedReference(field.name)
        entRef.setSimpleNamedReference(true)
        attrDef.asInstanceOf[CdmEntityAttributeDefinition].setEntity(entRef)
        // set attribute name
        attrDef.asInstanceOf[CdmEntityAttributeDefinition].setName(field.name.toLowerCase())
        // set resolution guidance
        val resolutionGuidance = cdmCorpus.makeObject(CdmObjectType.AttributeResolutionGuidanceDef).asInstanceOf[CdmAttributeResolutionGuidance]
        resolutionGuidance.setImposedDirectives(Arrays.asList("structured", "noMaxDepth"))
        /* Renames the attribute name after resolving it. The default is {a}{M}.
        Ex: For Entity `Address` and its attribute `Street`. Without the renameFormat, the resolved entity will generate the attribute name - "AddressStreet".
        Since, we just want the original member name, with renameFormat = {m} will keep it as "Street"
       */
        resolutionGuidance.setRenameFormat("{m}")
        val entity = doc.getDefinitions.add(CdmObjectType.EntityDef, field.name).asInstanceOf[CdmEntityDefinition]
        if (field.dataType.isInstanceOf[StructType]) {
          attrDef.asInstanceOf[CdmEntityAttributeDefinition].setResolutionGuidance(resolutionGuidance)
          addAttributes(doc, entity, field.dataType.asInstanceOf[StructType])
        } else  {
          /* In CDM the default directives are: byReference and Normalized. In order to get the arrays working in CDM, we need to remove the normalized directive explicitly here.
             Normalized directive basically ignores the arrays of entities. */
          resolutionGuidance.setRemovedDirectives(Arrays.asList("normalized"))
          /* set cardinality to let OM know there can be many items in the array */
          resolutionGuidance.setCardinality("many")
          attrDef.asInstanceOf[CdmEntityAttributeDefinition].setResolutionGuidance(resolutionGuidance)
          val arrayElementType = field.dataType.asInstanceOf[ArrayType].elementType
          addAttributes(doc, entity, arrayElementType.asInstanceOf[StructType])
        }
      } else {
        // If the existing metadata field is empty, just build a entity from scratch.
        // One way for this to occur is the original source of the dataframe is not CDM, meaning we won't have
        // metadata
        if (field.metadata != null && field.metadata.contains(Constants.MD_DATATYPE_OVERRIDE)) {
          // If the user sets the datatype metadata key, set the CDM schema to that type. This is currently only useful
          // for type Time as Spark doesn't have that type.
          val dataType = field.metadata.getString(Constants.MD_DATATYPE_OVERRIDE)
          attrDef = cdmCorpus.makeObject(CdmObjectType.TypeAttributeDef, field.name).asInstanceOf[CdmTypeAttributeDefinition]
          attrDef.asInstanceOf[CdmTypeAttributeDefinition].setDataType(cdmCorpus.makeRef(CdmObjectType.DataTypeRef, dataConverter.toCdmDataTypeOverride(dataType), true))
          attrDef.asInstanceOf[CdmTypeAttributeDefinition].setName(field.name)
        } else {
          attrDef = cdmCorpus.makeObject(CdmObjectType.TypeAttributeDef, field.name).asInstanceOf[CdmTypeAttributeDefinition]
          attrDef.asInstanceOf[CdmTypeAttributeDefinition].setDataType(cdmCorpus.makeRef(CdmObjectType.DataTypeRef, dataConverter.toCdmDataType(field.dataType), true))
          if(field.dataType.isInstanceOf[DecimalType]) {
            val decimalType =  field.dataType.asInstanceOf[DecimalType]
            val traitReference = cdmCorpus.makeObject(CdmObjectType.TraitRef, Constants.CDM_DECIMAL_TRAIT).asInstanceOf[CdmTraitReference]
            traitReference.getArguments.add("precision", decimalType.precision)
            traitReference.getArguments.add("scale",  decimalType.scale)
            attrDef.getAppliedTraits.add(traitReference)
          }
        }
      }
      entDef.asInstanceOf[CdmEntityDefinition].getAttributes().add(attrDef)
    }
  }

  def setPartitionExhibitTraits(fileFormatCDM: String,
                                partition: CdmObjectDefinitionBase,
                                fileDirName: Option[String] = None) = {

    if (manifestFileName == Constants.MODEL_JSON) partition.asInstanceOf[CdmDataPartitionDefinition].setLocation(fileDirName.get + "." + fileFormatCDM)
    fileFormatCDM match {
      case "csv" => {
        val csvTrait = partition.getExhibitsTraits.add("is.partition.format.CSV")
        csvTrait.asInstanceOf[CdmTraitReference].getArguments.add("delimiter", fileFormatSettings.delimiter.toString)
        csvTrait.asInstanceOf[CdmTraitReference].getArguments.add("columnHeaders", fileFormatSettings.showHeader.toString)
      }
      case "parquet" => {
        partition.getExhibitsTraits.add("is.partition.format.parquet")
      }
    }
  }

  def addPartitionPattern(entDecl: CdmEntityDeclarationDefinition, dataFolder:String): Unit = {
    val partitionPattern = cdmCorpus.makeObject(CdmObjectType.DataPartitionPatternDef, "Partition-" + jobId, false).asInstanceOf[CdmDataPartitionPatternDefinition]

    val fileRootLocation = if (useSubManifest) {
      dataFolder +  "/"
    } else {
      entDecl.getEntityName + "/" + dataFolder +  "/"
    }
    val globPattern=Constants.GLOB_PATTERN.format(entityName, jobId, fileFormatSettings.fileFormat)
    partitionPattern.setRootLocation(fileRootLocation)
    partitionPattern.setGlobPattern(globPattern)
    setPartitionExhibitTraits(fileFormatSettings.fileFormat, partitionPattern)
    entDecl.getDataPartitionPatterns.add(partitionPattern)
  }

  def addExplicitPartitions(entDecl: CdmEntityDeclarationDefinition,
                            partitionList: ListBuffer[FileCommitMessage],
                            dataFolder:String): Unit = {

    partitionList.foreach { part =>
      val partition = cdmCorpus.makeObject(CdmObjectType.DataPartitionDef, part.name, false).asInstanceOf[CdmDataPartitionDefinition]
      assert(!useSubManifest)
      val fileDirName = entDecl.getEntityName + "/" + dataFolder +  "/" + part.name
      setPartitionExhibitTraits(fileFormatSettings.fileFormat, partition, Some(fileDirName))
      entDecl.getDataPartitions.add(partition)
    }
  }

  def addPartitions(entDecl: CdmEntityDeclarationDefinition,
                    partitionList: ListBuffer[FileCommitMessage],
                    dataFolder:String): Unit = {
    if (manifestFileName == Constants.MODEL_JSON) {
      addExplicitPartitions(entDecl, partitionList, dataFolder)
    } else {
      addPartitionPattern(entDecl, dataFolder)
    }
  }

  // create an entity and put it in a submanifest
  def createRootManifestWithEntityScratch(schema:StructType,
                                          partitionList: ListBuffer[FileCommitMessage],
                                          dataFolder:String): CdmManifestDefinition = {

    // Create the entity
    val docAbs = cdmCorpus.makeObject(CdmObjectType.DocumentDef, entityName +".cdm.json").asInstanceOf[CdmDocumentDefinition]
    docAbs.getImports.add("cdm:/foundations.cdm.json", null)

    val folder = cdmCorpus.getStorage.fetchRootFolder(Constants.SPARK_NAMESPACE)

    val entitySubFolder = folder.getChildFolders.add(entityName)

    val logicalEntityFolder = entitySubFolder.getChildFolders.add(Constants.LOGICAL_ENTITY_DIR)
    // Place the logical "$entityName".cdm.json file in a folder with the name "logicalDefinition"
    logicalEntityFolder.getDocuments.add(docAbs, docAbs.getName)

    // Create attributes for this entity
    val entDef = docAbs.getDefinitions.add(CdmObjectType.EntityDef, entityName).asInstanceOf[CdmEntityDefinition]
    addAttributes(docAbs, entDef, schema)

    // Create the for this entity
    val manifest = cdmCorpus.makeObject(CdmObjectType.ManifestDef, manifestName).asInstanceOf[CdmManifestDefinition]
    manifest.getImports.add("cdm:/foundations.cdm.json", null)
    folder.getDocuments.add(manifest, manifestFileName)

    // Create a resolved entity
    val resolveOpts = new ResolveOptions(entDef.getInDocument)
    val entDefResolved = entDef.createResolvedEntityAsync(entityName, resolveOpts, entitySubFolder).get

    manifest.getEntities.add(entDefResolved)
    val entDecl = manifest.getEntities.asScala.find(_.getEntityName == entDefResolved.getEntityName).get
    addPartitions(entDecl, partitionList, dataFolder)
    manifest
  }

  def createRootManifestWithEntityPredefined(partitionList: ListBuffer[FileCommitMessage],
                                             dataFolder:String): CdmManifestDefinition = {

    val folder = cdmCorpus.getStorage.fetchRootFolder(Constants.SPARK_NAMESPACE)
    val entitySubFolder = folder.getChildFolders.add(entityName)

    val eDef = cdmCorpus.fetchObjectAsync(Constants.SPARK_MODELROOT_NAMESPACE + ":" + entDefinition).get.asInstanceOf[CdmEntityDefinition]
    val entDefResolved = eDef.createResolvedEntityAsync(entityName, new ResolveOptions(eDef), entitySubFolder).get

    val manifest = cdmCorpus.makeObject(CdmObjectType.ManifestDef, manifestName).asInstanceOf[CdmManifestDefinition]
    manifest.getImports.add("cdm:/foundations.cdm.json", null)
    folder.getDocuments.add(manifest, manifestFileName)

    val doc = entDefResolved.getInDocument()
    doc.getImports.add("cdm:/foundations.cdm.json")

    manifest.getEntities.add(entDefResolved)
    val entDecl = manifest.getEntities.asScala.find(_.getEntityName == entDefResolved.getEntityName).get
    addPartitions(entDecl, partitionList, dataFolder)
    manifest
  }

  // create an entity and put it in a submanifest
  def createSubmanifestWithEntityScratch(schema:StructType,
                                         partitionList: ListBuffer[FileCommitMessage],
                                         isOverwrite: Boolean,
                                         dataFolder:String): CdmManifestDefinition = {
    // Create the entity
    val docAbs = cdmCorpus.makeObject(CdmObjectType.DocumentDef, entityName +".cdm.json").asInstanceOf[CdmDocumentDefinition]

    docAbs.getImports.add("cdm:/foundations.cdm.json", null)

    val folder = cdmCorpus.getStorage.fetchRootFolder(Constants.SPARK_NAMESPACE)
    // Place the logical "$entityName".cdm.json file in its own folder
    val subFolder = folder.getChildFolders.add(entityName)
    val defSubFolder = subFolder.getChildFolders.add(Constants.LOGICAL_ENTITY_DIR)
    defSubFolder.getDocuments.add(docAbs, docAbs.getName)

    // Create an attribute for this entity
    val entDef = docAbs.getDefinitions.add(CdmObjectType.EntityDef, entityName).asInstanceOf[CdmEntityDefinition]
    addAttributes(docAbs, entDef, schema)

    // Create the submanifest for this entity
    val subManifest = cdmCorpus.makeObject(CdmObjectType.ManifestDef, entityName).asInstanceOf[CdmManifestDefinition]
    subManifest.getImports.add("cdm:/foundations.cdm.json", null)
    subFolder.getDocuments.add(subManifest, entityName+".manifest.cdm.json")

    // Create a resolved entity
    val resolveOpts = new ResolveOptions(docAbs)
    val entDefResolved = entDef.createResolvedEntityAsync(entityName, resolveOpts, subFolder).get

    subManifest.getEntities.add(entDefResolved)
    val entDecl = subManifest.getEntities.asScala.find(_.getEntityName == entDefResolved.getEntityName).get
    addPartitions(entDecl, partitionList, dataFolder)
    if (isOverwrite) {
      // TODO: Remove this logic on CDM library update. This is here because on overwrite of an entity declaration
      // in a submanifest is not preserved if we only save the root manifest
      // In case of success, we will rename the file back to <EntityName>.manifest.cdm.json. Save it by a different name - <Entity>.rename.manifest.cdm.json to handle abort
      subManifest.saveAsAsync(String.format(Constants.SUBMANIFEST_WITH_OVERWRITTEN_PARTITIONS, subManifest.getManifestName), true).get
    }
    subManifest
  }

  // create an entity and put it in a submanifest
  def createSubmanifestWithEntityPredefined(schema:StructType,partitionList: ListBuffer[FileCommitMessage], isOverwrite: Boolean,
                                            dataFolder:String): CdmManifestDefinition = {

    val subManifest= cdmCorpus.makeObject(CdmObjectType.ManifestDef, entityName).asInstanceOf[CdmManifestDefinition]
    subManifest.getEntities.add(entityName, Constants.SPARK_MODELROOT_NAMESPACE + ":" + entDefinition)
    val folder= cdmCorpus.getStorage.fetchRootFolder(Constants.SPARK_NAMESPACE)
    val subFolder = folder.getChildFolders.add(entityName)
    subFolder.getDocuments.add(subManifest)
    val manifestResolved = subManifest.createResolvedManifestAsync(entityName, "").get

    val entity = manifestResolved.getEntities().asScala.find(_.getEntityName == entityName).get
    val eDef= cdmCorpus.fetchObjectAsync(entity.getEntityPath,manifestResolved, true).get().asInstanceOf[CdmEntityDefinition]
    val doc = eDef.getInDocument()
    doc.getImports.add("cdm:/foundations.cdm.json")

    manifestResolved.getImports.add("cdm:/foundations.cdm.json", "")
    val entityDecl = manifestResolved.getEntities.asScala.find(_.getEntityName == entityName).get
    addPartitions(entityDecl, partitionList, dataFolder)
    if (isOverwrite) {
      // TODO: Remove this logic on CDM library update. This is here because on overwrite of an entity declaration
      // in a submanifest is not preserved if we only save the root manifest
      // In case of success, we will rename the file back to <EntityName>.manifest.cdm.json. Save it by a different name - <Entity>.rename.manifest.cdm.json to handle abort
      manifestResolved.saveAsAsync(String.format(Constants.SUBMANIFEST_WITH_OVERWRITTEN_PARTITIONS, subManifest.getManifestName), true).get
    }
    manifestResolved
  }

  //add the submanifest to the root manifest
  def addSubManifestToRootManifestAndSave(rootManifest:CdmManifestDefinition, subManifest:CdmManifestDefinition): Boolean = {
    val subManifestDecl2 = cdmCorpus.makeObject(CdmObjectType.ManifestDeclarationDef, entityName, false).asInstanceOf[CdmManifestDeclarationDefinition]
    val subManifestPath2 = cdmCorpus.getStorage.createRelativeCorpusPath(subManifest.getAtCorpusPath, rootManifest)
    subManifestDecl2.setDefinition(subManifestPath2)
    rootManifest.getSubManifests.add(subManifestDecl2)
    rootManifest.saveAsAsync(manifestFileName, true).get.booleanValue()

  }

  def createRootManifest(): CdmManifestDefinition={
    val manifest= cdmCorpus.makeObject(CdmObjectType.ManifestDef, manifestName).asInstanceOf[CdmManifestDefinition]
    manifest.getImports.add("cdm:/foundations.cdm.json", null)
    val rootFolder = cdmCorpus.getStorage.fetchRootFolder(Constants.SPARK_NAMESPACE)
    rootFolder.getDocuments.add(manifest, manifestFileName)
    manifest
  }

  def createEntity(schema:StructType, partitionList: ListBuffer[FileCommitMessage], allowOverwrite: Boolean,
                   dataFolder: String, conf: SparkSerializableConfiguration, cdmEntity: CDMEntity): Boolean = {
    //if the manifest doesn't exist create it
    if(cdmEntity.rootManifest == null) {

      if (useSubManifest) {
        // create a submanifest with an entity inside it
        val subManifest = if (entDefinition.isEmpty) {
          createSubmanifestWithEntityScratch(schema, partitionList, false, dataFolder)
        } else {
          createSubmanifestWithEntityPredefined(schema, partitionList, false, dataFolder)
        }

        // create a root manifest
        val manifest = createRootManifest()

        if(Constants.EXCEPTION_TEST) {
          throw new Exception("Mock Exception on create entity")
        }

        SparkCDMLogger.log(Level.INFO,"New Manifest. Adding entity " + entityName + " to a submanifest under manifest" + manifestFileName, logger)
        addSubManifestToRootManifestAndSave(manifest, subManifest)

      } else { /*Adding entity to root*/

        val manifest = if (entDefinition.isEmpty) {
          createRootManifestWithEntityScratch(schema, partitionList, dataFolder)
        } else  {
          createRootManifestWithEntityPredefined( partitionList, dataFolder)
        }

        SparkCDMLogger.log(Level.INFO, "New Manifest. Adding entity " + entityName + " to " + manifestFileName, logger)
        manifest.setManifestName(manifestName)
        if(Constants.EXCEPTION_TEST) {
          throw new Exception("Mock Exception on create entity")
        }
        manifest.saveAsAsync(manifestFileName, true).get.booleanValue()
      }
    } else {
      //TODO if the entity already exists in the manifest, we need to make sure the schema is not changing.
      // For now, just assuming that you will not try to overwrite the same entity
      val fetchManifest = cdmEntity.rootManifest
      val entityDoesExist = cdmEntity.entityDec
      val isOverwrite = (entityDoesExist !=null) && allowOverwrite

      if (entityDoesExist != null) {
        if (allowOverwrite) {
          SparkCDMLogger.log(Level.INFO, "Overwriting existing entity " + entityName, logger)
        } else {
          throw new RuntimeException("Found entity that already exists" + entityName)
        }
      }

      if (useSubManifest) {

        if (isOverwrite) {
          deleteSubManifestEntity(fetchManifest)
        }

        val subManifest = if (entDefinition.isEmpty) {
          createSubmanifestWithEntityScratch(schema, partitionList, isOverwrite, dataFolder)
        } else  {
          createSubmanifestWithEntityPredefined(schema, partitionList, isOverwrite, dataFolder)
        }

        if(Constants.EXCEPTION_TEST && isOverwrite) {
          throw new Exception("Mock Exception on overwrite")
        }

        SparkCDMLogger.log(Level.INFO, "Appending entity " + entityName + " to a new submanifest under manifest" + manifestFileName, logger)
        addSubManifestToRootManifestAndSave(fetchManifest, subManifest)

      } else {

        if (isOverwrite) {
          deleteEntity(fetchManifest)
        }

        if (entDefinition.isEmpty) {
          createEntityScratch(schema, partitionList, fetchManifest, dataFolder)
        } else {
          createEntityPredefined(partitionList, fetchManifest, dataFolder)
        }

        if(Constants.EXCEPTION_TEST && isOverwrite) {
          throw new Exception("Mock Exception on overwrite")
        }

        fetchManifest.setManifestName(manifestName)
        SparkCDMLogger.log(Level.INFO, "Appending entity " + entityName + " to " + manifestFileName, logger)
        fetchManifest.saveAsAsync(manifestFileName, true).get.booleanValue()
      }
    }
  }

  def updateEntity(entity: CDMEntity, partitionList: ListBuffer[FileCommitMessage], dataFolder:String): Unit = {
    val fetchManifest= entity.rootManifest
    var entDecl: CdmEntityDeclarationDefinition = null
    if (useSubManifest) {
      val subManifest = cdmCorpus.fetchObjectAsync(entityName+"/"+entityName+".manifest.cdm.json").get().asInstanceOf[CdmManifestDefinition]
      entDecl = subManifest.getEntities.asScala.find(_.getEntityName == entity.entityDec.getEntityName).get
      addPartitions(entDecl, partitionList, dataFolder)
    }else{
      entDecl= fetchManifest.getEntities.asScala.find(_.getEntityName == entity.entityDec.getEntityName).get
      addPartitions(entDecl, partitionList, dataFolder)
    }
    if(Constants.EXCEPTION_TEST) {
      throw new Exception("Mock exception on append")
    }
    fetchManifest.saveAsAsync(manifestFileName, true).get
  }

  def getOldPartitions(fs: FileSystem, cdmEntity: CDMEntity): ArrayBuffer[Path] = {

    val oldPartitions = new ArrayBuffer[Path]
    // If the manifest has partition patterns
    if(!cdmEntity.entityDec.getDataPartitionPatterns.isEmpty) {
      val it =  cdmEntity.entityDec.getDataPartitionPatterns.iterator()
      while(it.hasNext) {
        val partitionPattern = it.next().asInstanceOf[CdmDataPartitionPatternDefinition]
        val files = fs.globStatus(new Path(manifestPath + partitionPattern.getRootLocation + partitionPattern.getGlobPattern)).map(f => f.getPath)
        oldPartitions.appendAll(files)
      }
    }
    // If the manifest has explicit partitions
    else {
      val it =  cdmEntity.entityDec.getDataPartitions.iterator()
      while (it.hasNext) {
        val partition = it.next().asInstanceOf[CdmDataPartitionDefinition]
        val path = if (useSubManifest) {
          new Path(manifestPath + entityName + "/" + partition.getLocation)
        } else {
          new Path(manifestPath + partition.getLocation)
        }
        oldPartitions.append(path)
      }
    }
    oldPartitions
  }
}
