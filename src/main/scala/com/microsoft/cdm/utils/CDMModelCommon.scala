package com.microsoft.cdm.utils


import java.io.ByteArrayOutputStream
import java.net.URLDecoder

import com.microsoft.cdm.log.SparkCDMLogger
import com.microsoft.cdm.utils.StructTypeMetadata.StructTypeMetadataMap
import com.microsoft.commondatamodel.objectmodel.cdm.{CdmAttributeGroupDefinition, CdmAttributeGroupReference, CdmAttributeItem, CdmCollection, CdmCorpusDefinition, CdmEntityDeclarationDefinition, CdmEntityDefinition, CdmManifestDefinition, CdmTraitReference, CdmTypeAttributeDefinition}
import com.microsoft.commondatamodel.objectmodel.enums.{CdmDataFormat, CdmObjectType, CdmPropertyName}
import com.microsoft.commondatamodel.objectmodel.persistence.cdmfolder.{AttributeGroupPersistence, TypeAttributePersistence}
import com.microsoft.commondatamodel.objectmodel.resolvedmodel.ResolveContext
import com.microsoft.commondatamodel.objectmodel.storage.{AdlsAdapter, CdmStandardsAdapter, GithubAdapter, ResourceAdapter, StorageAdapter}
import com.microsoft.commondatamodel.objectmodel.utilities.JMapper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, Metadata, MetadataBuilder, StructField, StructType}
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

import scala.collection.JavaConverters._

class CDMModelCommon (storage: String,
                      container: String,
                      manifestPath:String,
                      manifestFileName:String,
                      entityName: String,
                      entDefinition: String,
                      entDefContAndPath: String,
                      auth: Auth,
                      tokenProvider: Option[CDMTokenProvider],
                      overrideConfigPath : String,
                      var cdmSource: CDMSource.Value,
                      entityDefinitionStorage: String,
                      maxCDMThreads: Int) {

  var logger  = LoggerFactory.getLogger(classOf[CDMModelCommon])
  val cdmCorpus = new CdmCorpusDefinition
  cdmCorpus.setEventCallback(CDMCallback)
  val platform = Environment.sparkPlatform

  //root path should not end with a "/" for databricks to work
  assert(manifestPath.endsWith("/"))
  val rootPath  = container + manifestPath.dropRight(1)
  SparkCDMLogger.log(Level.INFO, "Path for ADLSAdapter: " + storage + rootPath, logger)

  val adlsAdapter = CdmAdapterProvider(storage, rootPath, auth, tokenProvider)
  cdmCorpus.getStorage.mount(Constants.SPARK_NAMESPACE, adlsAdapter)


  val cdmSourceAdapter = if (cdmSource == CDMSource.BUILTIN) new ResourceAdapter else new CdmStandardsAdapter
  cdmCorpus.getStorage.mount("cdm", cdmSourceAdapter)
  cdmCorpus.getStorage().setMaxConcurrentReads(maxCDMThreads);
  /* If "", use the CdmStandardsAdapter as the model root */
  if (entDefContAndPath ==  "") {
    cdmCorpus.getStorage.mount(Constants.SPARK_MODELROOT_NAMESPACE, cdmSourceAdapter)
  } else {

    val cdmADLSAdapter = CdmAdapterProvider(entityDefinitionStorage, entDefContAndPath, auth, tokenProvider)
    cdmCorpus.getStorage.mount(Constants.SPARK_MODELROOT_NAMESPACE, cdmADLSAdapter)

    // If overrideConfigPath is not present, look for config.json in SparkModelRoot path.
    if("/".equals(overrideConfigPath)) {
      try {
        val config = cdmCorpus.getStorage.fetchAdapter(Constants.SPARK_MODELROOT_NAMESPACE).readAsync("config.json").get()
        cdmCorpus.getStorage.mountFromConfig(config)
      }
      catch {
        case e: java.util.concurrent.ExecutionException => {
          SparkCDMLogger.log(Level.ERROR, "Config.json not found in " + entDefContAndPath + " ", logger)
        }
      }
    }
  }

  // If overrideConfigPath is present, override it.
  if(!"/".equals(overrideConfigPath)) {
    mountFromOverrideConfigPath()
  }

  if (!entDefinition.isEmpty()) {
    var entDefParent:String = null
    try {
      // Get the filename containing this entity
      entDefParent = entDefinition.dropRight(entDefinition.length - entDefinition.lastIndexOf('/'))
      cdmCorpus.getStorage().getNamespaceAdapters().get(Constants.SPARK_MODELROOT_NAMESPACE).readAsync(entDefParent).get
    } catch {
      case e: java.util.concurrent.ExecutionException => throw new IllegalArgumentException(
        String.format(Messages.entityDefinitionModelFileNotFound, entDefParent))
    }
  }


  setAuthMechanismTOAllNamespace()
  cdmCorpus.getStorage.setDefaultNamespace(Constants.SPARK_NAMESPACE)

  val dataConverter = new DataConverter()

  // Read the config path and mount it to corpus
  def mountFromOverrideConfigPath() = {

    val configAdaptor = CdmAdapterProvider(storage, overrideConfigPath, auth, tokenProvider)
    cdmCorpus.getStorage.mount("configAdls", configAdaptor)
    try {
      val config = cdmCorpus.getStorage.fetchAdapter("configAdls").readAsync("config.json").get()
      SparkCDMLogger.log(Level.INFO, "Overriding the config.json path..", logger)
      cdmCorpus.getStorage.mountFromConfig(config)
    }
    catch {
      case e: java.util.concurrent.ExecutionException => throw new IllegalArgumentException(String.format(Messages.configJsonPathNotFound, overrideConfigPath))
    }
  }

  // set the same secret/token to all the Adls Adapter definitions, since we are using a single ADLS account.
  def setAuthMechanismTOAllNamespace() = {
    cdmCorpus.getStorage.getNamespaceAdapters.values.asScala.foreach({ case (value: StorageAdapter) =>
      if(value.isInstanceOf[AdlsAdapter]) {
        if (auth.getAuthType == CdmAuthType.Token.toString()) {
          value.asInstanceOf[AdlsAdapter].setTokenProvider(tokenProvider.get)
        } else if (auth.getAuthType == CdmAuthType.AppReg.toString()) {
          value.asInstanceOf[AdlsAdapter].setSecret(auth.getAppKey)
        } else {
          value.asInstanceOf[AdlsAdapter].setSasToken(auth.getSASToken)
        }
      }
    })
  }

  /*
   * Derive the CDM datatypes for the CDM write based on the struct types itself, or CDM
   * metadata in a struct field if it exists. This is currently used to determine how to
   * write timestamp out when it can be of type CDM.DateTime or CDM.DateTimeOffset
   */
  def getCDMSchemaFromStructType(schema: StructType): Iterable[Any]= {
    val dv = new DataConverter
    val list = schema.map(field => {
      // Only use the StructType for the CDMSchema type if there is are no metadata fields and neither the "traits" or "datatype"
      // are set
      if (field.metadata == null || (!field.metadata.contains(Constants.MD_TRAITS) && !field.metadata.contains(Constants.MD_DATATYPE_OVERRIDE))) {
        field.dataType match {
          case st: StructType => getCDMSchemaFromStructType(field.dataType.asInstanceOf[StructType])
          case DecimalType() => {
            val decimal = field.dataType.asInstanceOf[DecimalType]
            CDMDecimalType(decimal.precision,decimal.scale)
          }
          case ar : ArrayType => {
            if(!field.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]) {
              throw new UnsupportedOperationException(Messages.onlyStructsInArraySupported)
            }
            getCDMSchemaFromStructType(field.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
          }
          case _ => dv.toCdmDataFormat(field.dataType).toString
        }
      } else {
        if (field.metadata.contains(Constants.MD_DATATYPE_OVERRIDE)) {
          val dataType= field.metadata.getString(Constants.MD_DATATYPE_OVERRIDE)
          dataConverter.toCdmDataFormatOverride(dataType).toString
        } else {
          assert(field.metadata.contains(Constants.MD_TRAITS))
          /* Use the attribute traits stashed away in the metadata to drive the CDM type */
          val attributeJson = field.metadata.getString(Constants.MD_TRAITS)
          val jval = JMapper.MAP.readTree(attributeJson)
          val attrDef = TypeAttributePersistence.fromData(new ResolveContext(cdmCorpus),jval)
          if(attrDef.fetchDataFormat() == CdmDataFormat.Decimal) {
            var precision = 0;
            var scale = 0;
            /* Get the trait with namedReference = is.dataFormat.numeric.shaped. We also want to make sure there are arguments in the trait.
               In CDMModelWriter, we are explicitly adding the trait "is.dataFormat.numeric.shaped", so isFromProperty = false */
            var traitName = attrDef.getAppliedTraits().asScala.find(each => Constants.CDM_DECIMAL_TRAIT.equals(each.getNamedReference) && !each.asInstanceOf[CdmTraitReference].isFromProperty && each.asInstanceOf[CdmTraitReference].getArguments.size() > 0 ).getOrElse(-1)
            // default values in Spark DecimalType, in case the definition file does not have a precision and scale information
            if ( traitName == -1 ||
              traitName.asInstanceOf[CdmTraitReference].getArguments.fetchValue("precision") == null ||
              traitName.asInstanceOf[CdmTraitReference].getArguments.fetchValue("scale") == null
            ) {
              precision = Constants.CDM_DEFAULT_PRECISION
              scale = Constants.CDM_DEFAULT_SCALE
            }else {
              precision = traitName.asInstanceOf[CdmTraitReference].getArguments.fetchValue("precision").toString.toInt
              scale = traitName.asInstanceOf[CdmTraitReference].getArguments.fetchValue("scale").toString.toInt
            }
            CDMDecimalType(precision,scale)
          }else if(attributeJson.contains("attributeGroupName")){
            val attrGroupDef = AttributeGroupPersistence.fromData(new ResolveContext(cdmCorpus),jval)
            if(attrGroupDef.getExhibitsTraits.indexOf(Constants.CDM_ARRAY_TRAIT) != -1) {
              if(!field.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]) {
                throw new UnsupportedOperationException(Messages.onlyStructsInArraySupported)
              }
              getCDMSchemaFromStructType(field.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
            } else {
              getCDMSchemaFromStructType(field.dataType.asInstanceOf[StructType])
            }
          } else {
            CDMDataFormat.withName(attrDef.fetchDataFormat().name()).toString
          }
        }
      }
    })
    list
  }

  def getType(attrs: CdmCollection[CdmAttributeItem]): Iterable[Any] = {
    val attrTypes = attrs.asScala.map(attr => {
      attr.getObjectType match {
        case CdmObjectType.TypeAttributeDef => {
          val attrDef = attr.asInstanceOf[CdmTypeAttributeDefinition]
          val dataFormat = attrDef.fetchDataFormat().name()
          if(dataFormat == CDMDataFormat.Decimal.toString) {
            var precision, scale = 0;
            // Get the trait with namedReference = is.dataFormat.numeric.shaped. We also want to make sure there are arguments in the trait. In CDMModelWriter, we are explicitly adding the trait "is.dataFormat.numeric.shaped", so isFromProperty = false
            val traitName = attrDef.getAppliedTraits().asScala.find(each => Constants.CDM_DECIMAL_TRAIT.equals(each.getNamedReference) && !each.asInstanceOf[CdmTraitReference].isFromProperty && each.asInstanceOf[CdmTraitReference].getArguments.size() > 0 ).getOrElse(-1)
            // default values in Spark DecimalType, in case the definition file does not have a precision and scale information
            if( traitName == -1 ) {
              precision = Constants.CDM_DEFAULT_PRECISION
              scale = Constants.CDM_DEFAULT_SCALE
            }else {
              precision = traitName.asInstanceOf[CdmTraitReference].getArguments.fetchValue("precision").toString.toInt
              scale = traitName.asInstanceOf[CdmTraitReference].getArguments.fetchValue("scale").toString.toInt
            }
            CDMDecimalType(precision, scale)
          }else {
            try{
              CDMDataFormat.withName(dataFormat).toString
            }catch{
              case _ =>  throw new UnsupportedOperationException(String.format(Messages.cdmDataFormatNotYetSupported, attrDef.getName))
            }
          }
        }
        case CdmObjectType.AttributeGroupRef => {
          val ref = attr.asInstanceOf[CdmAttributeGroupReference].getExplicitReference;
          val attrDef = ref.asInstanceOf[CdmAttributeGroupDefinition]
          getType(attrDef.getMembers)
        }
      }
    })
    attrTypes
  }

  /*
     * The entity to write out already exists, so it's columns must as well, therefore use
     * that to pass the CDM types to. This is currently used to determine how to
     * write timestamp out when it can be of type CDM.DateTime or CDM.DateTimeOffset
     */
  def getCDMSchemaTypesAsSeq(entityName: String, conf: SparkSerializableConfiguration): Iterable[Any] = {
    val entity = entityDecHandleError(entityName, conf)
    val relPath = getRelPath(entity.entityDec.getEntityPath)
    val eDef= cdmCorpus.fetchObjectAsync(relPath, entity.parentManifest, true).get().asInstanceOf[CdmEntityDefinition]
    val attrTypes = getType(eDef.getAttributes);
    attrTypes
  }

  /*
   * This entity is being written for the first time, but we know the schema because the entity
   * comes from a predefined schema, therefore use that to pass the CDM types to. This is
   * currently used to determine how to write timestamp out when it can be of type
   * CDM.DateTime or CDM.DateTimeOffset
   */
  def getCDMSchemaTypesAsSeqFromPredefinedSource(entityPath:String): Iterable[Any] = {
    val eDef = cdmCorpus.fetchObjectAsync(Constants.SPARK_MODELROOT_NAMESPACE + ":" + entityPath).get.asInstanceOf[CdmEntityDefinition]
    val resolvedDef = eDef.createResolvedEntityAsync("temporary").get
    val attrTypes = getType(resolvedDef.getAttributes)
    attrTypes
  }

  def getManifest(conf : SparkSerializableConfiguration): CdmManifestDefinition = {
    val fs = conf.getFileSystem()
    val path = URLDecoder.decode(manifestPath +  manifestFileName, "UTF-8")
    if(fs.exists(new Path(path))) {
      cdmCorpus.fetchObjectAsync(manifestFileName).get().asInstanceOf[CdmManifestDefinition]
    } else {
      null
    }
  }

  def entityExists(entityName: String, conf : SparkSerializableConfiguration): CDMEntity = {
    val manifest = getManifest(conf)
    val entity = getEntity(entityName, manifest)
    entity
  }

  def entityDecHandleError(entityName: String, conf : SparkSerializableConfiguration) : CDMEntity = {
    val manifest = getManifest(conf)
    if(manifest == null) throw new RuntimeException ("Manifest doesn't exist: " + manifestFileName)
    val entity = getEntity(entityName, manifest)
    if(entity.entityDec == null) throw new RuntimeException ("Can't find entity: " + entityName)
    entity
  }


  def getEntity (entityName: String, manifest: CdmManifestDefinition): CDMEntity = {
    if(manifest == null) return CDMEntity(null, null, null, null)
    val entity = manifest.getEntities().asScala.find(_.getEntityName == entityName)
    //return the entity at the root of manifest
    if (entity != None) {
      // return a tuple of the manifest and entity declartion. The tuple allows us to get the entity definition from its
      // parent object, the manifest
      val eDec = entity.get
      eDec.getInDocument()
      return CDMEntity(manifest, manifest, eDec, null)
    } else {
      // There should be an idiomatic scala pattern to achieve this
      for (subManifest <- manifest.getSubManifests.asScala) {
        val subManDef = cdmCorpus.fetchObjectAsync(subManifest.getDefinition, manifest).join.asInstanceOf[CdmManifestDefinition]
        if(subManDef == null) throw new Exception("Submanifest - " + subManifest.getDefinition + " not found")
        val entity = subManDef.getEntities().asScala.find(_.getEntityName == entityName)
        if (entity != None) {
          val eDec = entity.get
          eDec.getInDocument()
          return CDMEntity(manifest, subManDef, eDec, null)
        }
      }
      CDMEntity(manifest, manifest, null, null)
    }
  }


  def getPropertiesAsMetadataForAttrGroup(attrDef: CdmAttributeGroupDefinition): Metadata = {
    val mdb = new MetadataBuilder()
    //serialize all the json traits for an attribute into a string
    val baos = new ByteArrayOutputStream()
    val attribute = AttributeGroupPersistence.toData(attrDef, null, null)
    JMapper.MAP.writeValue(baos, attribute)
    mdb.putString("traits", baos.toString())
    mdb.build()
  }

  def getStructType( attItem: CdmCollection[CdmAttributeItem]): StructType = {
    val structType = StructType(attItem.asScala.map(attr => {
      var metadata: Metadata = null
      var fieldType: DataType = null
      var name: String = null
      attr.getObjectType match {
        case CdmObjectType.TypeAttributeDef => {
          val attrDef = attr.asInstanceOf[CdmTypeAttributeDefinition]
          name = attrDef.getName()
          var precision = 0;
          var scale = 0;
          var dataFormat = attrDef.fetchDataFormat().toString
          // ADF assumes Unknown is of type String, so we are doing the same
          if (dataFormat== "Unknown") {
            SparkCDMLogger.log(Level.WARN, "Mapping Unknown type to String for attribute " + name, logger)
            dataFormat = "String"
          }
          if(dataFormat == CDMDataFormat.Decimal.toString) {
            /* Get the trait with namedReference = is.dataFormat.numeric.shaped. We also want to make sure there are arguments in the trait.
            In CDMModelWriter, we are explicitly adding the trait "is.dataFormat.numeric.shaped", so isFromProperty = false */
            var traitName = attrDef.getAppliedTraits().asScala.find(each => Constants.CDM_DECIMAL_TRAIT.equals(each.getNamedReference) && !each.asInstanceOf[CdmTraitReference].isFromProperty && each.asInstanceOf[CdmTraitReference].getArguments.size() > 0 ).getOrElse(-1)
            // default values in Spark DecimalType, in case the definition file does not have a precision and scale information
            if ( traitName == -1 ||
              traitName.asInstanceOf[CdmTraitReference].getArguments.fetchValue("precision") == null ||
              traitName.asInstanceOf[CdmTraitReference].getArguments.fetchValue("scale") == null
            ) {
              precision = Constants.CDM_DEFAULT_PRECISION
              scale = Constants.CDM_DEFAULT_SCALE
              // Add the decimal trait and arguments to the attribute. This information will be saved in the metadata.
              traitName = attrDef.getAppliedTraits().add(Constants.CDM_DECIMAL_TRAIT)
              traitName.asInstanceOf[CdmTraitReference].setFromProperty(false)
              traitName.asInstanceOf[CdmTraitReference].getArguments.add("precision", precision)
              traitName.asInstanceOf[CdmTraitReference].getArguments.add("scale", scale)
            }else {
              precision = traitName.asInstanceOf[CdmTraitReference].getArguments.fetchValue("precision").toString.toInt
              scale = traitName.asInstanceOf[CdmTraitReference].getArguments.fetchValue("scale").toString.toInt
            }
          }
          fieldType = dataConverter.toSparkType(CDMDataFormat.withName(dataFormat), precision, scale)
          metadata = getPropertiesAsMetadata(attrDef)
        }
        case CdmObjectType.AttributeGroupRef => {
          val ref = attr.asInstanceOf[CdmAttributeGroupReference].getExplicitReference;
          val attrDef = ref.asInstanceOf[CdmAttributeGroupDefinition]
          name = attrDef.getName()
          /* If attrDef is an array of entities, it should have `is.linkedEntity.array` trait
          https://github.com/microsoft/CDM/blob/master/schemaDocuments/primitives.cdm.json#L454 */
          if(attrDef.getExhibitsTraits.indexOf(Constants.CDM_ARRAY_TRAIT) != -1) {
            fieldType = ArrayType.apply(getStructType(attrDef.getMembers))
          }else{
            fieldType = getStructType(attrDef.getMembers)
          }
          metadata = getPropertiesAsMetadataForAttrGroup(attrDef)
        }
      }
      StructField(name, fieldType, true, metadata)
    }).toArray)
    structType
  }

  // Return the schema, in StructType format, for an entity
  def getSchema(relManifest: CdmManifestDefinition, entity: CdmEntityDeclarationDefinition): StructType = {
    var relPath = getRelPath(entity.getEntityPath)
    val eDef= cdmCorpus.fetchObjectAsync(relPath,relManifest, true).get().asInstanceOf[CdmEntityDefinition]
    val structType = getStructType(eDef.getAttributes)
    val imports = eDef.getInDocument.getImports.asScala.map(a => {
      new MetadataBuilder()
        .putString("corpusPath", a.getCorpusPath)
        .putString("moniker", a.getMoniker)
        .build()
    }).toArray
    val md = new MetadataBuilder().putMetadataArray("imports", imports).build()
    structType.setMetadata(entityName, md)
    structType
  }

  def getPropertiesAsMetadata(attrDef: CdmTypeAttributeDefinition): Metadata = {
    val mdb = new MetadataBuilder()
    //serialize all the json traits for an attribute into a string
    val baos = new ByteArrayOutputStream()
    attrDef.setAttributeContext(null)
    val attribute = TypeAttributePersistence.toData(attrDef.asInstanceOf[CdmTypeAttributeDefinition], null, null)
    JMapper.MAP.writeValue(baos, attribute)
    mdb.putString("traits", baos.toString())

    // Retrieve the Properties
    for (property <-CdmPropertyName.values) {
      property match {
        case CdmPropertyName.CDM_SCHEMAS => null
        case CdmPropertyName.DATA_FORMAT =>  {
          val value = attrDef.fetchDataFormat()
          if (value != null) mdb.putString(CdmPropertyName.DATA_FORMAT.toString, value.name())
        }
        case CdmPropertyName.DEFAULT=> {
          val value =  attrDef.fetchDefaultValue()
          if (value != null) mdb.putString(CdmPropertyName.DEFAULT.toString, value.toString)
        }
        case CdmPropertyName.DESCRIPTION=> {
          val value =  attrDef.fetchDescription()
          if (value != null) mdb.putString(CdmPropertyName.DESCRIPTION.toString, value)
        }
        case CdmPropertyName.DISPLAY_NAME=> {
          val value =  attrDef.fetchDisplayName()
          if (value != null) mdb.putString(CdmPropertyName.DISPLAY_NAME.toString, value)
        }
        case CdmPropertyName.IS_NULLABLE=>  {
          val value = attrDef.fetchIsNullable()
          if (value != null) mdb.putBoolean(CdmPropertyName.IS_NULLABLE.toString, value)
        }
        case CdmPropertyName.IS_PRIMARY_KEY => {
          val value = attrDef.fetchIsPrimaryKey()
          if (value != null) mdb.putBoolean(CdmPropertyName.IS_PRIMARY_KEY.toString, value)
        }
        case CdmPropertyName.IS_READ_ONLY=> {
          val value =  attrDef.fetchIsReadOnly()
          if (value != null) mdb.putBoolean(CdmPropertyName.IS_READ_ONLY.toString, value)
        }
        case CdmPropertyName.MAXIMUM_LENGTH=> {
          val value =  attrDef.fetchMaximumLength()
          if (value != null) mdb.putLong(CdmPropertyName.MAXIMUM_LENGTH.toString, value.toLong)
        }
        case CdmPropertyName.MAXIMUM_VALUE=> {
          val value =  attrDef.fetchMaximumValue()
          if (value != null) mdb.putString(CdmPropertyName.MAXIMUM_VALUE.toString, value)
        }
        case CdmPropertyName.MINIMUM_VALUE=> {
          val value =  attrDef.fetchMinimumValue()
          if (value != null) mdb.putString(CdmPropertyName.MINIMUM_VALUE.toString, value)
        }
        case CdmPropertyName.PRIMARY_KEY=>  null
        case CdmPropertyName.SOURCE_NAME=> {
          val value =  attrDef.fetchSourceName()
          if (value != null) mdb.putString(CdmPropertyName.SOURCE_NAME.toString, value)
        }
        case CdmPropertyName.SOURCE_ORDERING=> {
          /* TODO: Fix when CDM library switches from casting Integer to parsing
          val value =  attrDef.fetchSourceOrdering()
          if (value != null) mdb.putLong(CdmPropertyName.SOURCE_ORDERING.toString, value.toLong)
           */
        }
        case CdmPropertyName.VALUE_CONSTRAINED_TO_LIST=> {
          val value =  attrDef.fetchValueConstrainedToList()
          if (value != null) mdb.putBoolean(CdmPropertyName.VALUE_CONSTRAINED_TO_LIST.toString, value)
        }
        case CdmPropertyName.VERSION=>  null
      }
    }
    mdb.build()
  }

  def getRelPath = (path: String) =>  {
    if(path.contains(":/")) {
      path.substring(path.indexOf(":/")+2)
    } else {
      path
    }
  }
}
