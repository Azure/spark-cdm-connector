package com.microsoft.cdm.utils

import java.math.MathContext

object Messages {
  val invalidThreadCount= "The maxCDMThreads parameter is invalid"
  val invalidIndexSchema = "The dataframe schema does not match the cdm schema for field \"%s\". Path : \"%s\""
  val mismatchedSizeSchema = "The dataframe schema and cdm schema don't have an equal number of fields. Entity Path:  \"%s\""
  val invalidCompressionFormat = "Invalid Compression format specified - %s"
  val entityDefinitionModelFileNotFound= "Entity definition model file \"%s\" not found"
  val invalidDecimalFormat = "Invalid Decimal(%s,%s)"
  val onlyStructsInArraySupported = "Arrays with primitive types/MapType/ArrayType not yet supported"
  val cdmDataFormatNotYetSupported = "CDM dataformat for %s  not yet supported"
  val nestedTypesNotSupported = "Cannot write nested types to csv file. Please change the format to parquet."
  val characterNotInRange = "Invalid Delimiter - %s. The provided delimiter should be in a valid char range : 0-65535"
  val managedIdentitiesSynapseDataBricksOnly ="Managed identities only supported on Synapse or Databricks"
  val managedIdentitiesDatabricksTimeout = "Databricks jobs must not last more than 1 hour (they have not refresh mechanism)"
  val invalidDelimiterCharacter = "Invalid Delimiter - %s. Only one character is allowed in delimiter. Input should be a character."
  val overrideConfigJson = "%s. Please override config.json by option \"configPath\""
  val configJsonPathNotFound = "Config.json not found in %s."
  val incorrectDataFolderFormat = "Incorrect Data Folder format %s - follow DateTimeFormatter format"
  val invalidCDMSourceName = "Invalid cdmSource provided - %s. cdmSource can either be - builtin or referenced"
  val invalidBothStandardAndEntityDefCont = "Specifying CdmStandard and entityDefinitionModelRoot is not valid"
  val entityDefStorageAppCredError = "entityDefinitionStorage option is supported only with managed identities."
  val incompatibleFileWithDataframe = "The number of columns in CSV/parquet file is not equal to the number of fields in Spark StructType. Either modify the attributes in manifest to make it equal to the number of columns in CSV/parquet files or modify the csv/parquet file"
  val invalidMode = "Invalid mode provided. Suports - permissive or failfast"
  val invalidPermissiveMode = "Permissive Mode not supported with Parquet files"
  val dropMalformedNotSupported ="DropMalformed mode is not supported"
}