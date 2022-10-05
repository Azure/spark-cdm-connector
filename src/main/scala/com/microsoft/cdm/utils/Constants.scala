package com.microsoft.cdm.utils

import java.math.MathContext

import org.apache.hadoop.conf.Configuration

/**
 * Various constants for spark-csv.
 */
object Constants {

  // TODO: ensure these match the data provided
  //val DATE_FORMATS = Array("MM/dd/yyyy", "MM/dd/yyyy hh:mm:ss a")
  //val OUTPUT_FORMAT = "MM/dd/yyyy hh:mm:ss a"

  val MILLIS_PER_HOUR = 3600000
  val DECIMAL_PRECISION = 37
  val MATH_CONTEXT = new MathContext(28)
  val SINGLE_DATE_FORMAT = "M/d/yyyy"
  val TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"

  val MD_TRAITS = "traits"
  val MD_DATATYPE_OVERRIDE = "datatype"
  val MD_DATATYPE_OVERRIDE_TIME= "Time"

  val CDM_ARRAY_TRAIT = "is.linkedEntity.array"
  val CDM_DECIMAL_TRAIT = "is.dataFormat.numeric.shaped"

  val LOGICAL_ENTITY_DIR = "LogicalDefinition"
  val defaultCompressionFormat = "snappy"
  val SPARK_MODELROOT_NAMESPACE = "SparkModelRoot"
  val CDM_DEFAULT_PRECISION = 18
  val CDM_DEFAULT_SCALE = 4
  var PRODUCTION = true
  val DEFAULT_DELIMITER = ','
  val SPARK_NAMESPACE = "SparkManifestLocation"
  val PARTITION_DIR_PATTERN ="%s-%s-%s"
  val GLOB_PATTERN="%s-%s-*.%s"
  var EXCEPTION_TEST = false
  var SUBMANIFEST_WITH_OVERWRITTEN_PARTITIONS =  "%s.rename.manifest.cdm.json"
  var MODE = ""
  var MODEL_JSON = "model.json"
  var KUSTO_ENABLED = true
  var SASTOKEN_CONF_SETTING = "com.microsoft.cdm.sastoken"

  // For permissive/fail fast CSV reading mode.
  // Could be an Enumeration, but strings enable case-insensitive comparison
  val PERMISSIVE = "permissive"
  val FAILFAST = "failfast"
  val DROPMALFORMED= "dropmalformed"
}

object Environment {
  var sparkPlatform: SparkPlatform.Value = _
}
class Constants {

}
