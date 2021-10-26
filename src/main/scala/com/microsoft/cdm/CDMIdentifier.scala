package com.microsoft.cdm

import com.microsoft.cdm.utils.CDMOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CDMIdentifier(options: CaseInsensitiveStringMap) extends Identifier{

  val cdmOptions = new CDMOptions(options)

  override def namespace(): Array[String] = Array(cdmOptions.storage, cdmOptions.container, cdmOptions.manifestFileName)

  override def name(): String = cdmOptions.entity

  val optionsAsHashMap = options
}
