package com.microsoft.cdm.utils

import org.apache.spark.sql.types.{Metadata, StructType}

import scala.collection.mutable.HashMap

object StructTypeMetadata{
  object StructTypeMetadataMap{
    lazy val metadataMap = HashMap.empty[String, Metadata]
    def setMD(s: String, md: Metadata): Unit = metadataMap(s) = md
    def getMD(s: String): Metadata =  metadataMap.getOrElse(s, Metadata.empty)
  }

  // Since StructTypeMetadataMap is a singleton, it's the key that determines which
  // metadata object is returned.
  implicit class StructTypeMetadataMap(s: StructType) {
    def getMetadata(s: String): Metadata = StructTypeMetadataMap.getMD(s)
    def setMetadata(s:String, name: Metadata) = StructTypeMetadataMap.setMD(s, name)
  }
}