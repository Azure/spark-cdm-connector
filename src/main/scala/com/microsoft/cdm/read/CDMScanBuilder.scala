package com.microsoft.cdm.read


import com.microsoft.cdm.utils.{DataConverter}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.slf4j.LoggerFactory

class CDMScanBuilder (cdmOptions: CDMReadOptions) extends ScanBuilder {
  val logger  = LoggerFactory.getLogger(classOf[CDMScanBuilder])

  override def build(): Scan = new CDMSimpleScan(cdmOptions.storage,
                                                 cdmOptions.container,
                                                 cdmOptions.manifestPath,
                                                 cdmOptions.manifestFileName,
                                                 cdmOptions.entity,
                                                 cdmOptions.entDefContAndPath,
                                                 cdmOptions.authCreds,
                                                 cdmOptions.conf,
                                                 new DataConverter(),
                                                 cdmOptions.cdmSource,
                                                 cdmOptions.entityDefinitionStorage,
                                                 cdmOptions.maxCDMThreads,
                                                 cdmOptions.mode)
}