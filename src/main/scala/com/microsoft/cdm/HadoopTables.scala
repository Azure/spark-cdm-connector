package com.microsoft.cdm

import com.microsoft.cdm.utils.{CDMEntity, CDMModelCommon, CDMOptions, CDMTokenProvider, EntityNotFoundException, ManifestNotFoundException, SerializedABFSHadoopConf, SparkSerializableConfiguration}

class HadoopTables() {


  def load(cdmOptions: CDMOptions): CDMEntity = {
    val serializedHadoopConf  = SerializedABFSHadoopConf.getConfiguration(cdmOptions.storage, cdmOptions.container, cdmOptions.authCreds, cdmOptions.conf)

    val tokenProvider =  if (cdmOptions.authCreds.appId.isEmpty) Some(new CDMTokenProvider(serializedHadoopConf, cdmOptions.storage)) else None

    val cdmModel = new CDMModelCommon(cdmOptions.storage,
      cdmOptions.container,
      cdmOptions.manifestPath,
      cdmOptions.manifestFileName,
      cdmOptions.entity,
      "",
      "",
      cdmOptions.authCreds, tokenProvider,
      cdmOptions.overrideConfigPath,
      cdmOptions.cdmSource,
      "",
      cdmOptions.maxCDMThreads)

    val cdmEntity = cdmModel.entityExists(cdmOptions.entity, serializedHadoopConf)

    if(cdmEntity.rootManifest == null) {
      throw ManifestNotFoundException("Manifest doesn't exist: " + cdmOptions.manifestFileName)
    }
    if (cdmEntity.entityDec != null ) {
      cdmEntity.schema  = cdmModel.getSchema(cdmEntity.parentManifest, cdmEntity.entityDec)
      cdmEntity
    } else {
      throw EntityNotFoundException("Entity " + cdmOptions.entity + " not found in manifest - " + cdmOptions.manifestFileName)
    }
  }

}
