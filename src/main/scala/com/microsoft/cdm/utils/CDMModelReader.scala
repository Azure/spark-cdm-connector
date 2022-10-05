package com.microsoft.cdm.utils


class CDMModelReader(storage: String,
                     container: String,
                     manifestPath: String,
                     manifestFileName: String,
                     entityName: String,
                     entDefContAndPath: String,
                     auth: Auth,
                     tokenProvider: Option[CDMTokenProvider],
                     cdmSource: CDMSource.Value,
                     entityDefinitionStorage: String,
                     maxCDMThreads: Int)   extends CDMModelCommon (storage, container, manifestPath, manifestFileName,
  entityName, "", entDefContAndPath, auth,
  tokenProvider, "/", cdmSource, entityDefinitionStorage,
  maxCDMThreads){


}
