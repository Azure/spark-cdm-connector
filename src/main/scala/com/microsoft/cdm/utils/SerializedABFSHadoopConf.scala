package com.microsoft.cdm.utils

import org.apache.hadoop.conf.Configuration

object SerializedABFSHadoopConf {
  def getConfiguration(storage: String,
                       container: String,
                       authCredential: AuthCredential,
                       conf: Configuration): SparkSerializableConfiguration = {
    conf.set("fs.defaultFS", "abfss:/" + container + "@" + storage + "/")
    if (!authCredential.appId.isEmpty) {
      conf.set("fs.azure.account.auth.type", "OAuth")
      conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
      conf.set("fs.azure.account.oauth2.client.id", authCredential.appId)
      conf.set("fs.azure.account.oauth2.client.secret", authCredential.appKey)
      conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/" + authCredential.tenantId + "/oauth2/token")
    }
    new SparkSerializableConfiguration(new Configuration(conf))
  }
}
