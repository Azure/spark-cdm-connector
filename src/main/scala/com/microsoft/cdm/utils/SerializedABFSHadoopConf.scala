package com.microsoft.cdm.utils

import com.microsoft.cdm.utils.Constants.SASTOKEN_CONF_SETTING
import org.apache.hadoop.conf.Configuration

object SerializedABFSHadoopConf {
  def getConfiguration(storage: String,
                       container: String,
                       auth: Auth,
                       conf: Configuration): SparkSerializableConfiguration = {
    conf.set("fs.defaultFS", "abfss:/" + container + "@" + storage + "/")
    if (auth.getAuthType == CdmAuthType.AppReg.toString()) {
      conf.set("fs.azure.account.auth.type", "OAuth")
      conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
      conf.set("fs.azure.account.oauth2.client.id", auth.getAppId)
      conf.set("fs.azure.account.oauth2.client.secret", auth.getAppKey)
      conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/" + auth.getTenantId + "/oauth2/token")
    } else if (auth.getAuthType == CdmAuthType.Sas.toString()) {
      conf.set("fs.azure.account.auth.type", "SAS")
      conf.set("fs.azure.sas.token.provider.type", "com.microsoft.cdm.utils.CDMSASTokenProvider")
      conf.set("fs.azure.account.hns.enabled", "true")
      conf.set("fs.abfss.impl.disable.cache", "true") // disable cache for abfss creation so that the sas tokens for different folders don't conflict.
      conf.set(SASTOKEN_CONF_SETTING, auth.getSASToken) // setting to store the sas token
    }
    new SparkSerializableConfiguration(new Configuration(conf))
  }
}
