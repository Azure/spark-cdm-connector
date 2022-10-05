package com.microsoft.cdm.utils
import com.microsoft.cdm.utils.Constants.SASTOKEN_CONF_SETTING
import org.apache.hadoop.conf.Configuration

class CDMSASTokenProvider extends org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider {
  var sasToken = ""
  override def getSASToken(account: String, fileSystem: String, path: String, operation: String): String = {
    sasToken
  }

  override def initialize(configuration: Configuration, accountName: String): Unit = {
    sasToken = configuration.get(SASTOKEN_CONF_SETTING)
  }
}
