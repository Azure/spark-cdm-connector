package com.databricks.backend.daemon.data.client.adl
import shaded.databricks.v20180920_b33d810.org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken

class AdlGen2CredentialContextTokenProvider {
    def getToken(): AzureADToken = {
          throw new Exception("Error - this method should never be called")
    }
}
