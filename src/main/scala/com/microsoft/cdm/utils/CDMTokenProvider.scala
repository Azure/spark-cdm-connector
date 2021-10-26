package com.microsoft.cdm.utils

import com.microsoft.azure.synapse.tokenlibrary.TokenLibrary
import com.microsoft.commondatamodel.objectmodel.utilities.network.TokenProvider
import com.databricks.backend.daemon.data.client.adl.AdlGen2CredentialContextTokenProvider

class CDMTokenProvider(serConf: SparkSerializableConfiguration, accountName: String) extends TokenProvider {
  val platform = SparkPlatform.getPlatform(serConf.value)
  var startTime = System.currentTimeMillis()


  var curToken:String =
    if (platform == SparkPlatform.DataBricks) {
      val adpProvider = new AdlGen2CredentialContextTokenProvider()
      val dbToken = adpProvider.getToken().getAccessToken()
      dbToken
    } else if (platform == SparkPlatform.Synapse) {
      getSynapseToken
    } else {
      throw new Exception(Messages.managedIdentitiesSynapseDataBricksOnly)
    }

  def isTokenValid(): Boolean = {
    var validToken = true
    if (platform == SparkPlatform.DataBricks) {
      val endTime = System.currentTimeMillis();
      if ((endTime - startTime) > Constants.MILLIS_PER_HOUR) {
        validToken = false
      }
    }
    validToken
  }

  private def getSynapseToken: String = {
    val resource = s"""{"audience": "storage", "name": "$accountName"}"""
    val token = TokenLibrary.getAccessToken(resource)
    token.token
  }

  private def getCachedSynapseToken: String = {
    if (!TokenLibrary.isValid(curToken)) {
      curToken = getSynapseToken
    }
    "Bearer " + curToken
  }

  /*
   *  Databricks token cannot be called from an asynchronous context without an ExecutorService, which is
   *  not what the CDM-SDK does. Their getToken method (below) is called asynchronously without an
   *  executorService. However, since the Databricks token cannot be refreshed inside of a job, we cannot
   *  get a new token even if a job lasts longer than one hour. Therefore, we can simply grab the token
   *  during initialization (synchronous context call) and always return the same cached token. We
   *  need to error out if we try to use the same token on an request that spans over one hour. This is what the
   *  the isTokenValid() is  responsible for.
   *
   *  If the Databricks team implements a refreshToken mechanism, we need the CDM-SDK to implement an
   *  executorService for their asychronous calls so that we can call the Databricks's regreshToken()
   *  method at runTime.
   */
  private def getDataBricksToken: String = {
    "Bearer " + curToken
  }

  @Override
  override def getToken: String = {
    platform match {
      case SparkPlatform.DataBricks => getDataBricksToken
      case SparkPlatform.Synapse => getCachedSynapseToken
      case SparkPlatform.Other => throw new Exception(Messages.managedIdentitiesSynapseDataBricksOnly)
    }
  }
}