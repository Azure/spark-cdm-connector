package com.microsoft.cdm.utils

import com.microsoft.commondatamodel.objectmodel.storage.AdlsAdapter
trait CdmAdapterProvider {
  def getAdlsAdapter : AdlsAdapter
}

object CdmAdapterProvider {

  private class CdmTokenAuthAdapter(val storage: String, val root: String, val tokenProvider: CDMTokenProvider) extends CdmAdapterProvider {
    override def getAdlsAdapter : AdlsAdapter = {
      new AdlsAdapter(storage, root, tokenProvider)
    }
  }

  private class CdmCredentialAuthAdapter(val storage: String, val root: String, val authCredential: AuthCredential) extends CdmAdapterProvider {
    override def getAdlsAdapter: AdlsAdapter = {
      new AdlsAdapter(storage, root, authCredential.tenantId, authCredential.appId, authCredential.appKey)
    }
  }

  def apply(storage: String, rootPath: String, authCredential: AuthCredential, token: Option[CDMTokenProvider]): AdlsAdapter = {
    if(authCredential.appId.isEmpty) {
      new CdmTokenAuthAdapter(storage, rootPath, token.get).getAdlsAdapter
    }else {
      new CdmCredentialAuthAdapter(storage, rootPath, authCredential).getAdlsAdapter
    }
  }
}
