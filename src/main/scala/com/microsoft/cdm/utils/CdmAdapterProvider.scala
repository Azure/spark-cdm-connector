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

  private class CdmAppRegAdapter(val storage: String, val root: String, val auth: Auth) extends CdmAdapterProvider {
    override def getAdlsAdapter: AdlsAdapter = {
      new AdlsAdapter(storage, root, auth.getTenantId, auth.getAppId, auth.getAppKey)
    }
  }

  private class CdmSASAuthAdapter(val storage: String, val root: String, val auth: Auth) extends CdmAdapterProvider {
    override def getAdlsAdapter: AdlsAdapter = {
      val adapter = new AdlsAdapter(storage, root)
      adapter.setSasToken(auth.getSASToken)
      adapter
    }
  }

  def apply(storage: String, rootPath: String, auth: Auth, token: Option[CDMTokenProvider]): AdlsAdapter = {
    if(auth.getAuthType == CdmAuthType.AppReg.toString()) {
      new CdmAppRegAdapter(storage, rootPath, auth).getAdlsAdapter
    } else if(auth.getAuthType == CdmAuthType.Sas.toString()){
      new CdmSASAuthAdapter(storage, rootPath, auth).getAdlsAdapter
    } else {
      new CdmTokenAuthAdapter(storage, rootPath, token.get).getAdlsAdapter
    }
  }
}
