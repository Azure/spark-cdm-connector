package com.microsoft.cdm.utils

object CdmAuthType extends Enumeration {
  val AppReg, Sas, Token = Value
}

trait Auth {
  def getAppId: String
  def getAppKey: String
  def getTenantId: String
  def getSASToken: String
  def getAuthType: String
}

case class SasAuth(sasToken: String) extends Auth {
  override def getAuthType: String = CdmAuthType.Sas.toString()
  override def getSASToken: String = sasToken
  override def getAppId: String = ""
  override def getAppKey: String = ""
  override def getTenantId: String = ""
}

case class AppRegAuth(appId: String, appKey: String, tenantId: String) extends Auth {
  override def getAuthType: String = CdmAuthType.AppReg.toString()
  override def getAppId: String = appId
  override def getAppKey: String = appKey
  override def getTenantId: String = tenantId
  override def getSASToken: String = ""
}

case class TokenAuth() extends Auth {
  override def getAuthType: String = CdmAuthType.Token.toString()
  override def getAppId: String = ""
  override def getAppKey: String = ""
  override def getTenantId: String = ""
  override def getSASToken: String = ""
}
