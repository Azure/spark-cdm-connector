package com.microsoft.cdm.log

import com.microsoft.cdm.utils.{Constants, Environment, SparkPlatform}
import com.microsoft.spark.metricevents.{ComponentEventPublisher, ComponentSparkEvent}
import org.slf4j.Logger
import org.slf4j.event.Level

object SparkCDMLogger {
  var APPNAME: String = "Spark-CDM Connector"

  // Application log
  def log(loglevel: Level, message: String, logger: Logger) = {
    loglevel match {
      case Level.ERROR => logger.error(message)
      case Level.INFO => logger.info(message)
      case Level.DEBUG => logger.debug(message)
      case Level.WARN => logger.warn(message)
      case _ =>
    }
  }

  def logEventToKusto(className: String, methodName: String, loglevel: Level, message: String, logger: Option[Logger] = None): Unit = {
    if(logger.getOrElse(null) != null) {
      log(loglevel, message, logger.get)
    }
    // Log to Kusto only on Synapse
    if(SparkPlatform.Synapse == Environment.sparkPlatform && Constants.KUSTO_ENABLED) {
      val event = ComponentSparkEvent(APPNAME, className, methodName, None, None, None, Some(message), loglevel)
      ComponentEventPublisher.publishComponentEvent(event)
    }
  }

  /* Log event to kusto to know performance of @param code */
  def logEventToKustoForPerf[T](code: => T, className: String, methodName: String, loglevel: Level, message: String, logger: Option[Logger] = None): T = {
    if(logger.getOrElse(null) != null) {
      log(loglevel, message, logger.get)
    }
    // Log to Kusto only on Synapse
    if(SparkPlatform.Synapse == Environment.sparkPlatform && Constants.KUSTO_ENABLED) {
      val event = ComponentSparkEvent(APPNAME, className, methodName, None, None, None, Some(message), loglevel)
      ComponentEventPublisher.publishComponentEventFor(code, event)
    }else{
      code
    }
  }
}
