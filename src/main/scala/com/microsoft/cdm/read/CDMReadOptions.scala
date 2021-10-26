package com.microsoft.cdm.read

import com.microsoft.cdm.utils.{CDMOptions, Constants, Messages}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CDMReadOptions(options: CaseInsensitiveStringMap) extends  CDMOptions(options) {

  Constants.MODE = "read"

  val mode = if(options.containsKey("mode")) options.get("mode") else Constants.FAILFAST;
  // if mode is specified, it needs to either failfast or permissive
  if(Constants.DROPMALFORMED.equalsIgnoreCase(mode)) {
    throw new IllegalArgumentException(String.format(Messages.dropMalformedNotSupported))
  } else if(!Constants.PERMISSIVE.equalsIgnoreCase(mode) && !Constants.FAILFAST.equalsIgnoreCase(mode)) {
    throw new IllegalArgumentException(String.format(Messages.invalidMode))
  }

  var entDefContAndPath = ""
  var entityDefinitionStorage = ""

}
