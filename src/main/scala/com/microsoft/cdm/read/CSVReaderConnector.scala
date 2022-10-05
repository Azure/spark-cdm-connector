package com.microsoft.cdm.read

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.ChronoUnit
import com.microsoft.cdm.utils.{Constants, CsvParserFactory, SparkSerializableConfiguration}
import com.microsoft.cdm.log.SparkCDMLogger
import com.univocity.parsers.csv.CsvParser
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import scala.collection.mutable

class CSVReaderConnector(httpPrefix:String, filePath: String, serConf:SparkSerializableConfiguration, delimiter: Char, mode: String, schema: StructType, hasHeader: Boolean) extends ReaderConnector {
  val logger  = LoggerFactory.getLogger(classOf[CSVReaderConnector])
  SparkCDMLogger.log(Level.DEBUG, "CSV Reader for partition at path: " + httpPrefix + filePath, logger)

  private var goodRow:Boolean = true
  private var parser: CsvParser = _

  /**
   * Map the index with a tuple (Any, Generic function)
   * tuple._1 (Any) Format of value you are parsing
   * tuple._2 (Generic function) Generic function to plug in format
   */
  private var indexToCachedDateFunction: Array[(Any, (String, Any, Int, String) => LocalDate)] = _
  private var indexToCachedDateTimeFunction: Array[(Any, (String, Any, Int, String) => java.lang.Long)] = _

  /**
   * Formats have the format mapped to a generic function.
   * The generic function will take in a format to perform the correct conversions
   */
  private lazy val dateFormatStrings : mutable.Map[String, (String, Any, Int, String) => LocalDate] = mutable.Map(
    "yyyy-MM-dd" -> parseGenericDateCached1,
    "M/d/yyyy" -> parseGenericDateCached1)

  // @transient + lazy required to ensure that DateTimeFormatter is not serialized when sent to executor
  @transient private lazy val localTimeFormats : mutable.Map[DateTimeFormatter, (String, Any, Int, String) => java.lang.Long] = mutable.Map(
    DateTimeFormatter.ISO_OFFSET_DATE_TIME -> parseGenericDateTimeCached1,
    DateTimeFormatter.ISO_INSTANT -> parseGenericDateTimeCached1)
  
  @transient private lazy val localTimeFormat2 : mutable.Map[DateTimeFormatter, (String, Any, Int, String) => java.lang.Long] = mutable.Map(
    DateTimeFormatter.ISO_LOCAL_DATE_TIME -> parseGenericDateTimeCached2)

  private lazy val localTimeFormatsNonStandard : mutable.Map[String, (String, Any, Int, String) => java.lang.Long] = mutable.Map(
    "M/d/yyyy H:mm" -> parseGenericDateTimeCached3,
    "M/d/yyyy h:mm:ss a" -> parseGenericDateTimeCached3,
    "M/d/yyyy H:mm:ss" -> parseGenericDateTimeCached3,
    "yyyy-MM-dd H:mm:ss.S" -> parseGenericDateTimeCached3,
    "yyyy-MM-dd H:mm:ss.SS" -> parseGenericDateTimeCached3,
    "yyyy-MM-dd H:mm:ss.SSS" -> parseGenericDateTimeCached3,
    "yyyy-MM-dd H:mm:ss.SSSS" -> parseGenericDateTimeCached3,
    "yyyy-MM-dd H:mm:ss.SSSSS" -> parseGenericDateTimeCached3,
    "yyyy-MM-dd H:mm:ss.SSSSSS" -> parseGenericDateTimeCached3,
    "yyyy-MM-dd H:mm:ss" -> parseGenericDateTimeCached3,
    "MMM d yyyy h:mma" -> parseGenericDateTimeCached3)

  private lazy val dateFormatStringsAsDateTime : mutable.Map[String, (String, Any, Int, String) => java.lang.Long] = mutable.Map(
    "yyyy-MM-dd" -> parseGenericDateTimeCached4,
    "M/d/yyyy" -> parseGenericDateTimeCached4)

  private lazy val timeFormatStrings : mutable.Map[String, (String, Any, Int, String) => java.lang.Long] = mutable.Map(
    "HH:mm:ss" -> parseGenericDateTimeCached5,
    "HH:mm:ss.S" -> parseGenericDateTimeCached5,
    "HH:mm:ss.SS" -> parseGenericDateTimeCached5,
    "HH:mm:ss.SSS" -> parseGenericDateTimeCached5,
    "HH:mm:ss.SSSS" -> parseGenericDateTimeCached5,
    "HH:mm:ss.SSSSS" -> parseGenericDateTimeCached5,
    "HH:mm:ss.SSSSSS" -> parseGenericDateTimeCached5)

  def build: Unit = {
    try {
      val path = new Path(filePath)
      val inputFile = HadoopInputFile.fromPath(path, serConf.value)
      val inputStream = inputFile.newStream()
      parser = CsvParserFactory.build(delimiter)
      parser.beginParsing {
        inputStream
      }
      // Parse first row to catch methods
      val tempStream = inputFile.newStream()
      val tempParser = CsvParserFactory.build(delimiter)
      tempParser.beginParsing {
        tempStream
      }
      var temp = tempParser.parseNext()
      if (hasHeader) { // if first row is a header, then read next
        temp = tempParser.parseNext()
      }
      if (temp != null) { // if not null/CSV actually contains data, then set as first row
        val firstRow = temp.asInstanceOf[Array[Any]]
        // PARSE FIRST ROW
        if (firstRow.length > schema.fields.length) {
          schema.zipWithIndex.map{ case (col, index) =>
            indexToCachedDateFunction = new Array[(Any, (String, Any, Int, String) => LocalDate)](schema.size)
            indexToCachedDateTimeFunction = new Array[(Any, (String, Any, Int, String) => java.lang.Long)](schema.size)
            val dataType = schema.fields(index).dataType
            jsonToData(dataType, firstRow.apply(index), index, mode)
          }
        } else if (firstRow.length < schema.fields.length) {
          // When there are fewer columns in the CSV file the # of attributes in cdm entity file at the end
          schema.zipWithIndex.map{ case (col, index) =>
            indexToCachedDateFunction = new Array[(Any, (String, Any, Int, String) => LocalDate)](schema.size)
            indexToCachedDateTimeFunction = new Array[(Any, (String, Any, Int, String) => java.lang.Long)](schema.size)
            if (index >= firstRow.length) {
              null
            } else {
              val dataType = schema.fields(index).dataType
              jsonToData(dataType, firstRow.apply(index), index, mode)
            }
          }
        } else {
          firstRow.zipWithIndex.map { case (col, index) =>
            indexToCachedDateFunction = new Array[(Any, (String, Any, Int, String) => LocalDate)](firstRow.length)
            indexToCachedDateTimeFunction = new Array[(Any, (String, Any, Int, String) => java.lang.Long)](firstRow.length)
            val dataType = schema.fields(index).dataType
            jsonToData(dataType, firstRow.apply(index), index, mode)
          }
        }
        tempParser.stopParsing()
        tempStream.close()
      }

    } catch {
      case e: Throwable => SparkCDMLogger.log(Level.ERROR, e.printStackTrace.toString, logger)
    }
  }

  def close(): Unit = {
    parser.stopParsing()
  }

  def readRow(): Array[Any] = {
    val arr = parser.parseNext()
    arr.asInstanceOf[Array[Any]]
  }

  /* if the conversion failed, value is null, return false to indicate that whole row should be null */
  def checkResult(ret: Any): (Any, Boolean) = {
    if (ret == null) {
      (ret, false)
    } else {
      (ret, true)
    }
  }

  def isValidRow(): Boolean = goodRow

  def jsonToData(dt: DataType, value: Any, schemaIndex: Int, mode: String): Any= {
    /* null is a valid value */
    if (value == null) {
      null
    } else {

      val result = dt match {
        case ByteType => util.Try(value.toString.toByte).getOrElse(null)
        case ShortType => util.Try(value.toString.toShort).getOrElse(null)
        case IntegerType => util.Try(value.toString.toInt).getOrElse(null)
        case LongType => util.Try(value.toString.toLong).getOrElse(null)
        case DoubleType => util.Try(value.toString.toDouble).getOrElse(null)
        case FloatType => util.Try(value.toString.toFloat).getOrElse(null)
        case DecimalType() =>  util.Try(Decimal(value.toString)).getOrElse(null)
        case BooleanType =>  util.Try(value.toString.toBoolean).getOrElse(null)
        case DateType => {
          if (value != None && value != null) {
            var date : LocalDate = null
            if (indexToCachedDateFunction(schemaIndex) != null) {
              val tuple = indexToCachedDateFunction(schemaIndex)
              val format = tuple._1
              val fx = tuple._2
              date = fx(value.toString, format, schemaIndex, mode)
            } else {
              date = tryParseDate(value.toString, schemaIndex, mode)
            }

            /* If we can't parse the date we return a null. This enables permissive mode to work*/
            if (date == null) {
              null
            } else {
              date.toEpochDay.toInt
            }
          } else {
            null
          }
        }
        case StringType => util.Try(UTF8String.fromString(value.toString)).getOrElse(null)
        case TimestampType => {
          if (value != None && value != null) {
            var date : java.lang.Long = null
            if (indexToCachedDateTimeFunction(schemaIndex) != null) {
              val tuple = indexToCachedDateTimeFunction(schemaIndex)
              val format = tuple._1
              val fx = tuple._2
              date = fx(value.toString, format, schemaIndex, mode)
            } else {
              date = tryParseDateTime(value.toString, schemaIndex, mode)
            }

            /* If we can't parse the date we return a null. This enables permissive mode to work*/
            if (date == null) {
              null
            } else {
              date
            }
          } else {
            null
          }
        }
        case _ => util.Try(UTF8String.fromString(value.toString)).getOrElse(null)
      }
      if (result == null) {
        val msg = "Mode: " + mode + ". Could not parse " + value + " as " + dt.simpleString + ", converting to null"
        SparkCDMLogger.log(Level.ERROR,  msg, logger)

        /*
         *If we want pure fail-fast, we should add below exception
         */
        /*
        if (Constants.FAILFAST.equalsIgnoreCase(mode)) {
          throw new IllegalArgumentException(msg)
        }*/
        goodRow = false
      }
      result
    }
  }

  /**
   * Attempts to parse Date using all possible formats.
   * Upon reaching the first successful parse, map the column index to a cached tuple (format, function).
   */
  def tryParseDate(dateString: String, index: Int, mode: String): LocalDate= {
    for (formatString <- dateFormatStrings.keySet) {
      try {
          val dateTimeFormatter = DateTimeFormatter.ofPattern(formatString)
          val localDate= LocalDate.parse(dateString, dateTimeFormatter)
          indexToCachedDateFunction(index) = (formatString, dateFormatStrings(formatString))
          return localDate
      } catch {
        case e: DateTimeParseException=>
      }
    }

    val msg = s"Mode: $mode. Could not parse \'$dateString\'. Data in this format is not supported."
    SparkCDMLogger.log(Level.ERROR,  msg, logger)
    if (Constants.FAILFAST.equalsIgnoreCase(mode)) {
      throw new IllegalArgumentException(msg)
    }
    null
  }

  /**
   * Attempts to parse DateTime using all possible formats.
   * Upon reaching the first successful parse, map the column index to a cached tuple (format, function).
   */
  def tryParseDateTime(dateString: String, index: Int, mode: String): java.lang.Long = {
    /* Conversions that to local time first */
    for (format <- localTimeFormats.keySet) {
      var instant: Instant = null;
      try {
        val i = Instant.from(format.parse(dateString))
        val zt = i.atZone(ZoneId.systemDefault())
        instant = zt.toLocalDateTime.atZone(ZoneId.systemDefault()).toInstant();
        val res = ChronoUnit.MICROS.between(Instant.EPOCH, instant)
        indexToCachedDateTimeFunction(index) = (format, localTimeFormats(format))
        return res
      } catch {
        case e: ArithmeticException => {
          indexToCachedDateTimeFunction(index) = (format, localTimeFormats(format))
          return instant.toEpochMilli()*1000
        }
        case e: DateTimeParseException=>
      }
    }

    /* Local Time formatting */
    for (format <- localTimeFormat2.keySet) {
      var instant: Instant = null
      try {
        val localDateTime = LocalDateTime.parse(dateString, format)
        instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
        val res = ChronoUnit.MICROS.between(Instant.EPOCH, instant)
        indexToCachedDateTimeFunction(index) = (format, localTimeFormat2(format))
        return res
      } catch {
        case e: ArithmeticException => {
          indexToCachedDateTimeFunction(index) = (format, localTimeFormat2(format))
          return instant.toEpochMilli()*1000
        }
        case e: DateTimeParseException =>
      }
    }

    /* Non-common formats in local time */
    for (formatString <- localTimeFormatsNonStandard.keySet) {
      var instant: Instant = null
      try {
        val dateTimeFormatter = DateTimeFormatter.ofPattern(formatString)
        val localDateTime = LocalDateTime.parse(dateString, dateTimeFormatter)
        /* Assume non-standard times are in UTC */
        instant =  localDateTime.atZone(ZoneId.of("UTC")).toInstant();
        val res = ChronoUnit.MICROS.between(Instant.EPOCH, instant)
        indexToCachedDateTimeFunction(index) = (formatString, localTimeFormatsNonStandard(formatString))
        return res
      } catch {
        case e: ArithmeticException => {
          indexToCachedDateTimeFunction(index) = (formatString, localTimeFormatsNonStandard(formatString))
          return instant.toEpochMilli()*1000
        }
        case e: DateTimeParseException =>
      }
    }

    /* Just Dates (no-time element) formats formatting */
    for (formatString <- dateFormatStringsAsDateTime.keySet) {
      var instant: Instant = null
      try {
        val dateTimeFormatter = DateTimeFormatter.ofPattern(formatString)
        val localDate = LocalDate.parse(dateString, dateTimeFormatter)
        val localDateTime1 = localDate.atStartOfDay();
        instant = localDateTime1.atZone(ZoneId.of("UTC")).toInstant();
        val res = ChronoUnit.MICROS.between(Instant.EPOCH, instant)
        indexToCachedDateTimeFunction(index) = (formatString, dateFormatStringsAsDateTime(formatString))
        return res
      } catch {
        case e: ArithmeticException => {
          indexToCachedDateTimeFunction(index) = (formatString, dateFormatStringsAsDateTime(formatString))
          return instant.toEpochMilli()*1000
        }
        case e: DateTimeParseException =>
      }
    }

    /* Finally, this could just be a Time - Try that */
    for (formatString <- timeFormatStrings.keySet) {
      var instant: Instant = null
      try {
        val formatterTime1 = DateTimeFormatter.ofPattern(formatString)
        val ls = LocalTime.parse(dateString, formatterTime1)
        instant = ls.atDate(LocalDate.of(1970, 1, 1)).atZone(ZoneId.of("UTC")).toInstant
        val res = ChronoUnit.MICROS.between(Instant.EPOCH, instant)
        indexToCachedDateTimeFunction(index) = (formatString, timeFormatStrings(formatString))
        return res
      } catch {
        case e: ArithmeticException => {
          indexToCachedDateTimeFunction(index) = (formatString, timeFormatStrings(formatString))
          return instant.toEpochMilli()*1000
        }
        case e: DateTimeParseException =>
      }
    }

    val msg = s"Mode: $mode. Could not parse \'$dateString\'. Data in this format is not supported."
    SparkCDMLogger.log(Level.ERROR,  msg, logger)
    if (Constants.FAILFAST.equalsIgnoreCase(mode)) {
      throw new IllegalArgumentException(msg)
    }
    null
  }

  // DATE PARSING
  private def parseGenericDateCached1(dateString: String, format: Any, index: Int, mode: String) : LocalDate = {
    try {
      val dateTimeFormatter = DateTimeFormatter.ofPattern(format.asInstanceOf[String])
      return LocalDate.parse(dateString, dateTimeFormatter)
    } catch {
      case e: DateTimeParseException=>
    }
    return checkFailFast(dateString, format, index, mode)
  }

  // DATETIME PARSING
  private def parseGenericDateTimeCached1(dateString: String, format: Any, index: Int, mode: String) : java.lang.Long = {
    var instant: Instant = null;
    try {
      val i = Instant.from(format.asInstanceOf[DateTimeFormatter].parse(dateString))
      val zt = i.atZone(ZoneId.systemDefault())
      instant = zt.toLocalDateTime.atZone(ZoneId.systemDefault()).toInstant();
      return ChronoUnit.MICROS.between(Instant.EPOCH, instant)
    } catch {
      case e: ArithmeticException => { return instant.toEpochMilli()*1000 }
      case e: DateTimeParseException=>
    }
    return checkFailFast(dateString, format, index, mode)
  }

  private def parseGenericDateTimeCached2(dateString: String, format: Any, index: Int, mode: String) : java.lang.Long = {
    var instant: Instant = null
    try {
      val localDateTime = LocalDateTime.parse(dateString, format.asInstanceOf[DateTimeFormatter])
      instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
      return ChronoUnit.MICROS.between(Instant.EPOCH, instant)
    } catch {
      case e: ArithmeticException => { return instant.toEpochMilli()*1000 }
      case e: DateTimeParseException =>
    }
    return checkFailFast(dateString, format, index, mode)
  }

  private def parseGenericDateTimeCached3(dateString: String, format: Any, index: Int, mode: String) : java.lang.Long = {
    var instant: Instant = null
    try {
      val dateTimeFormatter = DateTimeFormatter.ofPattern(format.asInstanceOf[String])
      val localDateTime = LocalDateTime.parse(dateString, dateTimeFormatter)
      /* Assume non-standard times are in UTC */
      instant =  localDateTime.atZone(ZoneId.of("UTC")).toInstant();
      return ChronoUnit.MICROS.between(Instant.EPOCH, instant)
    } catch {
      case e: ArithmeticException => { return instant.toEpochMilli()*1000 }
      case e: DateTimeParseException =>
    }
    return checkFailFast(dateString, format, index, mode)
  }

  private def parseGenericDateTimeCached4(dateString: String, format: Any, index: Int, mode: String) : java.lang.Long = {
    var instant: Instant = null
    try {
      val dateTimeFormatter = DateTimeFormatter.ofPattern(format.asInstanceOf[String])
      val localDate = LocalDate.parse(dateString, dateTimeFormatter)
      val localDateTime1 = localDate.atStartOfDay();
      instant = localDateTime1.atZone(ZoneId.of("UTC")).toInstant();
      return ChronoUnit.MICROS.between(Instant.EPOCH, instant)
    } catch {
      case e: ArithmeticException => { return instant.toEpochMilli()*1000 }
      case e: DateTimeParseException =>
    }
    return checkFailFast(dateString, format, index, mode)
  }

  private def parseGenericDateTimeCached5(dateString: String, format: Any, index: Int, mode: String) : java.lang.Long = {
    var instant: Instant = null
    try {
      val formatterTime1 = DateTimeFormatter.ofPattern(format.asInstanceOf[String])
      val ls = LocalTime.parse(dateString, formatterTime1)
      instant = ls.atDate(LocalDate.of(1970, 1, 1)).atZone(ZoneId.of("UTC")).toInstant
      return ChronoUnit.MICROS.between(Instant.EPOCH, instant)
    } catch {
      case e: ArithmeticException => { return instant.toEpochMilli()*1000 }
      case e: DateTimeParseException =>
    }
    return checkFailFast(dateString, format, index, mode)
  }

  private def checkFailFast(dateString: String, format: Any, index: Int, mode: String) : Null = {
    val msg = s"Mode: $mode. Each item in a column must have the same format. Column $index, item \'$dateString\' does not match the cached format \'$format\'."
    SparkCDMLogger.log(Level.ERROR, msg, logger)
    if (Constants.FAILFAST.equalsIgnoreCase(mode)) {
      throw new IllegalArgumentException(msg)
    }
    return null
  }
}
