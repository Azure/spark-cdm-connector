package com.microsoft.cdm.read

import java.net.URLDecoder
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.ChronoUnit
import com.microsoft.cdm.utils.{Constants, CsvParserFactory, SparkSerializableConfiguration}
import com.microsoft.cdm.log.SparkCDMLogger
import com.univocity.parsers.csv.CsvParser
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

class CSVReaderConnector(httpPrefix:String, filePath: String, serConf:SparkSerializableConfiguration, delimiter: Char, mode: String) extends ReaderConnector {
  val logger  = LoggerFactory.getLogger(classOf[CSVReaderConnector])
  SparkCDMLogger.log(Level.DEBUG, "CSV Reader for partition at path: " + httpPrefix + filePath, logger)

  private var goodRow:Boolean = true
  private var parser: CsvParser = _
  private val dateFormatStrings = List(
    "yyyy-MM-dd",
    "M/d/yyyy" )

  private val localTimeFormatsNonStandard= List(
    "M/d/yyyy H:mm",
    "M/d/yyyy h:mm:ss a",
    "M/d/yyyy H:mm:ss",
    "yyyy-MM-dd H:mm:ss.S",
    "yyyy-MM-dd H:mm:ss.SS",
    "yyyy-MM-dd H:mm:ss.SSS",
    "yyyy-MM-dd H:mm:ss.SSSS",
    "yyyy-MM-dd H:mm:ss.SSSSS",
    "yyyy-MM-dd H:mm:ss.SSSSSS",
    "yyyy-MM-dd H:mm:ss",
    "MMM d yyyy h:mma")

  private val timeFormatStrings = List(
    "HH:mm:ss",
    "HH:mm:ss.S",
    "HH:mm:ss.SS",
    "HH:mm:ss.SSS",
    "HH:mm:ss.SSSS",
    "HH:mm:ss.SSSSS",
    "HH:mm:ss.SSSSSS")

  def build: Unit = {
    try {
      val path = new Path(filePath)
      val inputFile = HadoopInputFile.fromPath(path, serConf.value)
      val inputStream = inputFile.newStream()
      parser = CsvParserFactory.build(delimiter)
      parser.beginParsing {
        inputStream
      }
    } catch {
      case e: Throwable => SparkCDMLogger.log(Level.ERROR, e.printStackTrace.toString, logger)
    }
  }

  def close(): Unit = {
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

  def jsonToData(dt: DataType, value: Any, mode: String): Any= {
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
            val date = tryParseDate(value.toString, mode)

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
            val date = tryParseDateTime(value.toString, mode)

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

  def tryParseDate(dateString: String, mode: String): LocalDate= {
    for (formatString <- dateFormatStrings) {
      try {
          val dateTimeFormatter = DateTimeFormatter.ofPattern(formatString)
          val localDate= LocalDate.parse(dateString, dateTimeFormatter)
          return localDate
      } catch {
        case e: DateTimeParseException=>
      }
    }

    val msg = "Mode: " + mode + ". Could not parse " + dateString + " using any possible format"
    SparkCDMLogger.log(Level.ERROR,  msg, logger)
    if (Constants.FAILFAST.equalsIgnoreCase(mode)) {
      throw new IllegalArgumentException(msg)
    }
    null
  }

  def tryParseDateTime(dateString: String, mode: String): java.lang.Long = {

    val localTimeFormats = List(DateTimeFormatter.ISO_OFFSET_DATE_TIME,
      DateTimeFormatter.ISO_INSTANT)

    /* Conversions that to local time first */
    for (format <- localTimeFormats) {
      var instant: Instant = null;
      try {
        val i = Instant.from(format.parse(dateString))
        val zt = i.atZone(ZoneId.systemDefault())
        instant = zt.toLocalDateTime.atZone(ZoneId.systemDefault()).toInstant();
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant)
      } catch {
        case e: ArithmeticException => {
          return instant.toEpochMilli()*1000
        }
        case e: DateTimeParseException=>
      }
    }

    /* Local Time formatting */
    for (format <- List(DateTimeFormatter.ISO_LOCAL_DATE_TIME)) {
      var instant: Instant = null
      try {
        val localDateTime = LocalDateTime.parse(dateString, format)
        instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant)
      } catch {
        case e: ArithmeticException => {
          return instant.toEpochMilli()*1000
        }
        case e: DateTimeParseException =>
      }
    }

    /* Non-common formats in local time */
    for (formatString <- localTimeFormatsNonStandard) {
      var instant: Instant = null
      try {
        val dateTimeFormatter = DateTimeFormatter.ofPattern(formatString)
        val localDateTime = LocalDateTime.parse(dateString, dateTimeFormatter)
        /* Assume non-standard times are in UTC */
        instant =  localDateTime.atZone(ZoneId.of("UTC")).toInstant();
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant)
      } catch {
        case e: ArithmeticException => {
          return instant.toEpochMilli()*1000
        }
        case e: DateTimeParseException =>
      }
    }

    /* Just Dates (no-time element) formats formatting */
    for (formatString <- dateFormatStrings) {
      var instant: Instant = null
      try {
        val dateTimeFormatter = DateTimeFormatter.ofPattern(formatString)
        val localDate = LocalDate.parse(dateString, dateTimeFormatter)
        val localDateTime1 = localDate.atStartOfDay();
        instant = localDateTime1.atZone(ZoneId.of("UTC")).toInstant();
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant)
      } catch {
        case e: ArithmeticException => {
          return instant.toEpochMilli()*1000
        }
        case e: DateTimeParseException =>
      }
    }

    /* Finally, this could just be a Time - Try that */
    for (formatString <- timeFormatStrings) {
      var instant: Instant = null
      try {
        val formatterTime1 = DateTimeFormatter.ofPattern(formatString)
        val ls = LocalTime.parse(dateString, formatterTime1)
        instant = ls.atDate(LocalDate.of(1970, 1, 1)).atZone(ZoneId.of("UTC")).toInstant
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant)
      } catch {
        case e: ArithmeticException => {
          return instant.toEpochMilli()*1000
        }
        case e: DateTimeParseException =>
      }
    }


    val msg = "Mode: " + mode + ". Could not parse " + dateString + " using any possible format"
    SparkCDMLogger.log(Level.ERROR,  msg, logger)
    if (Constants.FAILFAST.equalsIgnoreCase(mode)) {
      throw new IllegalArgumentException(msg)
    }
    null
  }
}
