package com.microsoft.cdm.utils

import java.text.SimpleDateFormat
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.time.{Instant, LocalDate, ZoneId}
import java.util.TimeZone
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * Converts between CSV/CDM data and Spark data tpyes.
 */
@SerialVersionUID(100L)
class DataConverter extends Serializable{

  val logger  = LoggerFactory.getLogger(classOf[DataConverter])
  val dateFormatter = new SimpleDateFormat(Constants.SINGLE_DATE_FORMAT)


  def toSparkType(dt: CDMDataFormat.Value, precision: Int, scale: Int) = {
    val map = Map(
      CDMDataFormat.Byte -> ByteType,
      CDMDataFormat.Int16 -> ShortType,
      CDMDataFormat.Int32 -> IntegerType,
      CDMDataFormat.Int64 -> LongType,
      CDMDataFormat.Date -> DateType,
      CDMDataFormat.DateTime -> TimestampType,
      CDMDataFormat.String -> StringType,
      CDMDataFormat.Double -> DoubleType,
      CDMDataFormat.Decimal -> DecimalType(precision, scale),
      CDMDataFormat.Boolean -> BooleanType,
      CDMDataFormat.DateTimeOffset -> TimestampType,
      CDMDataFormat.Guid -> StringType, //There is no UuidType in Spark
      CDMDataFormat.Time -> TimestampType,
      CDMDataFormat.Float -> FloatType
    )
    map(dt)
  }

  def toParquet(dataType: DataType, data: Any): Any= {
    (dataType, data) match {
      case (_, null) => null
      case (IntegerType, _) => data.asInstanceOf[Int]
      case (StringType, _) => data.asInstanceOf[String]
      case _ => data.toString
    }
  }

  def toCdmDataFormat(dt: DataType): CDMDataFormat.Value = {
    return dt match {
      case ByteType => CDMDataFormat.Byte
      case ShortType => CDMDataFormat.Int16
      case IntegerType => CDMDataFormat.Int32
      case LongType => CDMDataFormat.Int64
      case DateType => CDMDataFormat.Date
      case StringType => CDMDataFormat.String
      case DoubleType => CDMDataFormat.Double
      case DecimalType() => CDMDataFormat.Decimal
      case BooleanType => CDMDataFormat.Boolean
      case TimestampType => CDMDataFormat.DateTime
      case structType: StructType => CDMDataFormat.entity
      case FloatType => CDMDataFormat.Float
    }
  }
  def toCdmDataFormatOverride(dt: String): CDMDataFormat.Value = {
    return dt match {
      case Constants.MD_DATATYPE_OVERRIDE_TIME=> CDMDataFormat.Time
    }
  }

  def toCdmDataType(dt: DataType): CDMDataType.Value = {
    return dt match {
      case ByteType => CDMDataType.byte
      case ShortType => CDMDataType.smallInteger
      case IntegerType => CDMDataType.integer
      case LongType => CDMDataType.bigInteger
      case DateType => CDMDataType.date
      case StringType => CDMDataType.string
      case DoubleType => CDMDataType.double
      case DecimalType() => CDMDataType.decimal
      case BooleanType => CDMDataType.boolean
      case TimestampType => CDMDataType.dateTime
      case structType: StructType => CDMDataType.entity
      case FloatType => CDMDataType.float
    }
  }

  def toCdmDataTypeOverride(dt: String): CDMDataType.Value = {
    return dt match {
      case Constants.MD_DATATYPE_OVERRIDE_TIME => CDMDataType.time
    }
  }

  def dataToString(data: Any, dataType: DataType, cdmType:Any): String = {
    (dataType, data, cdmType) match {
      case (_, null, _) => null
      case (DateType, v: Number, _) => {
        LocalDate.ofEpochDay(v.intValue()).toString
      }
      case (TimestampType, v: Number, "DateTimeOffset") => {
        val nanoAdjustment  = TimeUnit.MICROSECONDS.toNanos(Math.floorMod(v.asInstanceOf[Long], TimeUnit.SECONDS.toMicros(1)))
        val instant = Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(v.asInstanceOf[Long]), nanoAdjustment);
        val date = instant.atZone(ZoneId.systemDefault())
        /*
         * Using this format rather than ISO_OFFSET_DATE_TIME forces the format
         * to use +00:00 when the local time is UTC
         */
        val formatter = new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
          .appendOffset("+HH:MM","+00:00").toFormatter // min 0 max 6
        date.format(formatter)
      }
      case (TimestampType, v: Number, "DateTime") => {
        val nanoAdjustment  = TimeUnit.MICROSECONDS.toNanos(Math.floorMod(v.asInstanceOf[Long], TimeUnit.SECONDS.toMicros(1)))
        val instant = Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(v.asInstanceOf[Long]), nanoAdjustment);
        val date = instant.atZone(ZoneId.systemDefault())
        date.format(DateTimeFormatter.ISO_INSTANT)
      }
      case (TimestampType, v: Long, "Time") => {
        val nanoAdjustment  = TimeUnit.MICROSECONDS.toNanos(Math.floorMod(v.asInstanceOf[Long], TimeUnit.SECONDS.toMicros(1)))
        val instant = Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(v.asInstanceOf[Long]), nanoAdjustment);
        val localTime= instant.atZone(ZoneId.of("UTC")).toLocalTime
        localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")).toString
      }
      case _ => {
        data.toString
      }
    }
  }

}

