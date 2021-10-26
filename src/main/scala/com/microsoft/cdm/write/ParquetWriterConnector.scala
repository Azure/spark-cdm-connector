package com.microsoft.cdm.write

import java.nio.{ByteBuffer, ByteOrder}
import java.time.{Instant, ZoneId}
import java.util.TimeZone

import com.microsoft.cdm.utils.{CDMSparkToParquetSchemaConverter, DataConverter, Messages, SparkSerializableConfiguration}
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.{NanoTime, SimpleGroup, SimpleGroupFactory}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{MessageType, Type}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

import com.microsoft.cdm.log.SparkCDMLogger
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REQUIRED}
import org.slf4j.event.Level

import scala.collection.JavaConverters._


class ParquetWriterConnector(httpPrefix:String,
                             filePath: String,
                             cdmSchema: List[Any],
                             compression: CompressionCodecName,
                             var seriazliedHadoopConf: SparkSerializableConfiguration) extends WriterConnector {
  val logger  = LoggerFactory.getLogger(classOf[ParquetWriterConnector])
  private var structType: StructType= _
  private var schema: MessageType = _
  private var writer: ParquetWriter[Group]= _
  private var groupFactory:SimpleGroupFactory = _
  private var path:Path=_
  private val httpPath = httpPrefix + filePath
  def getPath: String = httpPath
  private var converter: CDMSparkToParquetSchemaConverter=_

  val NANOS_PER_HOUR: Long = TimeUnit.HOURS.toNanos (1)
  val NANOS_PER_MINUTE: Long = TimeUnit.MINUTES.toNanos (1)
  val NANOS_PER_SECOND: Long = TimeUnit.SECONDS.toNanos (1)
  val NANOS_PER_MILLISECOND: Long = TimeUnit.MILLISECONDS.toNanos (1)

  SparkCDMLogger.log(Level.INFO, "Parquet Writer for partition at path: " + httpPrefix + filePath, logger)

  def build(inStructType: StructType): Unit = {
    try {
      /*
       * CCDMSparkToParquetSchemaConverter is a modified version of SparkToParquetSchemaConverter.
       * Since Spark does not support the TIME type, We use this to tell Parquet that the field type should be TIME.
       * We do this ine one of two ways:
       * * Set a metadata overwrite field set to Time on implicit write
       * * the CDM type is of type Time
       * In either case, we will set the column to be of type TIME and not type Timestamp.
       * If Spark supported a Time type, we would not need to do this. However, they do not. See:
       * https://github.com/apache/spark/pull/25678
       */
      converter = new CDMSparkToParquetSchemaConverter(writeLegacyParquetFormat = false)
      structType = inStructType
      schema = converter.convert(inStructType, cdmSchema)
      groupFactory = new SimpleGroupFactory(schema)
      GroupWriteSupport.setSchema(schema, seriazliedHadoopConf.value)
      val writeSupport = new GroupWriteSupport()
      path = new Path(filePath)
      writer = new ParquetWriter[Group](
        path,
        writeSupport,
        compression,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE,
        ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
        ParquetProperties.DEFAULT_WRITER_VERSION,
        seriazliedHadoopConf.value
      )
    }
    catch {
      case e : Exception => {
        SparkCDMLogger.log(Level.ERROR, e.printStackTrace.toString, logger)
      }
    }
  }

  def upload() = {
    writer.close()
  }


  def parseDateToBinary(value: Long) = {
    val (julianDay, timeOfDayNanos) = DateTimeUtils.toJulianDay(value.toLong)

    // Write INT96 timestamp
    val timestampBuffer = new Array[Byte](12)
    val buf = ByteBuffer.wrap(timestampBuffer)
    buf.order(ByteOrder.LITTLE_ENDIAN).putLong(timeOfDayNanos).putInt(julianDay)

    // This is the properly encoded INT96 timestamp
    val tsValue = Binary.fromReusedByteArray(timestampBuffer);
    tsValue;
  }

  /**
   * Converts Decimal to FIX_LEN_BYTE_ARRAY of len @param typeLength
   * @param decimal
   * @param typeLength
   * @return
   */
  def decimaltoBytes(decimal: Decimal, typeLength: Int): Array[Byte] = {
    val bigDecimal = decimal.toJavaBigDecimal
    val bytes = new Array[Byte](typeLength)
    val fillByte: Byte = if (bigDecimal.signum < 0) 0xFF.toByte else 0x00.toByte
    val unscaled: Array[Byte] = bigDecimal.unscaledValue.toByteArray

    // If unscaled.length > typeLength. It means we cant that accomodate it in `bytes` array. We have FIX_LEN_BYTE_ARRAY of size = tyeLength
    if (unscaled.length > typeLength) {
      throw new UnsupportedOperationException("Decimal size greater than "+ typeLength+" bytes")
    }
    // Fill the all bytes with fillByte or unscaled
    val offset = typeLength - unscaled.length
    for( i <- 0 until bytes.length)
    {
      if(i<offset) bytes(i) = fillByte else bytes(i) = unscaled(i-offset)
    }
    bytes
  }

  def writeDecimal(group: Group, index: Int, decimal: Decimal) = {
    val primitive = group.getType.getType(index).asPrimitiveType()
    primitive.getPrimitiveTypeName match {
      case PrimitiveTypeName.INT32 =>  group.add(index, decimal.toUnscaledLong.asInstanceOf[Int])
      case PrimitiveTypeName.INT64 => group.add(index, decimal.toUnscaledLong)
      case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => {
        val typeLength = primitive.getTypeLength
        val byteArray = decimaltoBytes(decimal, typeLength)
        group.add(index,Binary.fromReusedByteArray(byteArray))
      }
      case PrimitiveTypeName.BINARY => {
        val typeLength = primitive.getTypeLength
        val byteArray = decimaltoBytes(decimal, typeLength)
        group.add(index,Binary.fromReusedByteArray(byteArray))
      }
      case _ => throw new UnsupportedOperationException("Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
    }
  }

  def writeRowUtil(row: InternalRow, group: Group, structType: StructType, cdmSchemaLocal:List[Any]): Unit = {
    if(row != null) {
      row.toSeq(structType).zipWithIndex.foreach {
        case (field, index) => {
          (structType.fields(index).dataType, field) match {
            case (_, null) => {
              // If precision <= scale, spark store null value; we dont want that, so controlling it here..
              if (structType.fields(index).dataType.isInstanceOf[DecimalType]) {
                val primitive = group.getType.getType(index).asPrimitiveType()
                val precision = primitive.getDecimalMetadata.getPrecision
                val scale = primitive.getDecimalMetadata.getScale
                if (precision <= scale) {
                  throw new IllegalArgumentException(String.format(Messages.invalidDecimalFormat, new Integer(precision), new Integer(scale)))
                }
              }
            }
            case (ByteType, _) => group.add(index, field.asInstanceOf[Byte])
            case (ShortType, _) => group.add(index, field.asInstanceOf[Short])
            case (ar: ArrayType, _) => {
              val arrayData = field.asInstanceOf[ArrayData]
              val arrayElementType = structType.fields(index).dataType.asInstanceOf[ArrayType].elementType
              /* Convert Spark type to parquet type schema. Here Spark ArrayType gets converted to Parquet GroupType
                 This is the converted parquetType schema  :
                  `optional group field.name (LIST) {
                    repeated group list {
                      optional group element {
                        optional <structfieldname1type> <structfieldname1> ;
                        optional <structfieldname2type> <structfieldname2>;
                      }
                    }
                  }` */
              val parquetType = converter.convertField(structType.fields(index), Type.Repetition.REPEATED, cdmSchemaLocal(index))

              /* Create a Row group from the converted schema above to insert data */
              val mainGroup = new SimpleGroup(parquetType.asGroupType())

              /* `repeated group list` is represented as 0th index in mainGroup
              Adding that as group here because later we will insert StructType data inside this group.
              Refer: https://github.com/apache/parquet-mr/blob/b2d366a83f293914195f9de86d918f8ddd944374/parquet-column/src/main/java/org/apache/parquet/example/data/simple/SimpleGroup.java#L80 */
              mainGroup.addGroup(0)

              /* parquetType will always have field names "list". Get this group to add the array of objects
                 This is repeatedGroup structure
                 repeated group list {
                    optional group element {
                      optional <structfieldname1type> <structfieldname1> ;
                      optional <structfieldname2type> <structfieldname2>;
                    }
                  }  */
              val repeatedGroup = mainGroup.getGroup("list", 0)
              val subGroupFactory = new SimpleGroupFactory(converter.convert(arrayElementType.asInstanceOf[StructType], cdmSchemaLocal(index).asInstanceOf[List[Any]]))
              val iterator = arrayData.toObjectArray(arrayElementType).iterator

              /* Iterate through the `arrayData` */
              while (iterator.hasNext) {
                val subgroup = subGroupFactory.newGroup();
                val itemType = iterator.next().asInstanceOf[InternalRow]
                writeRowUtil(itemType, subgroup, arrayElementType.asInstanceOf[StructType],cdmSchemaLocal(index).asInstanceOf[List[Any]]);
                repeatedGroup.add(0, subgroup)
              }
              group.add(index, mainGroup);
            }
            case (BooleanType, _) => group.add(index, field.asInstanceOf[Boolean])
            case (DateType, _) => group.add(index, field.asInstanceOf[Integer])
            case (DoubleType, _) => group.add(index, field.asInstanceOf[Double])
            case (DecimalType(), _) => {
              val decimal = field.asInstanceOf[Decimal]
              writeDecimal(group, index, decimal)
            }
            case (FloatType, _) => group.add(index, field.asInstanceOf[Float])
            case (IntegerType, _) => group.add(index, field.asInstanceOf[Int])
            case (LongType, _) => group.add(index, field.asInstanceOf[Long])
            case (StringType, _) => {
              val string = field.asInstanceOf[UTF8String].toString
              group.add(index, string)
            }
            case (TimestampType, _) => {
              if (cdmSchemaLocal(index).equals("Time")) {
                val value = field.asInstanceOf[Long];
                val nanoAdjustment  = TimeUnit.MICROSECONDS.toNanos(Math.floorMod(value, TimeUnit.SECONDS.toMicros(1)))
                val instant = Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(value.asInstanceOf[Long]), nanoAdjustment);
                val localTime= instant.atZone(ZoneId.of("UTC")).toLocalTime
                group.add(index, localTime.toNanoOfDay / 1000)
              } else {
                val value = field.asInstanceOf[Long];
                val binary = parseDateToBinary(value)
                group.add(index, NanoTime.fromBinary(binary))
              }
            }
            case _ => {
              if (structType.fields(index).dataType.isInstanceOf[StructType]) {
                val subSchema = structType.fields(index).dataType.asInstanceOf[StructType];
                val subGroupFactory = new SimpleGroupFactory(converter.convert(subSchema, cdmSchemaLocal(index).asInstanceOf[List[Any]]))
                val subgroup = subGroupFactory.newGroup();
                writeRowUtil(field.asInstanceOf[InternalRow], subgroup, structType.fields(index).dataType.asInstanceOf[StructType], cdmSchemaLocal(index).asInstanceOf[List[Any]])
                group.add(index, subgroup);
              } else {
                group.add(index, field.toString)
              }
            }
          }
        }
      }
    }
  }

  def writeRow(row: InternalRow, dataConverter: DataConverter) {
    val group = groupFactory.newGroup()
    writeRowUtil(row, group, structType, cdmSchema);
    writer.write(group)
  }


  def abort(): Unit = {
    SparkCDMLogger.log(Level.ERROR, "ParquetWriter aborting.." + httpPrefix + filePath, logger)
    writer.close()
  }

  def close (): Unit = {
    writer.close()
  }
}
