package com.microsoft.cdm.read

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream}
import java.math.BigInteger
import java.text.SimpleDateFormat
import java.time.temporal.{ChronoField}
import java.time.{Instant, LocalDate, ZoneId}
import java.util.Base64
import java.nio.charset.StandardCharsets.UTF_8
import com.microsoft.cdm.utils.{Constants, SparkSerializableConfiguration}
import com.microsoft.cdm.log.SparkCDMLogger
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.{NanoTime, SimpleGroup}
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO, RecordReader}
import org.apache.parquet.schema.{MessageType, OriginalType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils}
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory
import org.slf4j.event.Level


class ParquetReaderConnector(httpPrefix: String,
                             filePath: String,
                             sparkSchema: StructType,
                             serializedHadoopConf:SparkSerializableConfiguration) extends ReaderConnector  {

  /*
   * Note. If these variables are initialized in the constructor, we get the "objects are not serializable"
   * for all of the classes below. Therefore, we need to wait to initialize the class until it is on the worker.
   */
  var i = 0
  var rows = 0
  var pages: PageReadStore = _
  var schema: MessageType = _
  var reader: ParquetFileReader = _
  var recordReader: RecordReader[Group] = _
  var columnIO: MessageColumnIO = _
  var path:Path = _
  var thisSparkSchema: StructType = _

  val logger  = LoggerFactory.getLogger(classOf[ParquetReaderConnector])
  SparkCDMLogger.log(Level.DEBUG, "Parquet Reader for partition at path: " + httpPrefix + filePath, logger)

  def build() {
    try {
      val path = new Path(filePath)
      val readFooter= ParquetFileReader.readFooter(serializedHadoopConf.value, path)
      schema = readFooter.getFileMetaData.getSchema
      columnIO = new ColumnIOFactory().getColumnIO(schema);
      reader = new ParquetFileReader(serializedHadoopConf.value, path, readFooter)
      //does this have to be in a loop?
      pages = reader.readNextRowGroup()
      if(pages != null) {
        rows = pages.getRowCount().toInt;
        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      }
      this.thisSparkSchema = sparkSchema
    } catch {
      case e: Throwable => SparkCDMLogger.log(Level.ERROR, e.printStackTrace.toString, logger)
    }
  }

  def close (): Unit = {
    reader.close()
  }

  def readRow(): Array[Any] = {
    if (i < rows) {
      i += 1
      getRowAsString(recordReader.read(), thisSparkSchema)
    } else {
      pages = reader.readNextRowGroup()
      if (pages == null) {
        //No more
        null
      } else {
        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
        assert(i<rows)
        i = 0
        getRowAsString(recordReader.read(), thisSparkSchema)
      }
    }
  }

  def getNumberOfDaysFromEpoch(value: String) : Any = {
    if (value != None && value != null) {
      val date = dateFormatter.parse(value);
      val localDate = Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
      val days = localDate.getLong(ChronoField.EPOCH_DAY)
      days.asInstanceOf[Int]
    } else {
      null
    }
  }

  def getTimeStamp(sg: SimpleGroup, field:Int, index: Int): Long= {
    val bin = sg.getInt96(field,index)
    val nanoTime = NanoTime.fromBinary(bin)
    val dateTimeUtil = DateTimeUtils.fromJulianDay(nanoTime.getJulianDay, nanoTime.getTimeOfDayNanos)
    dateTimeUtil
    //val dateTimeUtilAsString = DateTimeUtils.timestampToString(dateTimeUtil)
    //dateTimeUtilAsString
  }


  def getDateAsString(sg: SimpleGroup, field: Int, index: Int): String = {
    val localDate =  LocalDate.ofEpochDay(sg.getInteger(field, index))
    val formatter = new SimpleDateFormat(Constants.SINGLE_DATE_FORMAT)
    val date = java.sql.Date.valueOf(localDate)
    val strDate = formatter.format(date)
    strDate
  }

  def serializeObject(obj: Object) = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(obj)
    oos.close
    new String(Base64.getEncoder().encode(stream.toByteArray), UTF_8);
  }

  def deSerializeObject(arr: Array[Byte]): Array[Any] = {
    val bytes = Base64.getDecoder().decode(arr)
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val obj = ois.readObject;
    val strArr = obj.asInstanceOf[Array[Any]]
    ois.close
    strArr
  }

  def getRowAsString(g: Group, struct: StructType): Array[Any] = {
    val fieldCount = g.getType().getFieldCount()
    val arr = new Array[Any](fieldCount)
    for (field <- 0 until fieldCount) {
      val valueCount = g.getFieldRepetitionCount(field);

      val fieldType = g.getType().getType(field);
      val fieldName = fieldType.getName();
      for (index <- 0 until valueCount) {
        if (fieldType.isPrimitive()) {
          if (g.isInstanceOf[SimpleGroup]) {
            val sg = g.asInstanceOf[SimpleGroup]
            val ptype = fieldType.asPrimitiveType().getPrimitiveTypeName
            if (ptype == PrimitiveTypeName.INT96) {
              arr(field) = getTimeStamp(sg, field, index)
            } else if (ptype == PrimitiveTypeName.INT32 && fieldType.getOriginalType == OriginalType.DATE) {
              arr(field) = getDateAsString(sg, field, index)
            } else if (fieldType.getOriginalType == OriginalType.DECIMAL) {
              val sparkDecimal = struct.fields(field).dataType.asInstanceOf[DecimalType]
              val parquetDecimal = fieldType.asPrimitiveType().getDecimalMetadata
              if(parquetDecimal.getPrecision != sparkDecimal.precision && parquetDecimal.getScale != sparkDecimal.scale) {
                throw new Exception("Parquet decimal metadata don't match the CDM decimal arguments for field \"" + fieldName +"\". Found Parquet decimal ("+ parquetDecimal.getPrecision +", " + parquetDecimal.getScale+")")
              }
              fieldType.asPrimitiveType().getPrimitiveTypeName match {
                case PrimitiveTypeName.INT32 =>
                  arr(field) = Decimal.apply(g.getInteger(field, index), parquetDecimal.getPrecision, parquetDecimal.getScale).toString()
                case PrimitiveTypeName.INT64 =>
                  arr(field) = Decimal.apply(g.getLong(field, index), parquetDecimal.getPrecision, parquetDecimal.getScale).toString()
                case PrimitiveTypeName.BINARY =>
                  arr(field) = new java.math.BigDecimal(new BigInteger(g.getBinary(field, index).getBytes), parquetDecimal.getScale).toString
                case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
                  arr(field) = new java.math.BigDecimal(new BigInteger(g.getBinary(field, index).getBytes), parquetDecimal.getScale).toString
              }
            } else if (fieldType.getOriginalType == OriginalType.TIME_MICROS
              && ptype == PrimitiveTypeName.INT64) {
              arr(field) = g.getLong(field, index)
            } else {
              arr(field) = g.getValueToString(field, index)
            }
          }
        }else{
            val listgroup = g.asInstanceOf[SimpleGroup]
            if (fieldType.getOriginalType == OriginalType.LIST) {
              val elementGroup = listgroup.getGroup(field, index)
              /* get how many structs are present in the array */
              val repeatedListSize = elementGroup.getFieldRepetitionCount("list")
              val serializedCombinedStruct = new StringBuilder
              for(i <- 0 until repeatedListSize) {
                /* check if its an empty array*/
                if (elementGroup.getGroup(index, i).toString != "") {
                  val rowAsVal = getRowAsString(elementGroup.getGroup(index, i).getGroup(0, 0), struct.fields(field).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
                  serializedCombinedStruct.append(serializeObject(rowAsVal))
                  serializedCombinedStruct.append(" ")
                }
              }
              if ( serializedCombinedStruct.length > 0) serializedCombinedStruct.setLength(serializedCombinedStruct.length -1)
              arr(field) = serializedCombinedStruct.toString()
            } else {
              val rowAsVal = getRowAsString(listgroup.getGroup(field, index), struct.fields(field).dataType.asInstanceOf[StructType]);
              /* Serializing is necessary because we want to encode the rowAsVal object as a single string.
                rowAsVal is an array of string */
              arr(field) = serializeObject(rowAsVal)
            }
        }
      }
    }
    arr
  }

  def isValidRow(): Boolean = true

  def jsonToData(dt: DataType, value: Any, schemaIndex: Int, mode: String): Any = {
    return dt match {
      case ar: ArrayType => {
        util.Try({
          val structs = value.toString.split(" ")
          val seq = structs.zipWithIndex.map{ case (col, index) =>
            val dataType = ar.elementType
            jsonToData(dataType, col, schemaIndex, mode)
          }
          ArrayData.toArrayData(seq)
        }).getOrElse(null)
      }
      case BooleanType => util.Try(value.toString.toBoolean).getOrElse(null)
      case ByteType => util.Try(value.toString.toByte).getOrElse(null)
      case ShortType => util.Try(value.toString.toShort).getOrElse(null)
      case DateType => util.Try(getNumberOfDaysFromEpoch(value.toString)).getOrElse(null)
      case DecimalType() => util.Try(Decimal(value.toString)).getOrElse(null)
      case DoubleType => util.Try(value.toString.toDouble).getOrElse(null)
      case FloatType => util.Try(value.toString.toFloat).getOrElse(null)
      case IntegerType => util.Try(value.toString.toInt).getOrElse(null)
      case LongType => util.Try(value.toString.toLong).getOrElse(null)
      case StringType => util.Try(UTF8String.fromString(value.toString)).getOrElse(null)
      case TimestampType => {
        if (value != None && value != null) {
          return value.asInstanceOf[Long]
        } else {
          null
        }
      }
      case st: StructType => {
        util.Try({
          /* we decode the binary string to an array of string containing nested values */
          val arr = deSerializeObject(value.toString.getBytes());
          val seq = arr.zipWithIndex.map { case (col, index) =>
            val dataType = st.fields(index).dataType
            jsonToData(dataType, col, schemaIndex, mode)
          }
          val isAllNull = arr.forall(x => x == null)
          if (isAllNull) null else InternalRow.fromSeq(seq)
        }).getOrElse(null)
      }
      case _ => util.Try(UTF8String.fromString(value.toString)).getOrElse(null)
    }
  }
}
