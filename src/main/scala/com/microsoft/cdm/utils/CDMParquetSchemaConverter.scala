/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.cdm.utils

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema._
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._
import org.apache.parquet.schema.Types.MessageTypeBuilder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


/**
 * This converter class is used to convert Spark SQL [[StructType]] to Parquet [[MessageType]].
 *
 * @param writeLegacyParquetFormat Whether to use legacy Parquet format compatible with Spark 1.4
 *        and prior versions when converting a Catalyst [[StructType]] to a Parquet [[MessageType]].
 *        When set to false, use standard format defined in parquet-format spec.  This argument only
 *        affects Parquet write path.
 * @param outputTimestampType which parquet timestamp type to use when writing.
 */
class CDMSparkToParquetSchemaConverter(
                                        writeLegacyParquetFormat: Boolean = SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get,
                                        outputTimestampType: SQLConf.ParquetOutputTimestampType.Value =
                                        SQLConf.ParquetOutputTimestampType.INT96) {

  def this(conf: SQLConf) = this(
    writeLegacyParquetFormat = conf.writeLegacyParquetFormat,
    outputTimestampType = conf.parquetOutputTimestampType)

  def this(conf: Configuration) = this(
    writeLegacyParquetFormat = conf.get(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key).toBoolean,
    outputTimestampType = SQLConf.ParquetOutputTimestampType.withName(
      conf.get(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key)))

  def convert(catalystSchema: StructType, cdmSchema: Iterable[Any]): MessageType = {
    val bm = Types .buildMessage()
    convert(catalystSchema, cdmSchema, bm)
    bm.named(SPARK_PARQUET_SCHEMA_NAME)
  }

  def convert(schema: StructType, cdmSchema: Iterable[Any], bm : MessageTypeBuilder):Unit = {
    val arr = cdmSchema.toArray
    schema.fields.zipWithIndex.foreach { case (field, i) =>
      bm.addField(convertField(field, if (field.nullable) OPTIONAL else REQUIRED, arr(i)))
    }
  }

  def convertField(field: StructField, repetition: Type.Repetition, cdmType: Any): Type = {
    checkFieldName(field.name)

    field.dataType match {
      // ===================
      // Simple atomic types
      // ===================

      case BooleanType =>
        Types.primitive(BOOLEAN, repetition).named(field.name)

      case ByteType =>
        Types.primitive(INT32, repetition).as(INT_8).named(field.name)

      case ShortType =>
        Types.primitive(INT32, repetition).as(INT_16).named(field.name)

      case IntegerType =>
        Types.primitive(INT32, repetition).named(field.name)

      case LongType =>
        Types.primitive(INT64, repetition).named(field.name)

      case FloatType =>
        Types.primitive(FLOAT, repetition).named(field.name)

      case DoubleType =>
        Types.primitive(DOUBLE, repetition).named(field.name)

      case StringType =>
        Types.primitive(BINARY, repetition).as(UTF8).named(field.name)

      case DateType =>
        Types.primitive(INT32, repetition).as(DATE).named(field.name)

      // NOTE: Spark SQL can write timestamp values to Parquet using INT96, TIMESTAMP_MICROS or
      // TIMESTAMP_MILLIS. TIMESTAMP_MICROS is recommended but INT96 is the default to keep the
      // behavior same as before.
      //
      // As stated in PARQUET-323, Parquet `INT96` was originally introduced to represent nanosecond
      // timestamp in Impala for some historical reasons.  It's not recommended to be used for any
      // other types and will probably be deprecated in some future version of parquet-format spec.
      // That's the reason why parquet-format spec only defines `TIMESTAMP_MILLIS` and
      // `TIMESTAMP_MICROS` which are both logical types annotating `INT64`.
      //
      // Originally, Spark SQL uses the same nanosecond timestamp type as Impala and Hive.  Starting
      // from Spark 1.5.0, we resort to a timestamp type with microsecond precision so that we can
      // store a timestamp into a `Long`.  This design decision is subject to change though, for
      // example, we may resort to nanosecond precision in the future.
      case TimestampType => {
        /* Spark-CDM:
         * * If there is a metadata field overwrite to type Time (implicit write) set the parquet field to type Time
         * * IF there is an explicit cdmType type of Type "Time", also set the field to type Time */
        if (field.metadata.contains(Constants.MD_DATATYPE_OVERRIDE) &&
          field.metadata.getString(Constants.MD_DATATYPE_OVERRIDE).equals(Constants.MD_DATATYPE_OVERRIDE_TIME)
          || cdmType.equals("Time")) {
          Types.primitive(INT64, repetition).as(OriginalType.TIME_MICROS).named(field.name)
        } else {
          outputTimestampType match {
            case SQLConf.ParquetOutputTimestampType.INT96 =>
              Types.primitive(INT96, repetition).named(field.name)
            case SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS =>
              Types.primitive(INT64, repetition).as(TIMESTAMP_MICROS).named(field.name)
            case SQLConf.ParquetOutputTimestampType.TIMESTAMP_MILLIS =>
              Types.primitive(INT64, repetition).as(TIMESTAMP_MILLIS).named(field.name)
          }
        }
      }

      case BinaryType =>
        Types.primitive(BINARY, repetition).named(field.name)

      case DecimalType() => {
        val decimal = field.dataType.asInstanceOf[DecimalType]
        val precision = decimal.precision
        val scale = decimal.scale
        if (writeLegacyParquetFormat) {
          // ======================
          // Decimals (legacy mode)
          // ======================
          // Spark 1.4.x and prior versions only support decimals with a maximum precision of 18 and
          // always store decimals in fixed-length byte arrays.  To keep compatibility with these older
          // versions, here we convert decimals with all precisions to `FIXED_LEN_BYTE_ARRAY` annotated
          // by `DECIMAL`.
          Types
            .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
            .as(DECIMAL)
            .precision(precision)
            .scale(scale)
            .length(Decimal.minBytesForPrecision(precision))
            .named(field.name)

          // ========================
          // Decimals (standard mode)
          // ========================
        } else if (precision <= Decimal.MAX_INT_DIGITS) {
          Types
            .primitive(INT32, repetition)
            .as(DECIMAL)
            .precision(precision)
            .scale(scale)
            .named(field.name)
        } else if (precision <= Decimal.MAX_LONG_DIGITS) {
          Types
            .primitive(INT64, repetition)
            .as(DECIMAL)
            .precision(precision)
            .scale(scale)
            .named(field.name)
        } else {
          Types
            .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
            .as(DECIMAL)
            .precision(precision)
            .scale(scale)
            .length(Decimal.minBytesForPrecision(precision))
            .named(field.name)
        }
      }

      // ===================================
      // ArrayType and MapType (legacy mode)
      // ===================================

      // Spark 1.4.x and prior versions convert `ArrayType` with nullable elements into a 3-level
      // `LIST` structure.  This behavior is somewhat a hybrid of parquet-hive and parquet-avro
      // (1.6.0rc3): the 3-level structure is similar to parquet-hive while the 3rd level element
      // field name "array" is borrowed from parquet-avro.
      case ArrayType(elementType, nullable @ true) if writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   optional group bag {
        //     repeated <element-type> array;
        //   }
        // }

        // This should not use `listOfElements` here because this new method checks if the
        // element name is `element` in the `GroupType` and throws an exception if not.
        // As mentioned above, Spark prior to 1.4.x writes `ArrayType` as `LIST` but with
        // `array` as its element name as below. Therefore, we build manually
        // the correct group type here via the builder. (See SPARK-16777)
        Types
          .buildGroup(repetition).as(LIST)
          .addField(Types
            .buildGroup(REPEATED)
            // "array" is the name chosen by parquet-hive (1.7.0 and prior version)
            .addField(convertField(StructField("array", elementType, nullable),if (field.nullable) OPTIONAL else REQUIRED,cdmType.asInstanceOf[Iterable[Any]]))
            .named("bag"))
          .named(field.name)

      // Spark 1.4.x and prior versions convert ArrayType with non-nullable elements into a 2-level
      // LIST structure.  This behavior mimics parquet-avro (1.6.0rc3).  Note that this case is
      // covered by the backwards-compatibility rules implemented in `isElementType()`.
      case ArrayType(elementType, nullable @ false) if writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   repeated <element-type> element;
        // }

        // Here too, we should not use `listOfElements`. (See SPARK-16777)
        Types
          .buildGroup(repetition).as(LIST)
          // "array" is the name chosen by parquet-avro (1.7.0 and prior version)
          .addField(convertField(StructField("array", elementType, nullable), REPEATED, cdmType.asInstanceOf[Iterable[Any]]))
          .named(field.name)

      // Spark 1.4.x and prior versions convert MapType into a 3-level group annotated by
      // MAP_KEY_VALUE.  This is covered by `convertGroupField(field: GroupType): DataType`.
      case MapType(keyType, valueType, valueContainsNull) if writeLegacyParquetFormat =>
        // <map-repetition> group <name> (MAP) {
        //   repeated group map (MAP_KEY_VALUE) {
        //     required <key-type> key;
        //     <value-repetition> <value-type> value;
        //   }
        // }
        ConversionPatterns.mapType(
          repetition,
          field.name,
          convertField(StructField("key", keyType, nullable = false),if (field.nullable) OPTIONAL else REQUIRED,cdmType.asInstanceOf[Iterable[Any]]),
          convertField(StructField("value", valueType, valueContainsNull),if (field.nullable) OPTIONAL else REQUIRED,cdmType.asInstanceOf[Iterable[Any]]))

      // =====================================
      // ArrayType and MapType (standard mode)
      // =====================================

      case ArrayType(elementType, containsNull) if !writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   repeated group list {
        //     <element-repetition> <element-type> element;
        //   }
        // }
        Types
          .buildGroup(repetition).as(LIST)
          .addField(
            Types.repeatedGroup()
              .addField(convertField(StructField("element", elementType, containsNull), if (field.nullable) OPTIONAL else REQUIRED,cdmType.asInstanceOf[Iterable[Any]]))
              .named("list"))
          .named(field.name)

      case MapType(keyType, valueType, valueContainsNull) =>
        // <map-repetition> group <name> (MAP) {
        //   repeated group key_value {
        //     required <key-type> key;
        //     <value-repetition> <value-type> value;
        //   }
        // }
        Types
          .buildGroup(repetition).as(MAP)
          .addField(
            Types
              .repeatedGroup()
              .addField(convertField(StructField("key", keyType, nullable = false),if (field.nullable) OPTIONAL else REQUIRED, cdmType.asInstanceOf[Iterable[Any]]))
              .addField(convertField(StructField("value", valueType, valueContainsNull),if (field.nullable) OPTIONAL else REQUIRED,cdmType.asInstanceOf[Iterable[Any]]))
              .named("key_value"))
          .named(field.name)

      // ===========
      // Other types
      // ===========

      case StructType(fields) => {
        val bg = Types.buildGroup(repetition)
        fields.zipWithIndex.foreach{
          case (field, fieldIndex) => {
            val cdmStruct = cdmType.asInstanceOf[List[Any]]
            bg.addField(convertField(field, if (field.nullable) OPTIONAL else REQUIRED,
              cdmStruct(fieldIndex)))
          }
        }
        bg.named(field.name)
      }
      /* fields.foldLeft(Types.buildGroup(repetition)) {(builder, field) => {
         builder.addField(convertField(field, if (field.nullable) OPTIONAL else REQUIRED,cdmType.asInstanceOf[Iterable[Any]]))
       }
       }.named(field.name)

       */

      //case udt: UserDefinedType[_] =>
      //  convertField(field.copy(dataType = udt.sqlType))

      case _ =>
        throw new Exception(s"Unsupported data type ${field.dataType.catalogString}")
    }
  }
  private val SPARK_PARQUET_SCHEMA_NAME = "spark_schema"

  private val EMPTY_MESSAGE: MessageType =
    Types.buildMessage().named(SPARK_PARQUET_SCHEMA_NAME)

  private def checkFieldName(name: String): Unit = {
    // ,;{}()\n\t= and space are special characters in Parquet schema
    checkConversionRequirement(
      !name.matches(".*[ ,;{}()\n\t=].*"),
      s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
         |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  private def checkFieldNames(names: Seq[String]): Unit = {
    names.foreach(checkFieldName)
  }

  private def checkConversionRequirement(f: => Boolean, message: String): Unit = {
    if (!f) {
      throw new Exception(message)
    }
  }
}

