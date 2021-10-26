package com.microsoft.cdm.test

import java.sql.Timestamp
import java.time.{LocalDate, LocalTime, ZoneId}
import java.time.format.DateTimeFormatter

import com.microsoft.cdm.utils.Constants
import com.microsoft.cdm.utils.Constants.DECIMAL_PRECISION
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, MetadataBuilder, ShortType, StringType, StructField, StructType, TimestampType}

class TestData(val spark: SparkSession) {

  val date= java.sql.Date.valueOf("2015-03-31");
  val timestamp = new java.sql.Timestamp(System.currentTimeMillis());

  def prepareDataWithAllTypes(): DataFrame =  {
    val byteVal = 2.toByte
    val shortVal = 129.toShort
    val data = Seq(
      Row("tim", 1, true, 12.34,6L, date, Decimal(999.00), timestamp, 2f, byteVal, shortVal),
      Row("tddim", 1, false, 13.34,7L, date, Decimal(434.3), timestamp, 3.59f, byteVal, shortVal),
      Row("tddim", 1, false, 13.34,7L, date, Decimal(100.0), timestamp, 3.59f, byteVal, shortVal),
      Row("tddim", 1, false, 13.34,7L, date, Decimal(99.898), timestamp, 3.59f, byteVal, shortVal),
      Row("tim", 1, true, 12.34,6L, date, Decimal(1.3), timestamp, 3.59f, byteVal, shortVal),
      Row("tddim", 1, false, 13.34,7L, date, Decimal(99999.3), timestamp, 3590.9f, byteVal, shortVal),
      Row("tddim", 1, false, 13.34,7L, date, Decimal(4324.4324324), timestamp, 359.8f, byteVal, shortVal),
      Row("tddim", 1, false, 13.34,7L, date, Decimal(42.4), timestamp, 3.593f, byteVal, shortVal),
      Row("tddim", 1, false, 13.34,7L, date, Decimal(1.43434), timestamp, 3.59f, byteVal, shortVal),
      Row("tddim", 1, false, 13.34,7L, date, Decimal(0.0167), timestamp, 3.59f, byteVal, shortVal),
      Row("tddim", 1, false, 13.34,7L, date, Decimal(0.00032), timestamp, 332.33f, byteVal, shortVal),
      Row("tddim", 1, false, 13.34,7L, date, Decimal(78.5), timestamp, 3.53232f, byteVal, shortVal)
    )

    val schema = new StructType()
      .add(StructField("name", StringType, true))
      .add(StructField("id", IntegerType, true))
      .add(StructField("flag", BooleanType, true))
      .add(StructField("salary", DoubleType, true))
      .add(StructField("phone", LongType, true))
      .add(StructField("dob", DateType, true))
      .add(StructField("weight",  DecimalType(Constants.DECIMAL_PRECISION,7), true))
      .add(StructField("time", TimestampType, true))
      .add(StructField("float", FloatType, true))
      .add(StructField("byte", ByteType, true))
      .add(StructField("short", ShortType, true))

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }


  def prepareNullAndEmptyArrays() = {
    val data = Seq(
      Row(Array(null, null)) ,
      Row(Array()),
      Row(null),
      Row(Array(null, Row(null)))
    )
    val schema = new StructType()
      .add(StructField("name", ArrayType(StructType(List(StructField("name", StringType, true)))), true)
      )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df
  }

  def  prepareSimpleDataArrayWithTime() : DataFrame = {

    val formatterTime1 = DateTimeFormatter.ofPattern("HH:mm:ss")
    val ls = LocalTime.parse("10:09:08", formatterTime1)
    val instant = ls.atDate(LocalDate.of(1970, 1, 1)).atZone(ZoneId.systemDefault()).toInstant
    val timestamp = Timestamp.from(instant)

    val md = new MetadataBuilder().putString(Constants.MD_DATATYPE_OVERRIDE, Constants.MD_DATATYPE_OVERRIDE_TIME).build()
    val data = Seq(
      Row(Array(Row("RowOneArray1", 1, timestamp))),
      Row(Array(Row("RowTwoArray1", 3, timestamp), Row("RowTwoArray2", 4, timestamp)))
    )
    val schema = new StructType()
      .add(StructField("name", ArrayType(StructType(
        List(StructField("name", StringType, true),
          StructField("number", IntegerType, true),
          StructField("aTime", TimestampType, true,md)))),
        true)
      )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data,1), schema)
    df
  }

  def   prepareNestedDataArrays() : DataFrame = {
    val date= java.sql.Date.valueOf("2015-03-31")

    val formatterTime1 = DateTimeFormatter.ofPattern("HH:mm:ss")
    val ls = LocalTime.parse("10:09:08", formatterTime1)
    val instant = ls.atDate(LocalDate.of(1970, 1, 1)).atZone(ZoneId.systemDefault()).toInstant
    val timestamp = Timestamp.from(instant)

    val data = Seq( Row(13, Row("Str1", true, 12.34,6L, date, Decimal(2.3), timestamp, Row("sub1", Row(timestamp), Array(Row("RowOneArray1", 1, timestamp), Row("RowOneArray2", 2, timestamp))))) ,
      Row(24, Row("Str2", false, 12.34,6L, date, Decimal(2.3), timestamp, Row("sub2", Row(timestamp), Array(Row("RowTwoArray1", 3, timestamp), Row("RowTwoArray2", 4, timestamp), Row("RowTwoArray3", 5, timestamp), Row("RowTwoArray4", 6, timestamp)))))
    )

    val schema = new StructType()
      .add(StructField("id", IntegerType, true))
      .add(StructField("details", new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))
        .add(StructField("dob", DateType, true))
        .add(StructField("weight",  DecimalType(DECIMAL_PRECISION,1), true))
        .add(StructField("time", TimestampType, true))
        .add(StructField("subRow", new StructType()
          .add(StructField("name", StringType, true))
          .add(StructField("level3", new StructType()
            .add(StructField("time1", TimestampType, true))
          )
          )
          .add(StructField("hit_songs", ArrayType(StructType(List(StructField("name", StringType, true),
            StructField("number", IntegerType, true),
            StructField("aTime", TimestampType, true))), true), true))
        )
        )))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data,1), schema)
    df
  }

  def prepareNestedData():  DataFrame = {
    val date= java.sql.Date.valueOf("2015-03-31")
    val timestamp = new java.sql.Timestamp(System.currentTimeMillis());
    val data = Seq(
      Row(13, Row("Str1", true, 12.34,6L, date, Decimal(2.3), timestamp,  Row("sub1", Row(timestamp)))) ,
      Row(24, Row("Str2", false, 12.34,6L, date, Decimal(2.3), timestamp, Row("sub2", Row(timestamp))))
    )

    val schema = new StructType()
      .add(StructField("id", IntegerType, true))
      .add(StructField("details", new StructType()
        .add(StructField("name", StringType, true))
        .add(StructField("flag", BooleanType, true))
        .add(StructField("salary", DoubleType, true))
        .add(StructField("phone", LongType, true))
        .add(StructField("dob", DateType, true))
        .add(StructField("weight",  DecimalType(DECIMAL_PRECISION,1), true))
        .add(StructField("time", TimestampType, true))
        .add(StructField("subRow", new StructType()
          .add(StructField("name", StringType, true))
          .add(StructField("level3", new StructType()
            .add(StructField("time1", TimestampType, true))
          )
          )
        )
        )))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df
  }

  def prepareNullData(): DataFrame = {

    val data = Seq(
      Row(null, null, null, null,null, null, null, null, null, null, null)
    )

    val schema = new StructType()
      .add(StructField("name", StringType, true))
      .add(StructField("id", IntegerType, true))
      .add(StructField("flag", BooleanType, true))
      .add(StructField("salary", DoubleType, true))
      .add(StructField("phone", LongType, true))
      .add(StructField("dob", DateType, true))
      .add(StructField("weight",  DecimalType(Constants.DECIMAL_PRECISION,7), true))
      .add(StructField("time", TimestampType, true))
      .add(StructField("float", FloatType, true))
      .add(StructField("byte", ByteType, true))
      .add(StructField("short", ShortType, true))

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

}
