package com.spark.assignment1

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter

import scala.collection.{immutable, mutable}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession


object Assignment2 {

  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  def Problem0(airlineData: DataFrame): Long = {
    println(airlineData.count())
    airlineData.count()
  }

  def Problem1(airlineData: DataFrame): DataFrame = {

    val delaysCount = airlineData.filter(airlineData.col("ArrDel15").gt(0)).count()
    val lateAircraftDelayCount = airlineData.filter(airlineData.col("LateAircraftDelay").gt(0)).toDF()
    val securityDelayCount = airlineData.filter(airlineData.col("SecurityDelay").gt(0)).toDF()
    val nASDelayCount = airlineData.filter(airlineData.col("NASDelay").gt(0)).toDF()
    val weatherDelayCount = airlineData.filter(airlineData.col("WeatherDelay").gt(0)).toDF()
    val carrierDelayCount = airlineData.filter(airlineData.col("CarrierDelay").gt(0)).toDF()

    val aggData = Seq(
      Row("lateAircraftDelayCount", lateAircraftDelayCount.count() * 1.0f / delaysCount * 100),
      Row("securityDelayCount", securityDelayCount.count() * 1.0f / delaysCount * 100),
      Row("nASDelayCount", nASDelayCount.count() * 1.0f / delaysCount * 100),
      Row("weatherDelayCount", weatherDelayCount.count() * 1.0f / delaysCount * 100),
      Row("carrierDelayCount", carrierDelayCount.count() * 1.0f / delaysCount * 100)
    )
    val someSchema = List(
      StructField("delayType", StringType, true),
      StructField("count", FloatType, false)
    )

    val responseDF = airlineData.sparkSession.createDataFrame(
      airlineData.sparkSession.sparkContext.parallelize(aggData),
      StructType(someSchema)
    )

    responseDF
  }

  /**
    * Filter the DF with arrival delay for delta airlines
    *
    * @param airlineData
    * @return
    */
  def Problem2(airlineData: DataFrame): DataFrame = {
    val data = airlineData.filter("ArrDel15 > 0 and Reporting_Airline = 'DL'").orderBy("FlightDate")
      .groupBy("Reporting_Airline", "FlightDate").count().limit(4)
    return data
  }

  def Problem3(airlineData: DataFrame): (Long, Long) = {
    //Declare the UDF
    val spark = SparkSession.builder().appName("udfTest").master("local").getOrCreate()
    import spark.implicits._
    spark.udf.register("airline_ownership", airline_ownership _)
    val airlineDataWithOwnership = airlineData.withColumn("ownership", callUDF("airline_ownership", col("Reporting_Airline")))
    val publicOwnership = airlineDataWithOwnership.filter("ArrDel15 > 0 and ownership = 'Public'").count()
    val privateOwnership = airlineDataWithOwnership.filter("ArrDel15 > 0 and ownership = 'Private'").count()

    (publicOwnership, privateOwnership)
  }

  def Problem4(airlineData: DataFrame): DataFrame = {
    val CarrierDelayDF = airlineData.filter("ArrDel15 > 0 and CarrierDelay > 0 and Origin = 'MSY'")
      .groupBy("Origin").count().limit(1)

    val WeatherDelayDF = airlineData.filter("ArrDel15 > 0 and WeatherDelay > 0 and Origin = 'MSY'")
      .groupBy("Origin").count().limit(1)

    val NASDelayDF = airlineData.filter("ArrDel15 > 0 and NASDelay > 0 and Origin = 'MSY'")
      .groupBy("Origin").count().limit(1)

    val SecurityDelayDF = airlineData.filter("ArrDel15 > 0 and SecurityDelay > 0 and Origin = 'MSY'")
      .groupBy("Origin").count().limit(1)

    val LateAircraftDelayDF = airlineData.filter("ArrDel15 > 0 and LateAircraftDelay > 0 and Origin = 'MSY'")
      .groupBy("Origin").count().limit(1)

    CarrierDelayDF.union(WeatherDelayDF).union(NASDelayDF).union(SecurityDelayDF).union(LateAircraftDelayDF)
  }

  def Problem5(modernFleet: DataFrame, legacyFleet: DataFrame): (Long, Long) = {
    val modernFleetDelay = modernFleet.where("ArrDel15 > 0 and CarrierDelay > 0")
                          .select(col("ArrDel15")).count()
    val legacyFleetDelay = legacyFleet.where("ArrDel15 > 0 and CarrierDelay > 0")
                          .count()
    (modernFleetDelay, legacyFleetDelay)
  }



  // Helper function to parse the timestamp format used in the trip dataset.
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))

  private def airline_ownership(row: String): String = {
    //Delta, united, American Airlines and SouthWest airlines are publicly traded
    if (row.equalsIgnoreCase("DL") ||
      row.equalsIgnoreCase("AA") ||
      row.equalsIgnoreCase("UA") ||
      row.equalsIgnoreCase("WN"))
      "Public"
    else //All other airline codes are privately managed
      "Private"
  }

}
