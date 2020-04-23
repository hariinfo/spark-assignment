package com.spark.assignment1

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter

import scala.collection.immutable
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, LongType, StringType, StructField, StructType}


object Assignment2 {

  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  def Problem0(airlineData: DataFrame): Long = {
    println(airlineData.count())
    return airlineData.count()
  }

  def Problem1(airlineData: DataFrame): DataFrame = {
    val delaysCount = airlineData.filter(airlineData.col("ArrDel15").gt(0)).toDF()
    val lateAircraftDelayCount = airlineData.filter(airlineData.col("LateAircraftDelay").gt(0)).toDF()
    val securityDelayCount = airlineData.filter(airlineData.col("SecurityDelay").gt(0)).toDF()
    val nASDelayCount = airlineData.filter(airlineData.col("NASDelay").gt(0)).toDF()
    val weatherDelayCount = airlineData.filter(airlineData.col("WeatherDelay").gt(0)).toDF()
    val carrierDelayCount = airlineData.filter(airlineData.col("CarrierDelay").gt(0)).toDF()

    val aggData = Seq(
      Row("lateAircraftDelayCount", lateAircraftDelayCount.count() * 1.0f / delaysCount.count() * 100),
      Row("securityDelayCount", securityDelayCount.count() * 1.0f / delaysCount.count() * 100),
      Row("nASDelayCount", nASDelayCount.count() * 1.0f / delaysCount.count() * 100),
      Row("weatherDelayCount", weatherDelayCount.count() * 1.0f / delaysCount.count() * 100),
      Row("carrierDelayCount", carrierDelayCount.count() * 1.0f / delaysCount.count() * 100)
    )
    val someSchema = List(
      StructField("delayType", StringType, true),
      StructField("count", FloatType, false)
    )

    val responseDF = airlineData.sparkSession.createDataFrame(
      airlineData.sparkSession.sparkContext.parallelize(aggData),
      StructType(someSchema)
    )

    return responseDF
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

    data.show()
    return data
  }

  def Problem3(airlineData: DataFrame): Array[(String, Float)] = {
    return ???
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

    CarrierDelayDF.show()
    WeatherDelayDF.show()
    NASDelayDF.show()
    SecurityDelayDF.show()
    LateAircraftDelayDF.show()

    CarrierDelayDF.union(WeatherDelayDF).union(NASDelayDF).union(SecurityDelayDF).union(LateAircraftDelayDF).show()

    return CarrierDelayDF.union(WeatherDelayDF).union(NASDelayDF).union(SecurityDelayDF).union(LateAircraftDelayDF)
  }

  def Problem5(modernFleet: DataFrame, legacyFleet: DataFrame): (Long, Long) = {
    modernFleet.show()
    legacyFleet.show()
    val modernFleetDelay = modernFleet.where("ArrDel15 > 0 and CarrierDelay > 0")
                          .select(col("ArrDel15")).count()
    val legacyFleetDelay = legacyFleet.where("ArrDel15 > 0 and CarrierDelay > 0")
                          .count()
    println(modernFleetDelay)
    println(legacyFleetDelay)

    val expectedData = Seq(
      ("lateAircraftDelayCount", modernFleetDelay),
      ("securityDelayCount", legacyFleetDelay),
    ).to

    return (modernFleetDelay, legacyFleetDelay)
  }




  // Helper function to parse the timestamp format used in the trip dataset.
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))

  private def airline_ownership(row: String): String = {
    if (row.equalsIgnoreCase("DL") ||
      row.equalsIgnoreCase("AA") ||
      row.equalsIgnoreCase("UA"))
      return "Public"
    else (row.equalsIgnoreCase("WN"))
    return "Private"
  }

}
