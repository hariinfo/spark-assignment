package com.spark.assignment1

import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.immutable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

object Assignment2 {

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

    val aggData = Seq (
      Row("lateAircraftDelayCount", lateAircraftDelayCount.count() * 1.0f / delaysCount.count() *100),
      Row("securityDelayCount", securityDelayCount.count() * 1.0f / delaysCount.count() *100),
      Row("nASDelayCount", nASDelayCount.count() * 1.0f/ delaysCount.count() *100),
      Row("weatherDelayCount", weatherDelayCount.count() * 1.0f/ delaysCount.count() *100),
      Row("carrierDelayCount", carrierDelayCount.count() * 1.0f/ delaysCount.count() *100)
    )
    val someSchema = List(
      StructField("delayType", StringType, true),
      StructField("count", FloatType, false)
    )

    val responseDF = airlineData.sparkSession.createDataFrame(
      airlineData.sparkSession.sparkContext.parallelize(aggData),
      StructType(someSchema)
    )

    //delaysCount.union(lateAircraftDelayCount).union(securityDelayCount).union(nASDelayCount).union(weatherDelayCount).union(carrierDelayCount)
/*
    delaysCount
    println(lateAircraftDelayCount.count() * 1.0f / delaysCount.count() *100)
    println(securityDelayCount.count() * 1.0f/ delaysCount.count() *100)
    println(nASDelayCount.count() * 1.0f/ delaysCount.count() *100)
    println(weatherDelayCount.count() * 1.0f/ delaysCount.count() *100)
    println(carrierDelayCount.count() * 1.0f/ delaysCount.count() *100)
    //val response = Array("" -> )
    val response = Array("lateAircraftDelayCount" -> lateAircraftDelayCount.count() * 1.0f / delaysCount.count() *100,
                      "securityDelayCount" -> securityDelayCount.count() * 1.0f/ delaysCount.count() *100,
                      "nASDelayCount" -> nASDelayCount.count() * 1.0f/ delaysCount.count() *100,
                      "weatherDelayCount" -> weatherDelayCount.count() * 1.0f/ delaysCount.count() *100,
                      "carrierDelayCount" -> carrierDelayCount.count() * 1.0f/ delaysCount.count() *100
                      )
    println(response.toMap)

 */
    //return response.toMap
    //return carrierDelayCount.count() * 1.0f/ delaysCount.count() *100
    return responseDF
  }

  def Problem2(airlineData: DataFrame): DataFrame = {
    val data = airlineData.filter("CarrierDelay > 0")
        .groupBy("Reporting_Airline").count()
    data.show()
    return data
  }

  def Problem3(airlineData: DataFrame): Array[(String, Float)] = {
    return ???
  }

  def Problem4(airlineData: DataFrame): Array[(String, Float)] = {
    return ???
  }

  def Problem5(airlineData: DataFrame): Array[(String, Float)] = {
    return ???
  }
}
