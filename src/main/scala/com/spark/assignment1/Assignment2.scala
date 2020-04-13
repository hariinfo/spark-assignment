package com.spark.assignment1

import org.apache.spark.sql.DataFrame

object Assignment2 {

  def Problem1(airlineData: DataFrame): Long = {
    println(airlineData.count())
    return airlineData.count()
  }

  def Problem2(airlineData: DataFrame): Array[(String, Float)] = {
    val delaysCount = airlineData.filter(airlineData.col("ArrDel15").gt(0))
    val lateAircraftDelayCount = airlineData.filter(airlineData.col("LateAircraftDelay").gt(0))
    val securityDelayCount = airlineData.filter(airlineData.col("SecurityDelay").gt(0))
    val nASDelayCount = airlineData.filter(airlineData.col("NASDelay").gt(0))
    val weatherDelayCount = airlineData.filter(airlineData.col("WeatherDelay").gt(0))
    val carrierDelayCount = airlineData.filter(airlineData.col("CarrierDelay").gt(0))

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
    println(response)
    return response
  }

  def Problem3(airlineData: DataFrame): Array[(String, Float)] = {
    return ???
  }

  def Problem5(airlineData: DataFrame): Array[(String, Float)] = {
    return ???
  }
}
