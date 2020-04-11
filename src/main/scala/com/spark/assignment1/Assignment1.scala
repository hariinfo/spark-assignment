package com.spark.assignment1

import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Hariharan Vadivelu <vadiv005@umn.edu>
  * @since 1.0
  * Implementation of the assignment 1 tests in scala. <br>
  * This implementation should be run from the Assignment1Test unit test.
  */
object Assignment1 {

  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  /**
    * Helper function to print out the contents of an RDD
    * @param label Label for easy searching in logs
    * @param theRdd The RDD to be printed
    * @param limit Number of elements to print
    */
  private def printRdd[_](label: String, theRdd: RDD[_], limit: Integer = 20) = {
    val limitedSizeRdd = theRdd.take(limit)
    println(s"""$label ${limitedSizeRdd.toList.mkString(",")}""")
  }

  /**
    * Sort the records in descending order and retrieve the first record which gives us the longest
    * trip duration
    * @param tripData The Trip RDD
    * @return Longest trip duration
    */

  def problem1(tripData: RDD[Trip]): Long = {
    val maxTripDuration = tripData.sortBy(line => line.duration, false)
    val Trip = maxTripDuration.take(1).map(line => line.duration)
    return Trip.mkString.toLong
  }

/*
  def problem1(tripData: RDD[Trip]): Long = {
    val maxTrip = tripData.map(d => d.duration)
    println(maxTrip.max())
    return maxTrip.max().toLong
  }
*/

  /**
    * Filter the RDD based on start_station = "San Antonio Shopping Center"
    * @param trips The trip RDD
    * @return Count of trips starting at the 'San Antonio Shopping Center' station.
    */
  def problem2(trips: RDD[Trip]): Long = {
    val yearList = List("San Antonio Shopping Center' station")
    val data = trips.filter(line => line.start_station.contains("San Antonio Shopping Center"))
    return data.count()
  }

  /**
    * Extract the subscribe type column and then perform a distinct operation to return the unique
    * subscriber type
    * @param trips The trip RDD
    * @return List of all the subscriber types from the 'trip' dataset.
    */
  def problem3(trips: RDD[Trip]): Seq[String] = {
    val subscriberTypeCol = trips.map(line => line.subscriber_type)
    return subscriberTypeCol.distinct().collect()
  }

  /**
    * RDD countByValue is used to retrieve each value in this RDD as a local map of value and count
    * @param trips The trip RDD
    * @return The zip code with the most rides taken.
    */
  def problem4(trips: RDD[Trip]): String = {
    val subscriberTypeCol = trips.map(line => line.zip_code).countByValue().maxBy(_._2)
    return subscriberTypeCol._1
  }

  /**
    *
    * @param trips The trip RDD
    * @return
    */
  def filterGreaterThanDay(trips: RDD[Trip]): RDD[Trip] = {
    ???
  }

  /**
    * Filter a new RDD containing only elements that satisfy the condition where
    * the bikes are retained over night
    * @param trips The trip RDD
    * @return
    */
  def problem5(trips: RDD[Trip]): Long = {
    val datesRDD =
      trips.filter(line => parseTimestamp(line.end_date).getDayOfYear != (parseTimestamp(line.start_date).getDayOfYear))
    return datesRDD.count()
  }

  /**
    * Perform a count on the RDD to return the number of elements
    * @param trips The trip RDD
    * @return Return the number of elements in the RDD
    */
  def problem6(trips: RDD[Trip]): Long = {
    trips.count()
  }

  /**
    * Use the output from problem 5 and divide by total trip count
    * @param trips The trip RDD
    * @return return the ration of bikes vs total trips that we kept at least one night
    */
  def problem7(trips: RDD[Trip]): Double = {
    val totalOverNightTrip = problem5(trips)
    val tripCount = trips.count()
    var percentageVal =
      BigDecimal((totalOverNightTrip * 1.0f / trips.count())).setScale(5, BigDecimal.RoundingMode.HALF_UP).toDouble
    return percentageVal
  }

  /**
    * Doubles the duration of each trip and return the sum total
    * @param trips The trip RDD
    * @return sum total of trip duration
    */
  def problem8(trips: RDD[Trip]): Double = {
    val totalDuration = trips.map(line => line.duration * 2).sum
    return totalDuration
  }

  def keyByF(t: Int) = t * t

  /**
    * Return a trip RDD consisting of trip id "913401", now retrieve the station name from the
    * filtered rdd and filter the stations rdd consisting of station name equal to the station name from the filtered
    * station rdd. Finally, from the filtered station rdd lookup the lat and long
    *
    * @param trips The trip RDD
    * @param stations The station RDD
    * @return (latitude and longitude) of the trip with the id 913401
    */
  def problem9(trips: RDD[Trip], stations: RDD[Station]): (Double, Double) = {
    val tripRdd = trips.filter(line => line.trip_id.contains("913401"))
    val startStation = tripRdd.take(1).map(line => line.start_station).take(1).mkString
    val dataCordinate = stations.filter(line => line.name.contains(startStation))
    val output =
      (dataCordinate.map(line => line.lat).take(1).mkString.toDouble, dataCordinate.map(line => line.lon).take(1).mkString.toDouble)
    return output
  }

  /**
    * We first create a map from the two RDDs and then create a pair rdd to make a join
    * From the joined result we create a map of station name and duration and then finally
    * group it based on the key (station name) and sum up the values to get duration of each trip
    * @param trips The trip RDD
    * @param stations The station RDD
    * @return Duration of all trips by starting at each station
    */
  def problem10(trips: RDD[Trip], stations: RDD[Station]): Array[(String, Long)] = {
    val tripsRdd = trips.map(line1 => (line1.start_station, line1.duration.toLong)).collect()
    val stationRdd = stations.map(line => (line.name, line.station_id)).collect()
    val tripsRddC = trips.sparkContext.parallelize(tripsRdd)
    val stationRddC = stations.sparkContext.parallelize(stationRdd)
    val joinRdd = tripsRddC.join(stationRddC)
    val groupedRdd = joinRdd.map(line => (line._1, line._2._1)).collect()
    val rdd1 = trips.sparkContext.parallelize(groupedRdd).groupByKey()
    val rdd2 = rdd1.map(line => (line._1, line._2.sum)).collect()
    return rdd2.toArray
  }

  /*
   Dataframes
   */

  /**
    * Perform a select on the dataframe for trip_id column
    * @param trips Trips dataframe
    * @return trip id column of the dataframe
    */
  def dfProblem11(trips: DataFrame): DataFrame = {
    val data = trips.select("trip_id")
    println(data)
    return data
  }

  /**
    * Filter the dataframe rows based on start station
    * @param trips Trips dataframe
    * @return Count of all the trips starting at 'Harry Bridges Plaza (Ferry Building)'
    */
  def dfProblem12(trips: DataFrame): DataFrame = {
    val data = trips.filter(trips("start_station") === "Harry Bridges Plaza (Ferry Building)")
    println(data.show())
    println("Data count is" + data.count())
    return data
  }

  /**
    * Aggregate on the entire dataframe based and perform a sum of duration
    * @param trips Trips dataframe
    * @return sum of the duration of all trips
    */
  def dfProblem13(trips: DataFrame): Long = {
    val data = trips.agg(sum("duration"))
    return data.first().getAs[Long](0)
  }

  // Helper function to parse the timestamp format used in the trip dataset.
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))

}
