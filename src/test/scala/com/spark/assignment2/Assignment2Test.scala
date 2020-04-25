package com.spark.assignment2

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import scala.concurrent.duration._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import com.spark.assignment1.Assignment2
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import org.apache.spark.sql.types.{FloatType, LongType, StringType, StructField, StructType}
import scala.reflect.io.File

class Assignment2Test extends AnyFunSuite with Matchers with BeforeAndAfterEach with DataFrameComparer with BeforeAndAfterAll {

  val AIRLINE_DATA_CSV_PATH = "data/On_Time_Reporting_Carrier_On_Time_Performance_*.csv"
  val CARRIERS_DATA_CSV_PATH = "data/carrier.csv"
  val PLANE_DATA_CSV_PATH = "data/plane-data.csv"

  val AIRLINE_PLANE_DATA_PARQUET_PATH ="airline_and_plane/joined.parquet"
  val BLOCK_ON_COMPLETION = false;

  /**
    * Create a SparkSession that runs locally on our laptop.
    */
  val spark =
    SparkSession
      .builder()
      .appName("Assignment 2")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .getOrCreate()
  /**
    * Let Spark infer the data types. Tell Spark this CSV has a header line.
    */
  val csvReadOptions =
    Map("inferSchema" -> true.toString, "header" -> true.toString)
  //TODO: Add Caching

  /**
    * Keep the Spark Context running so the Spark UI can be viewed after the test has completed.
    * This is enabled by setting `BLOCK_ON_COMPLETION = true` above.
    */
  override def afterEach: Unit = {
    if (BLOCK_ON_COMPLETION) {
      // open SparkUI at http://localhost:4040
      Thread.sleep(5.minutes.toMillis)
    }
  }

  override def beforeAll() {
    //Skip the parquet file creation logic if it already exists
    if (!File(AIRLINE_PLANE_DATA_PARQUET_PATH).exists){
      def airlineDataDF: DataFrame = spark.read.options(csvReadOptions).csv(AIRLINE_DATA_CSV_PATH)
      def carrierDataDF: DataFrame = spark.read.options(csvReadOptions).csv(CARRIERS_DATA_CSV_PATH)
      def planeDataDF: DataFrame = spark.read.option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", "MM/DD/YYYY")
        .option("mode", "DROPMALFORMED")
        .csv(PLANE_DATA_CSV_PATH)

      //Combine the dataframe and create a parquet of combined data
      //partition by airline
      val planeDataMod = planeDataDF.withColumnRenamed("tailnum", "Tail_Number").withColumnRenamed("year", "ManYear")
      val planeDataDFMod = planeDataMod.withColumn("issueDate", to_timestamp(col("issue_date"), "MM/DD/YYYY"))
      val planeDataCombined = airlineDataDF.join(planeDataDFMod, Seq("Tail_Number"), "left_outer")

      planeDataCombined.write.mode(SaveMode.Overwrite)
        .partitionBy("Reporting_Airline")
        .parquet(AIRLINE_PLANE_DATA_PARQUET_PATH)
    }
  }

  /**
    * This test performs sanity check on the data before
    * executing the tests related to business queries
    */
  test("Select count") {
    Assignment2.Problem0(readAirlineAndPlane.toDF()) must equal(835293)
  }

  /**
    * What is the percentage delay types by total delays?
    */
  test("percentage delay types by total delays") {
    val responseDF = Assignment2.Problem1(readAirlineAndPlane.toDF().cache())


    val expectedData = Seq(
      Row("lateAircraftDelayCount", 50.19418F),
      Row("securityDelayCount", 0.28828743F),
      Row("nASDelayCount", 57.59375F),
      Row("weatherDelayCount", 7.226842F),
      Row("carrierDelayCount", 47.694893F)
    )

    val expectedSchema = List(
      StructField("delayType", StringType, true),
      StructField("count", FloatType, false)
    )
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertSmallDataFrameEquality(expectedDF, responseDF)
  }

  /**
    * What is the min/max/average delays for an airline in a month and year?
    */
  test("min/max/average delays for an airline in a month and year") {
    val responseDF = Assignment2.Problem2(readAirlineAndPlane.toDF())
    val expectedData = Seq(
      Row("DL", "1/1/2019", 114L),
      Row("DL", "1/10/2019", 249L),
      Row("DL", "1/11/2019", 208L),
      Row("DL", "1/12/2019", 141L),
    )

    val expectedSchema = List(
      StructField("Reporting_Airline", StringType, true),
      StructField("FlightDate", StringType, true),
      StructField("count", LongType, false)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )
    assertSmallDataFrameEquality(expectedDF, responseDF)
  }

  /**
    * Did privately managed airlines perform better than publicly traded ones?
    */
  test("public vs private airlines") {
    val result = Assignment2.Problem3(readAirlineAndPlane().toDF())
    result must equal((108085,59803))
  }

  /**
    * What delay type is most common at each airport?
    */
  test("Delay type common at each airport") {
    val response = Assignment2.Problem4(readAirlineAndPlane().toDF())
    val expectedData = Seq(
      Row("MSY", 598L),
      Row("MSY", 25L),
      Row("MSY", 624L),
      Row("MSY", 1L),
      Row("MSY", 752L)
    )
    val expectedSchema = List(
      StructField("Origin", StringType, true),
      StructField("count", LongType, false)
    )
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )
    assertSmallDataFrameEquality(expectedDF, response)
  }

  /**
    *Did airlines with modernized fleet perform better?
    */
  test("Did airlines with modernized fleet perform better") {
   // val cachedAirlineData = readAirlineAndPlane().toDF().cache()
    val result = Assignment2.Problem5(
              readAirlineAndPlane().toDF().filter(col("ManYear")
                .gt(lit("2000")) and col("ArrDel15")
                .notEqual(0)),
              readAirlineAndPlane().toDF().filter(col("ManYear")
                .lt(lit("2000")) and col("ArrDel15")
                .notEqual(0))
              )

    result must equal((22200,9560))

  }

  private def readAirlineAndPlane(): DataFrame = {
    spark.read.parquet(AIRLINE_PLANE_DATA_PARQUET_PATH)
  }


}
