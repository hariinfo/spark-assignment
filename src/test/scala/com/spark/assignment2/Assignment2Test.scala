package com.spark.assignment2

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SaveMode, SparkSession}

import scala.concurrent.duration._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should._
import com.spark.assignment1.Airline
import com.spark.assignment1.Carrier
import com.spark.assignment1.Plane
import com.spark.assignment1.Assignment2
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

class Assignment2Test extends AnyFunSuite with Matchers with BeforeAndAfterEach with DataFrameComparer with BeforeAndAfterAll {

  val AIRLINE_DATA_CSV_PATH = "data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2019_1.csv"
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
  implicit val airlineEncoder: Encoder[Airline] = Encoders.product[Airline]
  implicit val carrierEncoder: Encoder[Carrier] = Encoders.product[Carrier]
  implicit val planeEncoder: Encoder[Plane] = Encoders.product[Plane]

  def airlineDS: Dataset[Airline] = spark.read.options(csvReadOptions).csv(AIRLINE_DATA_CSV_PATH).as[Airline]
  def airlineDataDF: DataFrame = airlineDS.toDF()

  def carrierDS: Dataset[Carrier] = spark.read.options(csvReadOptions).csv(CARRIERS_DATA_CSV_PATH).as[Carrier]
  def carrierDataDF: DataFrame = carrierDS.toDF()


  def planeDataDF: DataFrame = spark.read.option("header", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .option("timestampFormat", "MM/DD/YYYY")
    .option("mode", "DROPMALFORMED")
    .csv(PLANE_DATA_CSV_PATH)

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
    //Combine the dataframe and create a parquet of combined data
    //partition by airline
    val planeDataMod = planeDataDF.withColumnRenamed("tailnum", "Tail_Number").withColumnRenamed("year", "ManYear")
    val planeDataDFMod = planeDataMod.withColumn("issueDate", to_timestamp(col("issue_date"), "MM/DD/YYYY"))
    val planeDataCombined = airlineDataDF.join(planeDataDFMod, Seq("Tail_Number"), "left_outer")

    planeDataCombined.write.mode(SaveMode.Overwrite)
      .partitionBy("Reporting_Airline")
      .parquet(AIRLINE_PLANE_DATA_PARQUET_PATH)
  }

  /**
    * Check the record count
    */
  test("Select count") {
    Assignment2.Problem0(readAirlineAndPlane.toDF()) must equal(302118)
  }

  /**
    * What is the percentage delay types by total delays?
    */
  test("percentage delay types by total delays") {
    val responseDF = Assignment2.Problem1(readAirlineAndPlane.toDF())


    val expectedData = Seq(
      Row("lateAircraftDelayCount", 46.95214F),
      Row("securityDelayCount", 0.23548368F),
      Row("nASDelayCount", 57.34647F),
      Row("weatherDelayCount", 7.3970795F),
      Row("carrierDelayCount", 49.906013F)
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
    val response = Assignment2.Problem2(readAirlineAndPlane.toDF())
    println(response.schema)
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

    assertSmallDataFrameEquality(expectedDF, response)

  }

  /**
    * Did privately managed airlines perform better than publicly traded ones?
    */
  test("public vs private airlines") {
    val result = Assignment2.Problem3(readAirlineAndPlane().toDF())
    result must equal((32300,16111))
  }

  /**
    * What delay type is most common at each airport?
    */
  test("Delay type common at each airport") {

    val response = Assignment2.Problem4(readAirlineAndPlane().toDF())


    println(response.schema)
    val expectedData = Seq(
      Row("MSY", 205L),
      Row("MSY", 7L),
      Row("MSY", 214L),
      Row("MSY", 271L)
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
    val result = Assignment2.Problem5(
              readAirlineAndPlane().toDF().filter(col("ManYear")
                .gt(lit("2000")) and col("ArrDel15")
                .notEqual(0)),
              readAirlineAndPlane().toDF().filter(col("ManYear")
                .lt(lit("2000")) and col("ArrDel15")
                .notEqual(0))
              )

    result must equal((5457,4176))

  }

  private def readAirlineAndPlane(): DataFrame = {
    spark.read.parquet(AIRLINE_PLANE_DATA_PARQUET_PATH)
  }


}
