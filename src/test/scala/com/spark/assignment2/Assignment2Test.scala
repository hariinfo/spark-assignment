package com.spark.assignment2

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import scala.concurrent.duration._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import com.spark.assignment1.Airline
import com.spark.assignment1.Carrier
import com.spark.assignment1.Plane
import com.spark.assignment1.Assignment2

class Assignment2Test extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  val AIRLINE_DATA_CSV_PATH = "data/airline_performance.csv"
  val CARRIERS_DATA_CSV_PATH = "data/carrier.csv"
  val PLANE_DATA_CSV_PATH = "data/plane-data.csv"

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

  implicit val airlineEncoder: Encoder[Airline] = Encoders.product[Airline]
  implicit val carrierEncoder: Encoder[Carrier] = Encoders.product[Carrier]
  implicit val planeEncoder: Encoder[Plane] = Encoders.product[Plane]

  def airlineDS: Dataset[Airline] = spark.read.options(csvReadOptions).csv(AIRLINE_DATA_CSV_PATH).as[Airline]
  def airlineDataDF: DataFrame = airlineDS.toDF()

  def carrierDS: Dataset[Carrier] = spark.read.options(csvReadOptions).csv(CARRIERS_DATA_CSV_PATH).as[Carrier]
  def carrierDataDF: DataFrame = carrierDS.toDF()

  def planeDS: Dataset[Plane] = spark.read.options(csvReadOptions).csv(PLANE_DATA_CSV_PATH).as[Plane]
  def planeDataDF: DataFrame = planeDS.toDF()

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

  /**
    *
    */
  test("Select count") {
    Assignment2.Problem0(airlineDataDF) must equal(149033)
  }

  /**
    * What is the percentage delay types by total delays?
    */
  test("percentage delay types by total delays") {
    val result = Assignment2.Problem1(airlineDataDF).toSeq
    println(result)
    result.length must equal (5)
    result must contain (("lateAircraftDelayCount",48.255596))
    result must contain (("securityDelayCount",0.31032535))
    result must contain (("weatherDelayCount",5.110965))
    result must contain (("carrierDelayCount",52.115856))
    result must contain (("securityDelayCount",0.31032535))
  }

  /**
    * What is the min/max/average delays for an airline in a month and year?
    */
  test("min/max/average delays for an airline in a month and year") {
    val result = Assignment2.Problem2(airlineDataDF)
  }

  /**
    * Did privately managed airlines perform better than publicly traded ones?
    */
  test("public vs private airlines") {
    val result = Assignment2.Problem3(airlineDataDF)
  }

  /**
    * What delay type is most common at each airport?
    */
  test("Did airlines with modernized fleet perform better") {
    val result = Assignment2.Problem4(airlineDataDF)
  }

  /**
    *Did airlines with modernized fleet perform better?
    */
  test("Did airlines with modernized fleet perform better") {
    val result = Assignment2.Problem5(airlineDataDF)
  }

}
