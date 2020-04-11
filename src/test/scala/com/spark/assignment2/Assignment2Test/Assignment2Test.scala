package com.spark.assignment2.Assignment2Test

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import scala.concurrent.duration._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import com.spark.assignment1.Airline
import com.spark.assignment1.Assignment2

class Assignment2Test extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  val AIRLINE_DATA_CSV_PATH = "data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2019_1.csv"
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

  implicit val tripEncoder: Encoder[Airline] = Encoders.product[Airline]

  def airlineDataDS: Dataset[Airline] = spark.read.options(csvReadOptions).csv(AIRLINE_DATA_CSV_PATH).as[Airline]
  def airlineDataDF: DataFrame = airlineDataDS.toDF()

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
    Assignment2.Problem1(airlineDataDF) must equal(583985)
  }

  test("average, min, max delays") {
    Assignment2.Problem2(airlineDataDF) must equal(13)
  }

}
