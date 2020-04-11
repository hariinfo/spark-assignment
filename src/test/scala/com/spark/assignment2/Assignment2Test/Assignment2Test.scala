package com.spark.assignment2.Assignment2Test

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import scala.concurrent.duration._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import com.spark.assignment1.US
import com.spark.assignment1.Assignment2

class Assignment2Test extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  val US_CONFIRMED_DATA_CSV_PATH = "data/time_series_covid19_confirmed_US.csv"
  val US_DEATH_DATA_CSV_PATH = "data/time_series_covid19_deaths_US.csv"
  val GLOBAL_CONFIRMED_DATA_CSV_PATH = "data/time_series_covid19_confirmed_global.csv"
  val GLOBAL_DEATH_DATA_CSV_PATH = "data/time_series_covid19_deaths_global.csv"

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

  implicit val tripEncoder: Encoder[US] = Encoders.product[US]

  def usConfirmedDS: Dataset[US] = spark.read.options(csvReadOptions).csv(US_CONFIRMED_DATA_CSV_PATH).as[US]
  def usConfirmedDataDF: DataFrame = usConfirmedDS.toDF()

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
    Assignment2.Problem1(usConfirmedDataDF) must equal(3253)
  }



}
