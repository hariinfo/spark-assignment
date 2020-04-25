package com.spark.assignment1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Sample {
  val PLANE_DATA_CSV_PATH = "data/test.csv"
  val AIRLINE_PLANE_DATA_PARQUET_PATH = "airline_and_plane/joined.parquet"

  /**
    *
    * Create a SparkSession that runs locally on our laptop.
    */
  val spark =
    SparkSession
      .builder()
      .appName("Assignment 2")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .config("spark.executor.instance", "3")
      .config("spark.sql.parquet.filterPushdown", true)
      .getOrCreate()

  def main(args: Array[String]) = {
    import spark.sqlContext.implicits._
    def planeDataDF: DataFrame =
      spark.read
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", "MM/DD/YYYY")
        .option("mode", "DROPMALFORMED")
        .csv(PLANE_DATA_CSV_PATH)
    println(planeDataDF.schema)

    val planeDataDF1 = planeDataDF.withColumn("date", to_date(col("issue_date"), "YYYY"))
    planeDataDF1.show()

    val pivoted = planeDataDF1.groupBy("date").pivot("manufacturer").sum()
    pivoted.where("date > '1999'").show()

    planeDataDF.filter(col("year").gt(lit("1997")) and col("ArrDel15").notEqual(0)).show()
    planeDataDF.filter(col("year").lt(lit("1996")) and col("ArrDel15").notEqual(0)).show()
    //    planeDataDF.write
//        .partitionBy("issue_date")
//      .parquet("plane_data")

    /*
    spark.read.parquet(AIRLINE_PLANE_DATA_PARQUET_PATH).schema

    spark.read.parquet(AIRLINE_PLANE_DATA_PARQUET_PATH).toDF().filter(col("issueDate")
      .gt(lit("01/01/1999")) and col("ArrDel15")
      .notEqual(0)).show()

    spark.read.parquet(AIRLINE_PLANE_DATA_PARQUET_PATH).toDF().filter(col("issueDate")
      .lt(lit("01/01/1996")) and col("ArrDel15")
      .notEqual(0)).show()

   */
  }

}
