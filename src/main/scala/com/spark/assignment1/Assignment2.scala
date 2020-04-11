package com.spark.assignment1

import org.apache.spark.sql.DataFrame

object Assignment2 {

  def Problem1(airlineData: DataFrame): Long = {

    println(airlineData.count())
    return airlineData.count()
  }

  def Problem2(airlineData: DataFrame): Long = {
    val data =  airlineData.groupBy("ArrDel15").avg()
    data.describe("ArrDel15").show()

    return 1
  }
}
