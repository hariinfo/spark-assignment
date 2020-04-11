package com.spark.assignment1

import org.apache.spark.sql.DataFrame

object Assignment2 {

  def Problem1(usConfirmedData: DataFrame): Long = {

    println(usConfirmedData.count())
    return usConfirmedData.count()
  }


}
