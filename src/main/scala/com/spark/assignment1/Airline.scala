package com.spark.assignment1

import java.util.Date

case class Airline(
                    Year: Long,
                    Month: Long,
                    Reporting_Airline: String,
                    FlightDate: String,
                    OriginState: String,
                    OriginStateName: String,
                    DestState: String,
                    DestStateName: String,
                    ArrDel15: Double,
                    DepDel15: Double,
                    CarrierDelay: Int,
                    WeatherDelay: Int,
                    NASDelay: Int,
                    SecurityDelay: Int,
                    LateAircraftDelay: Int
                  )
