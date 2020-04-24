package com.spark.assignment1

import java.util.Date

case class Airline(
                    Year: String,
                    Month: String,
                    Tail_Number: String,
                    Reporting_Airline: String,
                    FlightDate: String,
                    OriginState: String,
                    OriginStateName: String,
                    DestState: String,
                    DestStateName: String,
                    ArrDel15: Int,
                    DepDel15: Int,
                    CarrierDelay: Int,
                    WeatherDelay: Int,
                    NASDelay: Int,
                    SecurityDelay: Int,
                    LateAircraftDelay: Int
                  )
