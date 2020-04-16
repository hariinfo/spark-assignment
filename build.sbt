ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "com.spark.example"
ThisBuild / organizationName := "example"

val sparkVersion = "2.4.5"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1"
lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion


libraryDependencies ++= Seq(scalaTest % Test, sparkCore, sparkSql)

libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.12"
