package org.example

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Challenge")
      .getOrCreate()

    println("Printing Spark Session Variables: ")
    println("App Name: " + spark.sparkContext.appName)
    println("Deployment Mode: " + spark.sparkContext.deployMode)
    println("Master: " + spark.sparkContext.master)

    spark.stop()
  }
}