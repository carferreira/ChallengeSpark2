package org.example

import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{avg, col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {

  val googleplaystoreFile = "data/googleplaystore.csv"
  val googleplaystoreUserReviewsFile = "data/googleplaystore_user_reviews.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Challenge")
      .getOrCreate()

    // Load files
    val gpsUserReviewsDF = loadFileDF(spark, googleplaystoreUserReviewsFile)
//    gpsUserReviewsDF.show(5, false)

    // Part 1
    val cleanGPSUserReviewsDF = gpsUserReviewsDF
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double"))
      .na.fill(0, Seq("Sentiment_Polarity"))

    val df_1 = cleanGPSUserReviewsDF
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
    df_1.show(5, false)


    spark.stop()
  }

  // Load file to a dataframe
  def loadFileDF(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(path)
  }



}