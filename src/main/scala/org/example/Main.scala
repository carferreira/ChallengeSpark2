package org.example

import org.apache.spark.sql.functions.{avg, col, desc}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Main {

  val googleplaystoreFile = "data/googleplaystore.csv"
  val googleplaystoreUserReviewsFile = "data/googleplaystore_user_reviews.csv"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "")
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Challenge")
      .getOrCreate()

    // Load files
    val gpsUserReviewsDF = loadFileDF(spark, googleplaystoreUserReviewsFile)
    val gpsDF = loadFileDF(spark, googleplaystoreFile)
    gpsDF.show(5, false)

    // Part 1
    val cleanGPSUserReviewsDF = gpsUserReviewsDF
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double"))
      .na.fill(0, Seq("Sentiment_Polarity"))

    val df_1 = cleanGPSUserReviewsDF
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
    df_1.show(5, false)


    // Part 2
    val cleanGPSDF = gpsDF
      .withColumn("Rating", col("Rating").cast("double"))
      .na.fill(0, Seq("Rating"))
//    val cleanGPSDF = cleanDF(gpsDF, "Rating", "Double", 0.0)
    val df_2 = cleanGPSDF
      .filter(col("Rating") >= 4.0)
      .sort(desc("Rating"))
    df_2.show(5, false)

//    saveDF(df_2, "output/best_apps.csv")


    spark.stop()
  }

  // Load file to a dataframe
  def loadFileDF(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("mode", "DROPMALFORMED")
      .csv(path)
  }

  def saveDF(df: DataFrame, filename: String): Unit = {
    df.write
      .option("header", "true")
      .option("delimiter", "§")
      .mode(SaveMode.Overwrite)
      .csv(filename)
  }

  // Pensar se faz sentido
  def cleanDF(df: DataFrame, column: String, cast: String, value: Double): DataFrame = {
    df.withColumn(column, col(column).cast(cast))
      .na.fill(value, Seq(column))
  }


}