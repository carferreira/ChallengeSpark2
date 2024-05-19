package org.example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array_distinct, avg, col, collect_list, count, date_format, desc, explode, regexp_replace, row_number, split, to_timestamp, udf, when}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {

  /*
   * Assumption followed:
   *  - to load the files, schema and the DROPMALFORMED mode were used so that records with incorrect formats were
   *    discarded. This caused records in the googleplaystore_user_reviews.csv file that had a 'nan' value in the
   *    "Sentiment_Polarity" and "Sentiment_Subjectivity" columns and records in the googleplaystore.csv file with
   *    missing data to be discarded;
   *  - datatype timestamp was considered for the "Last Updated" (in googleplaystore.csv file) field instead of date
   *    so that it would have the time (00:00:00).
   */

  val googleplaystoreFile = "data/googleplaystore.csv"
  val googleplaystoreUserReviewsFile = "data/googleplaystore_user_reviews.csv"
  val googleplaystoreSchema = StructType(
    Array(
      StructField("App", StringType, nullable = true),
      StructField("Category", StringType, nullable = true),
      StructField("Rating", DoubleType, nullable = true),
      StructField("Reviews", LongType, nullable = true),
      StructField("Size", StringType, nullable = true),
      StructField("Installs", StringType, nullable = true),
      StructField("Type", StringType, nullable = true),
      StructField("Price", StringType, nullable = true),
      StructField("Content Rating", StringType, nullable = true),
      StructField("Genres", StringType, nullable = true),
      StructField("Last Updated", StringType, nullable = true),
      StructField("Current Ver", StringType, nullable = true),
      StructField("Android Ver", StringType, nullable = true)
    )
  )
  val googleplaystoreUserReviewsSchema = StructType (
    Array (
      StructField("App", StringType, nullable = true),
      StructField("Translated_Review", StringType, nullable = true),
      StructField("Sentiment", StringType, nullable = true),
      StructField("Sentiment_Polarity", DoubleType, nullable = true),
      StructField("Sentiment_Subjectivity", DoubleType, nullable = true)
    )
  )
  val bestAppsFile = "output/best_apps.csv"
  val cleanedFile = "output/googleplaystore_cleaned.parquet"
  val metricsFile = "output/googleplaystore_metrics.parquet"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-3.2.0")
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Challenge")
      .getOrCreate()

    // Load files
    val gpsUserReviewsDF = loadFileDF(spark, googleplaystoreUserReviewsFile, googleplaystoreUserReviewsSchema)
    val gpsDF = loadFileDF(spark, googleplaystoreFile, googleplaystoreSchema)

    // Part 1
    val df_1 = part1(gpsUserReviewsDF)
    println("Part 1")
    df_1.show(5, false)
    df_1.printSchema()
    println(df_1.count())

    // Part 2
    val df_2 = part2(gpsDF)
    //saveDF(df_2, "csv", bestAppsFile, true, "§", false)
    println("Part 2")
    df_2.show(5, false)
    println(df_2.count())

    // Part 3
    val df_3 = part3(gpsDF)
    println("Part 3")
    df_3.show(5, false)
    df_3.printSchema()
    println(df_3.count())

    // Part 4
    val mergeDF3WithDF1 = part4(df_1, df_3)
    //saveDF(mergeDF3WithDF1, "parquet", cleanedFile, compress = true)
    println("Part 4")
    mergeDF3WithDF1.show(5, false)
    mergeDF3WithDF1.printSchema()
    println(mergeDF3WithDF1.count())

    // Part 5
    val df_4 = part5(mergeDF3WithDF1)
    //saveDF(df_4, "parquet", metricsFile, compress = true)
    println("Part 5")
    df_4.show(5, false)
    df_4.printSchema()
    println(df_4.count())

    spark.stop()
  }

  // Part 1
  def part1(df: DataFrame): DataFrame = {
    val cleanGPSUserReviewsDF = df
      .na.fill(0, Seq("Sentiment_Polarity"))

    cleanGPSUserReviewsDF
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
  }

  // Part 2
  def part2(df: DataFrame): DataFrame = {
    val cleanGPSDF = df
      .na.fill(0, Seq("Rating"))

    cleanGPSDF
      .filter(col("Rating") >= 4.0)
      .sort(desc("Rating"))
  }

  // Part 3
  def part3(df: DataFrame): DataFrame = {
    val partitionByApp = Window
      .partitionBy("App")

    val addRowNumberDF = df
      .withColumn("row_number", row_number().over(partitionByApp.orderBy(col("Reviews").desc)))

    val withoutAppDuplicatesDF = addRowNumberDF
      .filter(col("row_number") === 1)
      .drop("row_number", "Category")

    val categoriesDF = df
      .select("App", "Category")
      .withColumn("Categories", array_distinct(collect_list("Category").over(partitionByApp)))
      .dropDuplicates("App")
      .withColumnRenamed("App", "AppRemove")
      .drop("Category")

    val joinDF = withoutAppDuplicatesDF
      .join(categoriesDF, withoutAppDuplicatesDF("App") === categoriesDF("AppRemove"), "left_outer")
      .drop("AppRemove")

    val renameColumnsDF = joinDF
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")

    // Format Rating
    val formatRatingDF = renameColumnsDF
      .withColumn("Rating", when(col("Rating") === "NaN", null).otherwise(col("Rating")))

    // Format Reviews
    val formatReviewsDF = formatRatingDF
      .na.fill(0, Seq("Reviews"))

    // Format Size
    val sizeToMBUDF = udf((size: String) => sizeToMB(size).getOrElse(null.asInstanceOf[Double]))

    val formatSizeDF = formatReviewsDF
      .withColumn("size_mb", sizeToMBUDF(col("Size")).cast("double"))
      .withColumn("size_mb2", when(col("size_mb") === 0.0, null).otherwise(col("size_mb")))
      .drop("Size", "size_mb")
      .withColumnRenamed("size_mb2", "Size")

    // Format Installs
    val formatInstallsDF = formatSizeDF
      .withColumn("Installs", when(col("Installs") === "NaN", null).otherwise(col("Installs")))

    // Format Type
    val formatTypeDF = formatInstallsDF
      .withColumn("Type", when(col("Type") === "NaN", null).otherwise(col("Type")))

    // Format Price
    val formatPriceDF = formatTypeDF
      .withColumn("Price", regexp_replace(col("Price"), "\\$", "").cast("double") * 0.9)
      .withColumn("Price", when(col("Price") === "NaN", null).otherwise(col("Price")))

    // Format Content Rating
    val formatContentRatingDF = formatPriceDF
      .withColumn("Content_Rating", when(col("Content_Rating") === "NaN", null).otherwise(col("Content_Rating")))

    // Format Genres
    val formatGenresDF = formatContentRatingDF
      .withColumn("Genres", split(col("Genres"), ";"))

    // Format Last Updated
    val formatLastUpdatedDF = formatGenresDF
      .withColumn("Last_Updated", date_format(to_timestamp(col("Last_Updated"), "MMMM d, yyyy"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

    // Format Current Version
    val formatCurrentVersionDF = formatLastUpdatedDF
      .withColumn("Current_Version", when(col("Current_Version") === "NaN", null).otherwise(col("Current_Version")))

    // Format Minimum Android Version
    val formatAndroidVersionDF = formatCurrentVersionDF
      .withColumn("Minimum_Android_Version", when(col("Minimum_Android_Version") === "NaN", null).otherwise(col("Minimum_Android_Version")))

    formatAndroidVersionDF.select("App", "Categories", "Rating", "Reviews", "Size", "Installs", "Type",
      "Price", "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version")
  }

  // Part 4
  def part4(df1: DataFrame, df3: DataFrame): DataFrame = {
    val auxDF1 = df1
      .withColumnRenamed("App", "AppRemove")

    df3
      .join(auxDF1, df3("App") === auxDF1("AppRemove"), "left_outer")
      .drop("AppRemove")
  }

  // Part 5
  def part5(df: DataFrame): DataFrame = {
    val divideDF3 = df
      .withColumn("Genres", explode(col("Genres")))
      .select(col("App"), col("Genres"), col("Rating"), col("Average_Sentiment_Polarity"))

    divideDF3.groupBy("Genres")
      .agg(
        count("App").alias("Number_of_Apps"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )
      .withColumnRenamed("Genres", "Genre")
  }

  // Load file to a dataframe
  def loadFileDF(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    spark.read
      .schema(schema)
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("mode", "DROPMALFORMED")
      .csv(path)
  }

  // Save file
  def saveDF(df: DataFrame, format: String, path: String, header: Boolean = false, delimiter: String = ",", compress: Boolean = false): Unit = {
    val writeDF = df.write.format(format)
    if (format.equals("csv")) {
      writeDF.option("header", header).option("delimiter", delimiter)
    }
    if (compress) {
      writeDF.option("compression", "gzip")
    }
    writeDF.mode("overwrite").save(path)
  }

  // Convert size
  def sizeToMB(size: String): Option[Double] = {
    try {
      val numericPart = size.dropRight(1)
      val unit = size.last.toLower
      val sizeInBytes = unit match {
        case 'k' => numericPart.toDouble * 1024
        case 'm' => numericPart.toDouble * 1024 * 1024
        case 'g' => numericPart.toDouble * 1024 * 1024 * 1024
        case _ => return None
      }
      Some(sizeInBytes / (1024 * 1024))
    } catch {
      case _: Throwable => None
    }
  }


}

