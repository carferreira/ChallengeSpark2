package org.example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array_distinct, avg, col, collect_list, count, date_format, desc, explode, regexp_replace, row_number, split, to_timestamp, udf, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Main {

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

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "")
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Challenge")
      .getOrCreate()

    // Load files
    val gpsUserReviewsDF = loadFileDF(spark, googleplaystoreUserReviewsFile, googleplaystoreUserReviewsSchema)
    val gpsDF = loadFileDF(spark, googleplaystoreFile, googleplaystoreSchema)
    //gpsDF.show(5, false)
    //gpsDF.printSchema()
    //println(gpsDF.count())


    // Part 1
    val cleanGPSUserReviewsDF = gpsUserReviewsDF
      .na.fill(0, Seq("Sentiment_Polarity"))

    val df_1 = cleanGPSUserReviewsDF
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    //println("Part 1")
    //df_1.show(5, false)
    //df_1.printSchema()
    //println(df_1.count())


    // Part 2
    val cleanGPSDF = gpsDF
      .na.fill(0, Seq("Rating"))

    val df_2 = cleanGPSDF
      .filter(col("Rating") >= 4.0)
      .sort(desc("Rating"))

    //println("Part 2")
    //df_2.show(5, false)
    //println(df_2.count())
    //gpsDF.filter(col("App") === "AG Subway Simulator Pro").show(5, false)
    //cleanGPSDF.filter(col("App") === "AG Subway Simulator Pro").show(5, false)
    //df_2.filter(col("App") === "AG Subway Simulator Pro").show(5, false)

    //saveDF(df_2, "csv", "output/best_apps.csv", true, "§", false)


    // Part 3
    val partitionByApp = Window
      .partitionBy("App")

    val addRowNumberDF = gpsDF
      .withColumn("row_number", row_number().over(partitionByApp.orderBy(col("Reviews").desc)))

    val withoutAppDuplicatesDF = addRowNumberDF
      .filter(col("row_number") === 1)
      .drop("row_number", "Category")

    val categoriesDF = gpsDF
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
    //renameColumnsDF.show(5, false)

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

    val df_3 = formatAndroidVersionDF.select("App", "Categories", "Rating", "Reviews", "Size", "Installs", "Type",
      "Price", "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version")

    //println("Part 3")
    //df_3.show(5, false)
    //df_3.printSchema()
    //println(df_3.count())
    //gpsDF.filter(col("App") === "Market Update Helper").show(5, false)
    //df_3.filter(col("App") === "Market Update Helper").show(5, false)


    // Part 4
    val auxDF1 = df_1
      .withColumnRenamed("App", "AppRemove")

    val mergeDF3WithDF1 = df_3
      .join(auxDF1, df_3("App") === auxDF1("AppRemove"), "left_outer")
      .drop("AppRemove")

    //println("Part 4")
    //mergeDF3WithDF1.show(5, false)
    //mergeDF3WithDF1.filter(col("App") === "Basketball Stars").show(5, false)
    //mergeDF3WithDF1.printSchema()
    //println(mergeDF3WithDF1.count())

    //saveDF(mergeDF3WithDF1, "parquet", "output/googleplaystore_cleaned.parquet", compress = true)


    // Part 5
    val divideDF3 = mergeDF3WithDF1
      .withColumn("Genres", explode(col("Genres")))
      .select(col("App"), col("Genres"), col("Rating"), col("Average_Sentiment_Polarity"))
    //divideDF3.filter(col("App") === "Sandbox - Color by Number Coloring Pages").show(5, false)

    val df_4 = divideDF3.groupBy("Genres")
      .agg(
        count("App").alias("Number_of_Apps"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )
      .withColumnRenamed("Genres", "Genre")

    println("Part 5")
    df_4.show(5, false)
    df_4.printSchema()
    println(df_4.count())

    //saveDF(df_4, "parquet", "output/googleplaystore_metrics.parquet", compress = true)

    spark.stop()
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

