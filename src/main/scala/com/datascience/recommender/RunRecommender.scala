package com.datascience.recommender
import com.datascience.recommender.RunRecommenderTypes._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  * RunRecommender entry point.
  *
  * @author Marc Molina
  */
object RunRecommender {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Recommender")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val base: String = "./data/recommender/"
    val rawUserArtistData: sql.DataFrame = spark.read
      .format("csv")
      .option("delimiter", " ")
      .schema(RawUserArtistData)
      .load(base + "user_artist_data.txt")
    val rawArtistData: sql.DataFrame = spark.read
      .format("csv")
      .option("delimiter", "/t")
      .schema(RawArtistData)
      .load(base + "artist_data.txt")
    val rawArtistAlias: sql.DataFrame = spark.read
      .format("csv")
      .option("delimiter", "/t")
      .schema(RawArtistAlias)
      .load(base + "artist_alias.txt")

    rawUserArtistData.show()

    spark.stop()
  }
}
