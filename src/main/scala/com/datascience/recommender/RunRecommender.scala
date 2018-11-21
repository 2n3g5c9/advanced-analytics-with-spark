package com.datascience.recommender
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * RunRecommender entry point.
  *
  * @author Marc Molina
  */
object RunRecommender {
  val TITLE: String = "RECOMMENDING MUSIC AND THE AUDIOSCROBBLER DATA SET"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    println(TITLE + "\n" + "-" * TITLE.length)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Recommender")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    spark.sparkContext.setCheckpointDir("file:///tmp/")

    val base: String                       = "./data/recommender/"
    val rawUserArtistData: Dataset[String] = spark.read.textFile(base + "user_artist_data.txt")
    val rawArtistData: Dataset[String]     = spark.read.textFile(base + "artist_data.txt")
    val rawArtistAlias: Dataset[String]    = spark.read.textFile(base + "artist_alias.txt")

    val runRecommender = new RunRecommender(spark)
    runRecommender.preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
    runRecommender.model(rawUserArtistData, rawArtistData, rawArtistAlias)
    runRecommender.evaluate(rawUserArtistData, rawArtistAlias)
    runRecommender.recommend(rawUserArtistData, rawArtistData, rawArtistAlias)

    spark.stop()
  }
}

class RunRecommender(private val spark: SparkSession) {
  import spark.implicits._

  def preparation(rawUserArtistData: Dataset[String],
                  rawArtistData: Dataset[String],
                  rawArtistAlias: Dataset[String]): Unit = {
    val userArtistDF: DataFrame = rawUserArtistData
      .map { line =>
        val Array(user, artist, _*): Array[String] = line.split(' ')
        (user.toInt, artist.toInt)
      }
      .toDF("user", "artist")

    println("SIMPLE STATISTICS ON USER/ARTIST DATA:")
    userArtistDF.agg(min("user"), max("user"), min("artist"), max("artist")).show()

    val artistByID: DataFrame      = buildArtistByID(rawArtistData)
    val artistAlias: Map[Int, Int] = buildArtistAlias(rawArtistAlias)

    val (badID, goodID): (Int, Int) = artistAlias.head
    println("EXAMPLE OF AMBIGUITY ON ARTIST ID:")
    artistByID.filter($"id" isin (badID, goodID)).show()
  }

  def model(rawUserArtistData: Dataset[String],
            rawArtistData: Dataset[String],
            rawArtistAlias: Dataset[String]): Unit = {

    val bArtistAlias: Broadcast[Map[Int, Int]] =
      spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))

    val trainData: DataFrame = buildCounts(rawUserArtistData, bArtistAlias).cache()

    val model: ALSModel = new ALS()
      .setSeed(Random.nextLong())
      .setImplicitPrefs(true)
      .setRank(10)
      .setRegParam(0.01)
      .setAlpha(1.0)
      .setMaxIter(5)
      .setUserCol("user")
      .setItemCol("artist")
      .setRatingCol("count")
      .setPredictionCol("prediction")
      .fit(trainData)

    trainData.unpersist()

    val userID: Int = 2093760

    val existingArtistIDs = trainData.filter($"user" === userID).select("artist").as[Int].collect()

    val artistByID = buildArtistByID(rawArtistData)

    println("TOP 5 RECOMMENDATIONS FOR USER " + userID + ":")
    artistByID.filter($"id" isin (existingArtistIDs: _*)).show()

    println("AND THE ASSOCIATED PREDICTION SCORES:")
    val topRecommendations = makeRecommendations(model, userID, 5)
    topRecommendations.show()

    val recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()

    println("AND THE ARTIST NAMES:")
    artistByID.filter($"id" isin (recommendedArtistIDs: _*)).show()

    model.userFactors.unpersist()
    model.itemFactors.unpersist()
  }

  def evaluate(rawUserArtistData: Dataset[String], rawArtistAlias: Dataset[String]): Unit = {
    val bArtistAlias: Broadcast[Map[Int, Int]] =
      spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))

    val allData: DataFrame                            = buildCounts(rawUserArtistData, bArtistAlias)
    val Array(trainData, cvData): Array[Dataset[Row]] = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    val allArtistIDs: Array[Int]             = allData.select("artist").as[Int].distinct().collect()
    val bAllArtistIDs: Broadcast[Array[Int]] = spark.sparkContext.broadcast(allArtistIDs)

    val mostListenedAUC: Double =
      areaUnderCurve(cvData, bAllArtistIDs, predictMostListened(trainData))
    println("MOST LISTENED AUC: " + mostListenedAUC)

    val evaluations: Seq[(Double, (Int, Double, Double))] =
      for (rank     <- Seq(5, 30);
           regParam <- Seq(1.0, 0.0001);
           alpha    <- Seq(1.0, 40.0))
        yield {
          val model: ALSModel = new ALS()
            .setSeed(Random.nextLong())
            .setImplicitPrefs(true)
            .setRank(rank)
            .setRegParam(regParam)
            .setAlpha(alpha)
            .setMaxIter(20)
            .setUserCol("user")
            .setItemCol("artist")
            .setRatingCol("count")
            .setPredictionCol("prediction")
            .fit(trainData)

          val auc: Double = areaUnderCurve(cvData, bAllArtistIDs, model.transform)

          model.userFactors.unpersist()
          model.itemFactors.unpersist()

          (auc, (rank, regParam, alpha))
        }

    println("SORTED AUC:")
    evaluations.sorted.reverse.foreach(println)

    trainData.unpersist()
    cvData.unpersist()
  }

  def recommend(rawUserArtistData: Dataset[String],
                rawArtistData: Dataset[String],
                rawArtistAlias: Dataset[String]): Unit = {

    val bArtistAlias: Broadcast[Map[Int, Int]] =
      spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))
    val allData: DataFrame = buildCounts(rawUserArtistData, bArtistAlias).cache()
    val model: ALSModel = new ALS()
      .setSeed(Random.nextLong())
      .setImplicitPrefs(true)
      .setRank(10)
      .setRegParam(1.0)
      .setAlpha(40.0)
      .setMaxIter(20)
      .setUserCol("user")
      .setItemCol("artist")
      .setRatingCol("count")
      .setPredictionCol("prediction")
      .fit(allData)
    allData.unpersist()

    val userID: Int                   = 2093760
    val topRecommendations: DataFrame = makeRecommendations(model, userID, 5)

    val recommendedArtistIDs: Array[Int] = topRecommendations.select("artist").as[Int].collect()
    val artistByID: DataFrame            = buildArtistByID(rawArtistData)

    println("TOP 5 BEST RECOMMENDATIONS FOR USER " + userID + ":")
    artistByID
      .join(spark.createDataset(recommendedArtistIDs).toDF("id"), "id")
      .select("name")
      .show()

    model.userFactors.unpersist()
    model.itemFactors.unpersist()
  }

  def buildArtistByID(rawArtistData: Dataset[String]): DataFrame = {
    rawArtistData
      .flatMap { line =>
        val (id, name): (String, String) = line.span(_ != '\t')
        if (name.isEmpty) {
          None
        } else {
          try {
            Some((id.toInt, name.trim))
          } catch {
            case _: NumberFormatException => None
          }
        }
      }
      .toDF("id", "name")
  }

  def buildArtistAlias(rawArtistAlias: Dataset[String]): Map[Int, Int] = {
    rawArtistAlias
      .flatMap { line =>
        val Array(artist, alias): Array[String] = line.split('\t')
        if (artist.isEmpty) {
          None
        } else {
          Some((artist.toInt, alias.toInt))
        }
      }
      .collect()
      .toMap
  }

  def buildCounts(rawUserArtistData: Dataset[String],
                  bArtistAlias: Broadcast[Map[Int, Int]]): DataFrame = {
    rawUserArtistData
      .map { line =>
        val Array(userID, artistID, count): Array[Int] = line.split(' ').map(_.toInt)
        val finalArtistID: Int                         = bArtistAlias.value.getOrElse(artistID, artistID)
        (userID, finalArtistID, count)
      }
      .toDF("user", "artist", "count")
  }

  def makeRecommendations(model: ALSModel, userID: Int, howMany: Int): DataFrame = {
    val toRecommend: DataFrame =
      model.itemFactors.select($"id".as("artist")).withColumn("user", lit(userID))
    model
      .transform(toRecommend)
      .select("artist", "prediction")
      .orderBy($"prediction".desc)
      .limit(howMany)
  }

  def areaUnderCurve(positiveData: DataFrame,
                     bAllArtistIDs: Broadcast[Array[Int]],
                     predictFunction: DataFrame => DataFrame): Double = {
    val positivePredictions: DataFrame = predictFunction(positiveData.select("user", "artist"))
      .withColumnRenamed("prediction", "positivePrediction")

    val negativeData: DataFrame = positiveData
      .select("user", "artist")
      .as[(Int, Int)]
      .groupByKey { case (user, _) => user }
      .flatMapGroups {
        case (userID, userIDAndPosArtistIDs) =>
          val random: Random = new Random()
          val posItemIDSet
            : Set[Int]                   = userIDAndPosArtistIDs.map { case (_, artist) => artist }.toSet
          val negative: ArrayBuffer[Int] = new ArrayBuffer[Int]()
          val allArtistIDs: Array[Int]   = bAllArtistIDs.value
          var i: Int                     = 0
          while (i < allArtistIDs.length && negative.size < posItemIDSet.size) {
            val artistID: Int = allArtistIDs(random.nextInt(allArtistIDs.length))
            if (!posItemIDSet.contains(artistID)) {
              negative += artistID
            }
            i += 1
          }
          negative.map(artistID => (userID, artistID))
      }
      .toDF("user", "artist")

    val negativePredictions: DataFrame =
      predictFunction(negativeData).withColumnRenamed("prediction", "negativePrediction")

    val joinedPredictions: DataFrame = positivePredictions
      .join(negativePredictions, "user")
      .select("user", "positivePrediction", "negativePrediction")
      .cache()

    val allCounts: DataFrame =
      joinedPredictions.groupBy("user").agg(count(lit("1")).as("total")).select("user", "total")

    val correctCounts: DataFrame = joinedPredictions
      .filter($"positivePrediction" > $"negativePrediction")
      .groupBy("user")
      .agg(count("user").as("correct"))
      .select("user", "correct")

    val meanAUC: Double = allCounts
      .join(correctCounts, Seq("user"), "left_outer")
      .select($"user", (coalesce($"correct", lit(0)) / $"total").as("auc"))
      .agg(mean("auc"))
      .as[Double]
      .first()

    joinedPredictions.unpersist()

    meanAUC
  }

  def predictMostListened(train: DataFrame)(allData: DataFrame): DataFrame = {
    val listenCounts: DataFrame =
      train.groupBy("artist").agg(sum("count").as("prediction")).select("artist", "prediction")
    allData.join(listenCounts, Seq("artist"), "left_outer").select("user", "artist", "prediction")
  }
}
