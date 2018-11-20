package com.datascience.recommender

import org.apache.spark.sql.types._

/**
  * Data types for RunRecommender.
  *
  * @author Marc Molina
  */
object RunRecommenderTypes {
  val RawUserArtistData: StructType = StructType(
    Array(StructField("userid", DoubleType),
          StructField("artistid", DoubleType),
          StructField("playcount", IntegerType)))

  val RawArtistData: StructType = StructType(
    Array(StructField("artistid", DoubleType), StructField("artistname", StringType)))

  val RawArtistAlias: StructType = StructType(
    Array(StructField("badid", DoubleType), StructField("goodid", DoubleType)))
}
