package org.codepot.spark.jobs

import org.codepot.spark.{BatchJobConfig, SparkJob, SparkStarter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class AverageRating(master: String, jobConfig: AverageRatingConfig) extends SparkJob(master) {

  def runJob(sc: SparkContext): Unit = {
    val movies: RDD[Movie] = sc.textFile(jobConfig.moviesDirectory).map(toMovie)
    val ratings: RDD[Rating] = sc.textFile(jobConfig.ratingsDirectory).map(toRating)

    val averageRatings: RDD[(Int, Double)] = ratings.groupBy(_.movieId).mapValues(ratings => ratings.map(_.rating).sum.toDouble / ratings.size)

    val joined: RDD[(Movie, Double)] = movies.keyBy(_.id).join(averageRatings).values

    joined.map { case (Movie(_,title), rating) => s"$title::$rating" }
      .saveAsTextFile(jobConfig.outputDirectory)
  }

  private def toRating(row: String): Rating = {
    val tokens = row.split("::")
    Rating(tokens(1).toInt,tokens(2).toInt)
  }

  private def toMovie(row: String): Movie = {
    val tokens = row.split("::")
    Movie(tokens(0).toInt, tokens(1).dropRight(7))
  }

  private case class Movie(id: Int, title: String)
  private case class Rating(movieId: Int, rating: Int)

}


case class AverageRatingConfig(aggregationLevel: Char, moviesDirectory: String, ratingsDirectory: String, outputDirectory: String) extends BatchJobConfig

object AverageRatingConfig {
  def apply(args: Seq[String]): AverageRatingConfig = args match {
    case Seq(aggregationLevel, moviesDirectory, ratingsDirectory, outputDirectory) => AverageRatingConfig(aggregationLevel.charAt(0), moviesDirectory, ratingsDirectory, outputDirectory)
  }
}

object AverageRatingJob {

  def main(args: Array[String]): Unit = {
    SparkStarter.start("AverageRating job", AverageRating.apply, AverageRatingConfig.apply, args)
  }
}