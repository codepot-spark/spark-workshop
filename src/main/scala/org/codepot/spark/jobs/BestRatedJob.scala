package org.codepot.spark.jobs

import org.codepot.spark.{BatchJobConfig, SparkJob, SparkStarter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class BestRated(master: String, jobConfig: BestRatedConfig) extends SparkJob(master) {

  private def toGenderRating(input: (Rating, UserGender)) = input match {
    case (Rating(movieId,_,rating), UserGender(_, gender)) => GenderRating(movieId, gender, rating)
  }

  private def canBeBestRated(movieRatings: Iterable[GenderRating]) = {
    movieRatings.count(_.userGender == 'F') >= jobConfig.minRatingsFromEachGender && movieRatings.count(_.userGender == 'M') >= jobConfig.minRatingsFromEachGender
  }

  def runJob(sc: SparkContext): Unit = {
    val movies: RDD[Movie] = sc.textFile(jobConfig.moviesDirectory).map(toMovie)
    val ratings: RDD[Rating] = sc.textFile(jobConfig.ratingsDirectory).map(toRating)
    val userGender: RDD[UserGender] = sc.textFile(jobConfig.usersDirectory).map(toUserGender)

    val ratingsWithGender: RDD[GenderRating] = ratings.keyBy(_.userId).join(userGender.keyBy(_.userId)).values.map(toGenderRating)

    val averageRatings: RDD[(Int, Double)] = ratingsWithGender
      .groupBy(_.movieId)
      .filter(grouppedRatings => canBeBestRated(grouppedRatings._2))
      .mapValues(ratings => ratings.map(_.rating).sum.toDouble / ratings.size)

    val joined: RDD[(Movie, Double)] = movies.keyBy(_.id).join(averageRatings).values

    sc.parallelize(joined.takeOrdered(jobConfig.howMany)(Ordering.by(-_._2))).map { case (Movie(_,title), rating) => s"$title::$rating" }
      .saveAsTextFile(jobConfig.outputDirectory)
  }

  private def toRating(row: String): Rating = {
    val tokens = row.split("::")
    Rating(tokens(1).toInt,tokens(0).toInt, tokens(2).toInt)
  }

  private def toMovie(row: String): Movie = {
    val tokens = row.split("::")
    Movie(tokens(0).toInt, tokens(1).dropRight(7))
  }

  private def toUserGender(row: String): UserGender = {
    val tokens = row.split("::")
    UserGender(tokens(0).toInt, tokens(1).charAt(0))
  }

  private case class Movie(id: Int, title: String)
  private case class Rating(movieId: Int, userId: Int, rating: Int)
  private case class UserGender(userId: Int, gender: Char)
  private case class GenderRating(movieId: Int, userGender: Char, rating: Int)
}


case class BestRatedConfig(howMany: Int, minRatingsFromEachGender: Int, moviesDirectory: String, ratingsDirectory: String, usersDirectory: String, outputDirectory: String) extends BatchJobConfig

object BestRatedConfig {
  def apply(args: Seq[String]): BestRatedConfig = args match {
    case Seq(howMany, minRatingsFromEachGender, moviesDirectory, ratingsDirectory, usersDirectory, outputDirectory) =>
      BestRatedConfig(howMany.toInt, minRatingsFromEachGender.toInt, moviesDirectory, ratingsDirectory, usersDirectory, outputDirectory)
  }
}

object BestRatedJob {

  def main(args: Array[String]): Unit = {
    SparkStarter.start("AverageRating job", BestRated.apply, BestRatedConfig.apply, args)
  }
}