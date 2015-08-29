package org.codepot.spark.jobs

import org.codepot.spark.{BatchJobConfig, SparkJob, SparkStarter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.reflect.io.Directory

case class BestMoviesForGender(master: String, jobConfig: BestMoviesForGenderConfig) extends SparkJob(master) {

  private def toGenderRating(input: (Rating, UserGender)) = input match {
    case (Rating(movieId,_,rating), UserGender(_, gender)) => GenderRating(movieId, gender, rating)
  }

  private def canBeBestMoviesForGender(movieRatings: Iterable[GenderRating]) = {
    movieRatings.count(_.userGender == 'F') >= jobConfig.minRatingsFromEachGender && movieRatings.count(_.userGender == 'M') >= jobConfig.minRatingsFromEachGender
  }

  private def average(ratings: Iterable[GenderRating]) = {
    ratings.map(_.rating).sum.toDouble / ratings.size
  }

  private def toGenderDifferenceRating(movieRatings: Iterable[GenderRating]) = {
    val (femaleRatings, maleRatings) = movieRatings.partition(_.userGender == 'F')
    average(femaleRatings) - average(maleRatings)
  }

  def save(movies: RDD[Movie], topGenderMovies: RDD[(Int, Double)], directory: String) = {
    movies.keyBy(_.id).join(topGenderMovies).values.map { case (Movie(_,title), rating) => s"$title::$rating" }
      .saveAsTextFile(jobConfig.outputDirectory + directory)
  }

  def runJob(sc: SparkContext): Unit = {
    val movies: RDD[Movie] = sc.textFile(jobConfig.moviesDirectory).map(toMovie)
    val ratings: RDD[Rating] = sc.textFile(jobConfig.ratingsDirectory).map(toRating)
    val userGender: RDD[UserGender] = sc.textFile(jobConfig.usersDirectory).map(toUserGender)

    val ratingsWithGender: RDD[GenderRating] = ratings.keyBy(_.userId).join(userGender.keyBy(_.userId)).values.map(toGenderRating)

    val averageBothGenderRatings: RDD[(Int, Double)] = ratingsWithGender
      .groupBy(_.movieId)
      .filter(grouppedRatings => canBeBestMoviesForGender(grouppedRatings._2))
      .mapValues(toGenderDifferenceRating)

    val topFemaleMovies = sc.parallelize(averageBothGenderRatings.takeOrdered(jobConfig.howMany)(Ordering.by(-_._2)))
    val topMaleMovies = sc.parallelize(averageBothGenderRatings.takeOrdered(jobConfig.howMany)(Ordering.by(_._2))).mapValues(-_)

    save(movies, topFemaleMovies,"/female")
    save(movies, topMaleMovies,"/male")
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


case class BestMoviesForGenderConfig(howMany: Int, minRatingsFromEachGender: Int, moviesDirectory: String, ratingsDirectory: String, usersDirectory: String, outputDirectory: String) extends BatchJobConfig

object BestMoviesForGenderConfig {
  def apply(args: Seq[String]): BestMoviesForGenderConfig = args match {
    case Seq(howMany, minRatingsFromEachGender, moviesDirectory, ratingsDirectory, usersDirectory, outputDirectory) =>
      BestMoviesForGenderConfig(howMany.toInt, minRatingsFromEachGender.toInt, moviesDirectory, ratingsDirectory, usersDirectory, outputDirectory)
  }
}

object BestMoviesForGenderJob {

  def main(args: Array[String]): Unit = {
    SparkStarter.start("AverageRating job", BestMoviesForGender.apply, BestMoviesForGenderConfig.apply, args)
  }
}