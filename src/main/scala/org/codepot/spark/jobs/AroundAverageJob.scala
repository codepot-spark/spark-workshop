package org.codepot.spark.jobs

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.codepot.spark.{SparkStarter, BatchJobConfig, SparkJob}

case class AroundAverage(master: String, jobConfig: AroundAverageConfig) extends SparkJob(master) {

  def runJob(sc: SparkContext): Unit = {
    val ratings: RDD[Rating] = sc.textFile(jobConfig.ratingsDirectory).map(toRating)

    val perUserAverageRating: RDD[(Int, Double)] = ratings.groupBy(_.userId)
      .map { case (userId, userRatings) => (userId, rollingAverage(userRatings.map(_.rating)))}

    val globalAverage = ratings.map(_.rating).mean()

    val averageUsers = perUserAverageRating
      .map { case (userId, avgRate) => (userId, Math.abs(globalAverage - avgRate))}
      .sortBy(_._2)
      .take(jobConfig.numberOfUsers)

    sc.parallelize(averageUsers).saveAsTextFile(jobConfig.outputDirectory)
  }

  private def toRating(row: String): Rating = {
    val tokens = row.split("::")
    Rating(tokens(1).toInt,tokens(0).toInt, tokens(2).toInt)
  }

  // standard (possibly overflowing) average
  def average(s: Iterable[Int]): Double = {
    s.sum / s.toSeq.length
  }

  // rolling average
  def rollingAverage(s: Iterable[Int]): Double = s.foldLeft((0.0, 1))((acc, i) => (acc._1 + (i - acc._1) / acc._2, acc._2 + 1))._1

  private case class Movie(id: Int, title: String)
  private case class Rating(movieId: Int, userId: Int, rating: Int)
}


case class AroundAverageConfig(numberOfUsers: Int, ratingsDirectory: String, outputDirectory: String) extends BatchJobConfig

object AroundAverageConfig {
  def apply(args: Seq[String]): AroundAverageConfig = args match {
    case Seq(numberOfUsers, ratingsDirectory, outputDirectory) => AroundAverageConfig(numberOfUsers.toInt, ratingsDirectory, outputDirectory)
  }
}

object AroundAverageJob {

  def main(args: Array[String]): Unit = {
    SparkStarter.start("AverageRating job", AroundAverage.apply, AroundAverageConfig.apply, args)
  }
}