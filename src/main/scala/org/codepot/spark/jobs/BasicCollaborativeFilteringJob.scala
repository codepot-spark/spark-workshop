package org.codepot.spark.jobs

import org.codepot.spark.{BatchJobConfig, SparkJob, SparkStarter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class BasicCollaborativeFiltering(master: String, jobConfig: BasicCollaborativeFilteringConfig) extends SparkJob(master) {

  def runJob(sc: SparkContext): Unit = {
    val movies: RDD[Movie] = sc.textFile(jobConfig.moviesDirectory).map(toMovie)
    val ratings: RDD[Rating] = sc.textFile(jobConfig.ratingsDirectory).map(toRating)
    val users: RDD[User] = sc.textFile(jobConfig.usersDirectory).map(toUser)

    /*
    select count(userid) as nbusers, nbratings
from (select round(count(itemid)/10,0)*10 as nbratings, userid
      from ratingsdata
      group by userid
     ) as nbratingsbyusers
group by nbratings
order by nbratings desc;
     */

    // distribution of the number of ratings by user
    val byUserDistribution = ratings.groupBy(_.userId).map { case (userId, rating) =>
      (userId, Math.round(rating.size.toDouble/10)*10)
    }.groupBy(_._2).map { case (score, userIdPairs) =>
    (score, userIdPairs.size)
    }.sortByKey()
    println("User ratings histogram: " + byUserDistribution.take(10).mkString(", ")) // FIXME

    /*
    select count(itemid) as nbitems, nbratings
from (select round(count(userid)/10,0)*10 as nbratings, itemid
      from ratingsdata
      group by itemid
     ) as nbratingsbyitems
group by nbratings
order by nbratings desc;
     */

    val byMovieDistribution = ratings.groupBy(_.movieId).map { case (movieId, rating) =>
      (movieId, Math.round(rating.size.toDouble/10)*10)
    }.groupBy(_._2).map { case (score, userIdPairs) =>
      (score, userIdPairs.size)
    }.sortByKey()
    println("Item ratings histogram: " + byMovieDistribution.take(10).mkString(", ")) // FIXME

    /*
    select title, avgrating, nbratings
from items,
   (select round(avg(rating),1) as avgrating,
       count(userid) as nbratings, itemid
    from ratings
    group by itemid
    order by avgrating desc
    limit 10
   ) as avgratingbyitems
where items.itemid = avgratingbyitems.itemid
order by avgrating desc;
     */

    // global recommendations

    val globalReco = ratings.groupBy(_.movieId).map { case (itemId, ratingList) =>
      GlobalRecommendation(Math.round(ratingList.map(_.rating).sum/ratingList.size), ratingList.size, itemId)
    }.map(m => (m.movieId, m)).join(movies.map(m => (m.id, m))).take(10).map {
      case (itemId, (recommendation, item)) => item
    }
    println("Global recommendations: " + globalReco.mkString(", "))

  }

  private def toRating(row: String): Rating = {
    val tokens = row.split("::")
    Rating(tokens(1).toInt, tokens(0).toInt, tokens(2).toInt)
  }

  private def toMovie(row: String): Movie = {
    val tokens = row.split("::")
    Movie(tokens(0).toInt, tokens(1).dropRight(7))
  }

  private def toUser(row: String): User = {
    val tokens = row.split("::")
    User(tokens(0).toInt, tokens(2).toInt, tokens(1).charAt(0))
  }

  private case class Movie(id: Int, title: String)
  private case class Rating(movieId: Int, userId: Int, rating: Int)
  private case class User(userId: Int, age: Int, gender: Char)

  private case class GlobalRecommendation(avgRating: Double, numOfRatings: Int, movieId: Int)
}


case class BasicCollaborativeFilteringConfig(moviesDirectory: String, usersDirectory: String,
                                             ratingsDirectory: String, outputDirectory: String) extends BatchJobConfig

object BasicCollaborativeFilteringConfig {
  def apply(args: Seq[String]): BasicCollaborativeFilteringConfig = args match {
    case Seq(moviesDirectory, usersDirectory, ratingsDirectory, outputDirectory) =>
      BasicCollaborativeFilteringConfig(moviesDirectory, usersDirectory, ratingsDirectory, outputDirectory)
  }
}

object BasicCollaborativeFilteringJob {

  def main(args: Array[String]): Unit = {
    SparkStarter.start("CollaborativeFiltering job", BasicCollaborativeFiltering.apply, BasicCollaborativeFilteringConfig.apply, args)
  }
}