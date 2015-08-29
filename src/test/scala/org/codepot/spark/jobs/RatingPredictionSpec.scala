package org.codepot.spark.jobs

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.specs2.mutable.Specification

import scala.util.Random

object Job {

  case class Rating(movieId: Int, userId: Int, rating: Int)

  case class UserFeatures(userId: Int, gender: Int, age: Int, occupation: Int)

  case class MovieGenre(movieId: Int, genres: Seq[Int])

  def toRating(row: String): Rating = {
    val tokens = row.split("::")
    Rating(tokens(1).toInt, tokens(0).toInt, tokens(2).toInt)
  }

  def toMovieGenre(row: String) = {
    val split: Array[String] = row.split("::")
    MovieGenre(split(0).toInt, split(2).split("\\|").toSeq.map(genre => genreCodes.apply(genre)))
  }

  val genderCodes = Map("F" -> 0, "M" -> 1)
  val ageCodes = Map("1" -> 0, "18" -> 1, "25" -> 2, "35" -> 3, "45" -> 4, "50" -> 5, "56" -> 6)
  val genreCodes = Map(
    "Action" -> 0,
    "Adventure" -> 1,
    "Animation" -> 2,
    "Children's" -> 3,
    "Comedy" -> 4,
    "Crime" -> 5,
    "Documentary" -> 6,
    "Drama" -> 7,
    "Fantasy" -> 8,
    "Film-Noir" -> 9,
    "Horror" -> 10,
    "Musical" -> 11,
    "Mystery" -> 12,
    "Romance" -> 13,
    "Sci-Fi" -> 14,
    "Thriller" -> 15,
    "War" -> 16,
    "Western" -> 17
  )

  def toUser(row: String) = {
    val split: Array[String] = row.split("::")
    UserFeatures(split(0).toInt, genderCodes(split(1)), ageCodes(split(2)), split(3).toInt)
  }

  def average(ratings: Iterable[Rating]) = {
    ratings.map(_.rating).sum.toDouble / ratings.size
  }
}

class RatingPredictionSpec extends Specification with SparkTest {

  import Job._

  "prediction" should {

    "be good" in {
      // given
      val ratingsPath = getClass.getResource("/ml-1m/ratings")

      // when
      val sc = new SparkContext("local[*]", "rating prediction", new SparkConf())
      val ratings: RDD[Rating] = sc.textFile(ratingsPath.getPath).map(Job.toRating)

      val splits = ratings.randomSplit(Array(0.7, 0.3))
      val (trainData, testData) = (splits(0), splits(1))

      for {
        predictionFactory <- Seq[(String, RDD[Rating] => RDD[(Double, Double)])](
          ("static 2.5", staticPrediction(2.5)),
          ("random", randomPrediction),
          ("global average", globalAveragePrediction(trainData)),
          ("user average", userAveragePrediction(trainData)),
          ("movie average", movieAveragePrediction(trainData)),
          ("movie average with user default", movieAveragePredictionWithUserDefault(trainData)),
          ("features prediction", mllibPrediction(trainData)),
          ("features prediction (long)", mllibPredictionLong(trainData))) //runs about 10 minutes on modest hardware
        predictions = predictionFactory._2(testData)
        testMSE = predictions.map { case (r, p) => math.pow(r - p, 2) }.mean()
        unit = println(s"Test Mean Squared Error for ${predictionFactory._1}= " + testMSE)
      } yield None
      success
    }

  }

  def staticPrediction(value: Double)(testData: RDD[Rating]) = {
    testData.map(r => (r.rating.toDouble, value))
  }

  def randomPrediction(testData: RDD[Rating]) = {
    testData.map(r => (r.rating.toDouble, 1.0 + Random.nextDouble() * 4.0))
  }

  def globalAveragePrediction(trainData: RDD[Rating])(testData: RDD[Rating]) = {
    val globalAverage = trainData.map(_.rating).mean()
    staticPrediction(globalAverage)(testData)
  }

  def movieAveragePrediction(trainData: RDD[Rating])(testData: RDD[Rating]) = {
    val globalAverage = trainData.map(_.rating).mean()

    val movieAverage = trainData.groupBy(_.movieId).mapValues(Job.average)

    val join: RDD[(Int, (Rating, Option[Double]))] = testData.keyBy(_.movieId).leftOuterJoin(movieAverage)

    join.map { case (_, (rating, prediction)) => (rating.rating.toDouble, prediction.getOrElse(globalAverage)) }
  }

  def movieAveragePredictionWithUserDefault(trainData: RDD[Rating])(testData: RDD[Rating]) = {
    val globalAverage = trainData.map(_.rating).mean()

    val movieAverage = trainData.groupBy(_.movieId).mapValues(Job.average)

    val userAverage = trainData.groupBy(_.userId).mapValues(Job.average)

    val join: RDD[(Int, ((Rating, Option[Double]), Option[Double]))] = testData.keyBy(_.movieId).leftOuterJoin(movieAverage).leftOuterJoin(userAverage)

    join.map { case (_, ((rating, movieAverageValue), userAverageValue)) => (rating.rating.toDouble, movieAverageValue.getOrElse(userAverageValue.getOrElse(globalAverage))) }
  }

  def userAveragePrediction(trainData: RDD[Rating])(testData: RDD[Rating]) = {
    val globalAverage = trainData.map(_.rating).mean()

    val userAverage = trainData.groupBy(_.userId).mapValues(Job.average)

    val join: RDD[(Int, (Rating, Option[Double]))] = testData.keyBy(_.userId).leftOuterJoin(userAverage)

    join.map { case (_, (rating, prediction)) => (rating.rating.toDouble, prediction.getOrElse(globalAverage)) }
  }

  def mllibPrediction(trainData: RDD[Rating])(testData: RDD[Rating]) = {
    val moviesPath = getClass.getResource("/ml-1m/movies")
    val usersPath = getClass.getResource("/ml-1m/users")

    val movieAverage = trainData.groupBy(_.movieId).mapValues(Job.average)
    val userAverage = trainData.groupBy(_.userId).mapValues(Job.average)
    val movieGenres = trainData.sparkContext.textFile(moviesPath.getPath).map(Job.toMovieGenre).keyBy(_.movieId).join(movieAverage)
    val users = trainData.sparkContext.textFile(usersPath.getPath).map(Job.toUser).keyBy(_.userId).join(userAverage)


    def pointsFromRatings(data: RDD[Rating]): RDD[LabeledPoint] = {
      val joined: RDD[(Int, ((Rating, (UserFeatures, Double)), (MovieGenre, Double)))] = data.keyBy(_.userId).join(users).values.keyBy(_._1.movieId).join(movieGenres)
      val points = joined.flatMap {
        case (movieId, ((rating, (user, userAverageVal)), (genre, movieAverageVal))) => genre.genres.map(genre =>
          LabeledPoint(rating.rating.toDouble, Vectors.dense(Array(user.age.toDouble, user.gender, user.occupation, genre, userAverageVal, movieAverageVal))))
      }
      points
    }
    val trainPoints: RDD[LabeledPoint] = pointsFromRatings(trainData)
    val testPoints: RDD[LabeledPoint] = pointsFromRatings(testData)

    val categoricalFeaturesInfo = Map[Int, Int](0 -> Job.ageCodes.size, 1 -> 2, 2 -> 21, 3 -> Job.genreCodes.size)
    val numTrees = 20
    val featureSubsetStrategy = "auto"
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = categoricalFeaturesInfo.values.max

    val model = RandomForest.trainRegressor(trainPoints, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    testPoints.map { testPoint =>
      val prediction = model.predict(testPoint.features)
      (testPoint.label, prediction)
    }
  }

  def mllibPredictionLong(trainData: RDD[Rating])(testData: RDD[Rating]) = {
    val moviesPath = getClass.getResource("/ml-1m/movies")
    val usersPath = getClass.getResource("/ml-1m/users")

    val movieAverage = trainData.groupBy(_.movieId).mapValues(Job.average)
    val userAverage = trainData.groupBy(_.userId).mapValues(Job.average)
    val movieGenres = trainData.sparkContext.textFile(moviesPath.getPath).map(Job.toMovieGenre).keyBy(_.movieId).join(movieAverage)
    val maxMovieId = movieGenres.takeOrdered(1)(Ordering.by(-_._2._1.movieId)).head._2._1.movieId
    val users = trainData.sparkContext.textFile(usersPath.getPath).map(Job.toUser).keyBy(_.userId).join(userAverage)


    def pointsFromRatings(data: RDD[Rating]): RDD[LabeledPoint] = {
      val joined: RDD[(Int, ((Rating, (UserFeatures, Double)), (MovieGenre, Double)))] = data.keyBy(_.userId).join(users).values.keyBy(_._1.movieId).join(movieGenres)
      val points = joined.flatMap {
        case (movieId, ((rating, (user, userAverageVal)), (genre, movieAverageVal))) => genre.genres.map(genre =>
          LabeledPoint(rating.rating.toDouble, Vectors.dense(Array(user.age.toDouble, user.gender, user.occupation, genre, userAverageVal, movieAverageVal, movieId))))
      }
      points
    }
    val trainPoints: RDD[LabeledPoint] = pointsFromRatings(trainData)
    val testPoints: RDD[LabeledPoint] = pointsFromRatings(testData)

    val categoricalFeaturesInfo = Map[Int, Int](0 -> Job.ageCodes.size, 1 -> 2, 2 -> 21, 3 -> Job.genreCodes.size, 6 -> (maxMovieId + 1))
    val numTrees = 20
    val featureSubsetStrategy = "auto"
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = categoricalFeaturesInfo.values.max

    val model = RandomForest.trainRegressor(trainPoints, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    testPoints.map { testPoint =>
      val prediction = model.predict(testPoint.features)
      (testPoint.label, prediction)
    }
  }

}