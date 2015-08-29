package org.codepot.spark.jobs

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.codepot.spark.jobs.Job.Rating
import org.specs2.mutable.Specification

import scala.util.Random

object Job {

  case class Rating(movieId: Int, userId: Int, rating: Int)

  def toRating(row: String): Rating = {
    val tokens = row.split("::")
    Rating(tokens(1).toInt, tokens(0).toInt, tokens(2).toInt)
  }

}

class RatingPredictionSpec extends Specification with SparkTest {

  "prediction" should {

    "be good" in {
      // given
      val ratingsPath = getClass.getResource("/ml-1m/ratings")

      // when
      val sc = new SparkContext("local[*]", "rating prediction", new SparkConf())
      val ratings: RDD[Rating] = sc.textFile(ratingsPath.getPath).map(Job.toRating)

      val splits = ratings.randomSplit(Array(0.7, 0.3))
      val (trainData, testData) = (splits(0), splits(1))

      val results = for {
        predictionFactory <- Seq[(String, RDD[Rating] => RDD[(Double, Double)])](
          ("static 2.5", staticPrediction(2.5)),
          ("random", randomPrediction),
          ("global average", globalAveragePrediction(trainData))
        //write your own prediction model here
        )
        predictions = predictionFactory._2(testData)
        testMSE = predictions.map { case (r, p) => math.pow(r - p, 2) }.mean()
        result = (predictionFactory._1, testMSE)
      } yield result
      results.foreach(result => println(s"Test Mean Squared Error for ${result._1} = ${result._2}"))
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


}