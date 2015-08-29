package org.codepot.spark.jobs

import java.io.File

import org.specs2.mutable.Specification

class AverageRatingJobSpec extends Specification with SparkTest {

  "Average Rating job" should {

    "calculate average rating per movie" in {
      // given
      val moviesPath = getClass.getResource("/ml-1m/movies")
      val ratingsPath = getClass.getResource("/ml-1m/ratings")
      val usersPath = getClass.getResource("/ml-1m/users")
      val outPath = new File(mkTempDir())

      val args = Array("local", moviesPath.getPath, usersPath.getPath, ratingsPath.getPath, outPath.getPath)

      // when
      BasicCollaborativeFilteringJob.main(args)

      // then
      success
    }
  }

  def getOutputData(path: String): Seq[(String, Double)] = {
    getLines(path).map(line => line.split("::") match {
      case Array(title: String, rating: String) => (title, rating.toDouble)
    })
  }

}