package org.codepot.spark.jobs

import java.io.File

import org.specs2.mutable.Specification

class BestRatedJobSpec extends Specification with SparkTest {

  "Best Rated job" should {

    "calculate best rated movies" in {
      // given
      val howMany = 10.toString
      val minRatingsFromEachGender = 10.toString
      val moviesPath = getClass.getResource("/ml-1m/movies")
      val ratingsPath = getClass.getResource("/ml-1m/ratings")
      val usersPath = getClass.getResource("/ml-1m/users")
      val outPath = new File(mkTempDir())

      val args = Array("local", howMany, minRatingsFromEachGender, moviesPath.getPath, ratingsPath.getPath, usersPath.getPath, outPath.getPath)

      // when
      BestRatedJob.main(args)

      // then
      val resultInFile = getOutputData(outPath.getPath)
      resultInFile must contain(("Seven Samurai (The Magnificent Seven) (Shichinin no samurai)",4.560509554140127))
      resultInFile must contain(("Rear Window",4.476190476190476))
      resultInFile must haveSize(howMany.toInt)
    }
  }

  def getOutputData(path: String): Seq[(String, Double)] = {
    getLines(path).map(line => line.split("::") match {
      case Array(title: String, rating: String) => (title, rating.toDouble)
    })
  }

}