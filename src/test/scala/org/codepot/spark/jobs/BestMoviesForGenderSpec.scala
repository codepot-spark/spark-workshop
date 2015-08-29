package org.codepot.spark.jobs

import java.io.File

import org.specs2.mutable.Specification

class BestMoviesForGenderSpec extends Specification with SparkTest {

  "Best Movies for gender job" should {

    "calculate best male and female movies" in {
      // given
      val howMany = 10.toString
      val minRatingsFromEachGender = 10.toString
      val moviesPath = getClass.getResource("/ml-1m/movies")
      val ratingsPath = getClass.getResource("/ml-1m/ratings")
      val usersPath = getClass.getResource("/ml-1m/users")
      val outPath = new File(mkTempDir())

      val args = Array("local", howMany, minRatingsFromEachGender, moviesPath.getPath, ratingsPath.getPath, usersPath.getPath, outPath.getPath)

      // when
      BestMoviesForGenderJob.main(args)

      // then
      val bestMale = getOutputData(outPath.getPath + "/male")
      bestMale must contain(("Good, The Bad and The Ugly, The",0.7263506433630917))
      val bestFemale = getOutputData(outPath.getPath + "/female")
      bestFemale must contain(("Love in the Afternoon",1.1614906832298133))
    }
  }

  def getOutputData(path: String): Seq[(String, Double)] = {
    getLines(path).map(line => line.split("::") match {
      case Array(title: String, rating: String) => (title, rating.toDouble)
    })
  }

}