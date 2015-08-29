package org.codepot.spark.jobs

import java.io.File

import org.specs2.mutable.Specification

class CountWordsInMovieTitlesJobSpec extends Specification with SparkTest {

  "Count Words In Movie Titles job" should {

    "count words in a few movie titles" in {
      // given
      val inPath = getClass.getResource("/ml-1m/movies")
      val outPath = new File(mkTempDir())

      // when
      // call your job here

      // then
      val resultInFile = getOutputData(outPath.getPath)
      resultInFile must contain(("the",1252), ("young",11), ("bone",3))
      resultInFile.toMap must not haveKey ""
    }
  }

  def getOutputData(path: String): Seq[(String, Int)] = {
    getLines(path).map(line => line.split(",") match {
      case Array(word: String, count: String) => (word, count.toInt)
    })
  }

}