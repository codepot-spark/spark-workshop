package org.codepot.spark.jobs

import java.io.File

import org.specs2.mutable.Specification

class CountWordsInMovieTitlesWithSqlJobSpec extends Specification with SparkTest {

  "Count Words In Movie Titles job" should {

    "count words in a few movie titles" in {
      // given
      val inPath = getClass.getResource("/ml-1m/movies")
      val outPath = new File(mkTempDir())

      val args = Array("local", inPath.getPath, outPath.getPath)

      // when
      CountWordsInMovieTitlesWithSqlJob.main(args)

      // then
      val resultInFile = getOutputData(outPath.getPath)
      resultInFile must contain(("the",1252), ("young",11), ("bone",3))
      resultInFile.toMap must not haveKey ""
    }

    "parse" in {
      val g = """{"word":"the","total":1252}"""
      val jsonR = "word\":\"(\\w+)\",\"total\":(\\d+)".r

      val r =jsonR.findAllIn(g).matchData.map(l => (l.group(1), l.group(2).toInt)).toSeq.head
      println(r)
      success
    }
  }

  // {"word":"the","total":1252}
  def getOutputData(path: String): Seq[(String, Int)] = {
    val jsonR = "word\":\"(\\w+)\",\"total\":(\\d+)".r
    getLines(path).map(line =>
      jsonR.findAllIn(line).matchData.map(l => (l.group(1), l.group(2).toInt)).toSeq.head
    )
  }

}