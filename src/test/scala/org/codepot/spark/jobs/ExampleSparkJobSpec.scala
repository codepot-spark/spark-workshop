package org.codepot.spark.jobs

import java.io.File

import org.specs2.mutable.Specification

class ExampleSparkJobSpec extends Specification with SparkTest {

  "Example Spark Job" should {

    "sum partial records and save results" in {
      // given - hdfs files
      val inPath = getClass.getResource("/data")
      val outPath = new File(mkTempDir())

      val args = Array("local", inPath.getPath, outPath.getPath)

      // when
      ExampleSparkJob.main(args)

      // then
      val resultInFile = getOutputData(outPath.getPath)
      resultInFile must contain(("a",6), ("b",7), ("c",2)).exactly
    }
  }

  def getOutputData(path: String): Seq[(String, Int)] = {
    getLines(path).map(line => line.split(",") match {
      case Array(userId: String, count: String) => (userId, count.toInt)
    })
  }

}