package org.codepot.spark.jobs

import org.codepot.spark.{BatchJobConfig, SparkJob, SparkStarter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class CountWordsInMovieTitles(master: String, jobConfig: CountWordsInMovieTitlesConfig) extends SparkJob(master) {

  def runJob(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.textFile(jobConfig.inputDirectory).map(toMovieTitle).flatMap(extractWords)

    val counted = rdd.map((_, 1)).reduceByKey(_ + _)
//    val counted = sc.parallelize(rdd.countByValue().toSeq) spark built-in but collects results to driver, making it worse in terms of performance

    counted.map { case (word, count) => s"$word::$count" }
      .saveAsTextFile(jobConfig.outputDirectory)
  }

  private def extractWords(title: MovieTitle): Seq[String] = {
    title.title.replaceAll("[^a-zA-Z\\d]"," ").split("\\s").map(_.trim.toLowerCase).filterNot(_.isEmpty)
  }

  private def toMovieTitle(row: String): MovieTitle = {
    val tokens = row.split("::")
    MovieTitle(tokens(1).dropRight(7))
  }

  private case class MovieTitle(title: String)
}

case class CountWordsInMovieTitlesConfig(inputDirectory: String, outputDirectory: String) extends BatchJobConfig

object CountWordsInMovieTitlesConfig {
  def apply(args: Seq[String]): CountWordsInMovieTitlesConfig = args match {
    case Seq(inputDirectory, outputDirectory) => CountWordsInMovieTitlesConfig(inputDirectory, outputDirectory)
  }
}

object CountWordsInMovieTitlesJob {

  def main(args: Array[String]): Unit = {
    SparkStarter.start("CountWordsInMovieTitles job", CountWordsInMovieTitles.apply, CountWordsInMovieTitlesConfig.apply, args)
  }
}