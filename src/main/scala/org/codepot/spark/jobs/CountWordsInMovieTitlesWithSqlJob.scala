package org.codepot.spark.jobs

import org.codepot.spark.{BatchJobConfig, SparkJob, SparkStarter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

case class CountWordsInMovieTitlesWithSql(master: String, jobConfig: CountWordsInMovieTitlesWithSqlConfig) extends SparkJob(master) {

  def runJob(sc: SparkContext): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val rdd: DataFrame = sc.textFile(jobConfig.inputDirectory)
      .map(toMovieTitle)
      .flatMap(extractWords)
      .map(toRecord)
      .toDF()
    rdd.registerTempTable("movies")

    // other functions: https://spark.apache.org/docs/1.4.0/api/scala/index.html#org.apache.spark.sql.functions$
    val wordCountDF = sqlContext.sql("select word, count(*) as total from movies group by word order by total desc")
    wordCountDF.show()
    wordCountDF.write.format("json").save(jobConfig.outputDirectory)
  }

  private def toRecord(word: String): Record = Record(word)

  private def extractWords(title: MovieTitle): Seq[String] = {
    title.title.replaceAll("[^a-zA-Z\\d]"," ").split("\\s").map(_.trim.toLowerCase).filterNot(_.isEmpty)
  }

  private def toMovieTitle(row: String): MovieTitle = {
    val tokens = row.split("::")
    MovieTitle(tokens(1).dropRight(7))
  }

  private case class MovieTitle(title: String)
  private case class Record(word: String)
}

case class CountWordsInMovieTitlesWithSqlConfig(inputDirectory: String, outputDirectory: String) extends BatchJobConfig

object CountWordsInMovieTitlesWithSqlConfig {
  def apply(args: Seq[String]): CountWordsInMovieTitlesWithSqlConfig = args match {
    case Seq(inputDirectory, outputDirectory) => CountWordsInMovieTitlesWithSqlConfig(inputDirectory, outputDirectory)
  }
}

object CountWordsInMovieTitlesWithSqlJob {

  def main(args: Array[String]): Unit = {
    SparkStarter.start("CountWordsInMovieTitles job", CountWordsInMovieTitlesWithSql.apply, CountWordsInMovieTitlesWithSqlConfig.apply, args)
  }
}