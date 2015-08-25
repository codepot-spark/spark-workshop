package org.codepot.spark.jobs

import org.codepot.spark.{BatchJobConfig, SparkJob, SparkStarter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class Example(master: String, jobConfig: ExampleConfig) extends SparkJob(master) {

  def runJob(sc: SparkContext): Unit = {
    val rdd: RDD[StatsRow] = sc.textFile(jobConfig.inputDirectory).map(toStatsRow)

    val byKey = rdd
      .map(stats => (stats.name, stats))
      .reduceByKey((stats1, stats2) => StatsRow(stats1.name, stats1.count + stats2.count))

    byKey.map { case (_,StatsRow(key,count)) => s"$key,$count" }
      .saveAsTextFile(jobConfig.outputDirectory)
  }

  private def toStatsRow(row: String): StatsRow = {
    val tokens = row.split(",")
    StatsRow(tokens(0), tokens(1).toInt)
  }
}

case class StatsRow(name: String, count: Int)

case class ExampleConfig(inputDirectory: String, outputDirectory: String) extends BatchJobConfig

object ExampleConfig {
  def apply(args: Seq[String]): ExampleConfig = args match {
    case Seq(inputDirectory, outputDirectory) => ExampleConfig(inputDirectory, outputDirectory)
  }
}

object ExampleSparkJob {

  def main(args: Array[String]): Unit = {
    SparkStarter.start("example job", Example.apply, ExampleConfig.apply, args)
  }
}