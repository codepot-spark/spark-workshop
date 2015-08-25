package org.codepot.spark

import scala.reflect._

object SparkStarter {

  def start[J <: SparkJob : ClassTag, C <: BatchJobConfig](description: String,
                                                           jobFactory: (String, C) => J,
                                                           configFactory: (Seq[String]) => C,
                                                           args: Seq[String]): Unit = {
    val parser = new ArgsParser(classTag[J].runtimeClass.getName + "Job", description, "<master>", {
      case master +: tail => jobFactory(master, configFactory(tail))
    })
    parser.parse(args).startJob()
  }
}