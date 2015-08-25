package org.codepot.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

abstract class SparkJob(master: String) extends Serializable with Logging {

  val appName: String = getClass.getSimpleName

  protected def runJob(sc: SparkContext): Unit

  def startJob(): Unit = {
    logger.info(s"Initialize Spark context for app: $appName")

    val sc = new SparkContext(master, appName, new SparkConf())
    try {
      logger.info(s"Run job $appName")
      runJob(sc)
    } catch {
      case e: Exception =>
        logger.error(s"Spark job $appName failed", e)
        throw e
    } finally {
      logger.info(s"Spark job $appName ended")
      sc.stop()
    }
  }

}