package org.codepot.spark.jobs

import java.io.File
import java.util.UUID

import scala.io.Source

trait SparkTest {

  def mkTempDir() = s"/tmp/codepot-spark-starter-${UUID.randomUUID().toString}"

  def getLines(path: String): Seq[String] = {
    new File(path).listFiles()
      .filterNot(file => file.getName.startsWith("_") || file.getName.endsWith(".crc"))
      .flatMap(Source.fromFile(_).getLines())
  }
}
