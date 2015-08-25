package org.codepot.spark

import org.slf4j.LoggerFactory

trait Logging {

  protected lazy val logger: org.slf4j.Logger =
    LoggerFactory.getLogger(getClass.getName)
}