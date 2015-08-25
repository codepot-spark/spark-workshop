package org.codepot.spark

class ArgsParser[T](appName: String, description: String, argsDescription: String, factory: PartialFunction[Seq[String], T]) {

  def parse(args: Seq[String]): T = {
    if (factory.isDefinedAt(args)) {
      factory(args)
    } else {
      val helpInfo = s"$description. Usage: '$appName $argsDescription'. Supplied args: ${args.mkString("(", ", ", ")")}"
      System.err.println(helpInfo)
      throw new IllegalArgumentException(helpInfo)
    }
  }
}