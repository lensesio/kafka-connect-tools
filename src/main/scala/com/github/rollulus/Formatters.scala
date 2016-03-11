package com.github.rollulus

trait Formatter {
  def connectorNames(ns : Seq[String]): String
  def connectorInfo(i: ConnectorInfo) : String
}

//TODO: can be more elegant
class HumanFormatter extends Formatter {
  override def connectorNames(ns: Seq[String]): String = ns.mkString("\n")
  override def connectorInfo(i: ConnectorInfo): String =
    s"""${i.name}:
       |  config:
       |${i.config.toList.map{kv => s"    ${kv._1}: ${kv._2}"}.mkString("\n")}
       |  task ids: ${i.tasks.map{t=>t.task.toString}.mkString(sep = "; ")}""".stripMargin
}