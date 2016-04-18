package com.datamountaineer.connect.tools

/** A collection of methods that translate the output of the API into some string representation */
trait Formatter {
  /** Formats a list of connector names as a string */
  def connectorNames(ns : Seq[String]): String
  /** Formats a ConnectorInfo as a string */
  def connectorInfo(i: ConnectorInfo) : String
}

/** A collection of methods that translate the output of the API into a string representation that pleases the human eye. */
class HumanFormatter extends Formatter {
  /** Formats a list of connector names as a string */
  override def connectorNames(ns: Seq[String]): String = ns.mkString("\n")
  /** Formats a ConnectorInfo as a string */
  override def connectorInfo(i: ConnectorInfo): String =
    s"""${i.name}:
       |  config:
       |${i.config.toList.map{kv => s"    ${kv._1}: ${kv._2}"}.mkString("\n")}
       |  task ids: ${i.tasks.map{t=>t.task.toString}.mkString(sep = "; ")}""".stripMargin
}

/** A collection of methods that translate the output of the API into a string representation that is compatible with the .properties format. */
class PropertiesFormatter extends Formatter {
  /** Formats a list of connector names as a string */
  override def connectorNames(ns: Seq[String]): String = ns.mkString("\n")
  /** Formats a ConnectorInfo as a string with the config fields key=value and additional info as #comment */
  override def connectorInfo(i: ConnectorInfo): String =
    s"""#Connector `${i.name}`:
       |${i.config.toList.map{kv => s"${kv._1}=${kv._2}"}.mkString("\n")}
       |#task ids: ${i.tasks.map{t=>t.task.toString}.mkString(sep = "; ")}""".stripMargin
}
