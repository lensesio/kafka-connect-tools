package com.datamountaineer.connect.tools

/** A collection of methods that translate the output of the API into some string representation */
trait Formatter {
  /**
    * Formats a list of connector names as a string
    * @param connectorNames the connector names to format
    * @return A formatted string
    */
  def connectorNames(connectorNames : Seq[String]): String

  /**
    * Formats a ConnectorInfo as a string
    * @param connectorInfo the ConnectorInfo to format
    * @return A formatted string
    */
  def connectorInfo(connectorInfo: ConnectorInfo) : String

  def connectorStatus(s:ConnectorTaskStatus): String
}

/** A collection of methods that translate the output of the API into a string representation that pleases the human eye. */
class HumanFormatter extends Formatter {
  /**
    * Formats a list of connector names as a string
    * @param connectorNames the connector names to format
    * @return A formatted string
    */
  override def connectorNames(connectorNames: Seq[String]): String = connectorNames.mkString("\n")
  /**
    * Formats a ConnectorInfo as a string
    * @param connectorInfo the ConnectorInfo to format
    * @return A formatted string
    */
  override def connectorInfo(connectorInfo: ConnectorInfo): String =
    s"""${connectorInfo.name}:
       |  config:
       |${connectorInfo.config.toList.map{ kv => s"    ${kv._1}: ${kv._2}"}.mkString("\n")}
       |  task ids: ${connectorInfo.tasks.map{ t=>t.task.toString}.mkString(sep = "; ")}""".stripMargin

  def connectorStatus(s:ConnectorTaskStatus): String = ???

}

/** A collection of methods that translate the output of the API into a string representation that is compatible with the .properties format. */
class PropertiesFormatter extends Formatter {
  /**
    * Formats a list of connector names as a string
    * @param connectorNames the connector names to format
    * @return A formatted string
    */
  override def connectorNames(connectorNames: Seq[String]): String = connectorNames.mkString("\n")
  /**
    * Formats a ConnectorInfo as a string with the config fields key=value and additional info as #comment
    * @param connectorInfo the ConnectorInfo to format
    * @return A formatted string
    */
  override def connectorInfo(connectorInfo: ConnectorInfo): String =
    s"""#Connector `${connectorInfo.name}`:
       |${connectorInfo.config.toList.map{ kv => s"${kv._1}=${kv._2}"}.mkString("\n")}
       |#task ids: ${connectorInfo.tasks.map{ t=>t.task.toString}.mkString(sep = "; ")}""".stripMargin

  def trace(t:Option[String], indent:String="") =
    t match {
      case Some(trace) => s"${indent}trace: ${trace}\n"
      case None => ""
    }

  def taskStatus(t:TaskStatus) =
    s"  - taskId: ${t.id}\n" +
    s"    taskState: ${t.state}\n" + trace(t.trace,"    ")

  override def connectorStatus(s:ConnectorTaskStatus): String =
    s"connectorState: ${s.connector.state}\n"+
      trace(s.connector.trace) +
      s"numberOfTasks: ${s.tasks.length}\n"+
    s"tasks:\n"+
    s"${s.tasks.map(taskStatus).mkString("")}"

}
