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
  def connectorPlugins(s: Seq[ConnectorPlugins]) : String
  def connectorPluginsValidate(s: ConnectorPluginsValidate, validate: Boolean, props: Map[String, String] = Map.empty) : String
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
  def connectorPlugins(s: Seq[ConnectorPlugins]): String = ???
  def connectorPluginsValidate(s: ConnectorPluginsValidate, validate: Boolean, props: Map[String, String] = Map.empty) : String = ???

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
    s"""${Console.GREEN}#Connector `${connectorInfo.name}`:
       |${connectorInfo.config.toList.map{ kv => s"${kv._1}=${kv._2}"}.mkString("\n")}
       |#task ids: ${connectorInfo.tasks.map{ t=>t.task.toString}.mkString(sep = "; ")}""".stripMargin

  def trace(t:Option[String], indent:String="") =
    t match {
      case Some(trace) => s"${indent}trace: ${Console.RED} ${trace}\n ${Console.RESET}"
      case None => ""
    }

  def taskStatus(t:TaskStatus) =
    s"  - taskId: ${t.id}\n" +
    s"    taskState: ${if (t.state.equals("RUNNING")) Console.GREEN else Console.RED}${t.state}${Console.RESET}\n" + trace(t.trace,"    ") +
    s"    workerId: ${t.worker_id}\n"

  override def connectorStatus(s:ConnectorTaskStatus): String =
    s"connectorState: ${if (s.connector.state.equals("RUNNING")) Console.GREEN else Console.RED} ${s.connector.state}${Console.RESET}\n"+
    s"workerId: ${s.connector.worker_id}\n" +
      trace(s.connector.trace) +
      s"numberOfTasks: ${s.tasks.length}\n"+
    s"tasks:\n"+
    s"${s.tasks.map(taskStatus).mkString("")}"

  override def connectorPlugins(s: Seq[ConnectorPlugins]): String = {
    s.map(s=> s"Class name: ${s.`class`}").mkString("\n")
  }

  override def connectorPluginsValidate(s: ConnectorPluginsValidate, validate: Boolean = false, props: Map[String, String] = Map.empty): String = {
    import MyJsonProtocol._
    import spray.json._
    val json = s.toJson.prettyPrint

    validate match {
      case true => {
        if (s.error_count > 0) {
          val errors = s.configs.filter(e => e.value.errors.length > 0).flatMap(e => e.value.errors)
          val str = errors.mkString("\n")
          s"${json}\n${Console.RED}Validation failed.\n$str\n${Console.RESET}"
        } else {
          s"${Console.GREEN} ${s.name} \n No validation errors. ${Console.YELLOW} \n\t${props.mkString("\n\t")} ${Console.RESET}"
        }
      }
      case false =>  s"${Console.YELLOW} ${s.toJson.prettyPrint} ${Console.RESET}"
    }
  }
}
