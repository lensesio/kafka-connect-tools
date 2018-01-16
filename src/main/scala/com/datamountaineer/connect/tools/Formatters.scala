package com.datamountaineer.connect.tools

import com.google.common.collect.MapDifference
import scala.collection.JavaConverters._

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

  /**
    * Formats a MapDifference as a string
    * @param current the current config
    * @param provided the config provided by the user
    * @param connectorInfo the MapDifference to format
    * @return A formatted string
    */
  def connectorDiff(current: Map[String,String], provided: Map[String,String], connectorInfo: MapDifference[String,String]) : String

  def connectorStatus(s:ConnectorTaskStatus): String
  def connectorPlugins(s: Seq[ConnectorPlugins]) : String
  def connectorPluginsValidate(s: ConnectorPluginsValidate, validate: Boolean, props: Map[String, String] = Map.empty) : String
  def tasks(tasks: List[TaskInfo]): String
  def taskStatus(t:TaskStatus): String
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
       |${connectorInfo.config.toList.map{ kv => s"    ${kv._1}: ${if (kv._1.contains("password") || kv._1.contains("secret")) "**********" else kv._2}"}.mkString("\n")}
       |  task ids: ${connectorInfo.tasks.map{ t=>t.task.toString}.mkString(sep = "; ")}${Console.RESET}""".stripMargin

  def connectorDiff(current: Map[String,String], provided: Map[String,String], diff: MapDifference[String,String]): String = ???
  def connectorStatus(s:ConnectorTaskStatus): String = ???
  def connectorPlugins(s: Seq[ConnectorPlugins]): String = ???
  def connectorPluginsValidate(s: ConnectorPluginsValidate, validate: Boolean, props: Map[String, String] = Map.empty) : String = ???
  def tasks(tasks: List[TaskInfo]): String = ???
  def taskStatus(t:TaskStatus): String = ???
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
       |${connectorInfo.config.toList.map{ kv => s"${kv._1}=${if (kv._1.contains("password") || kv._1.contains("secret")) "**********" else kv._2}"}.mkString("\n")}
       |#task ids: ${connectorInfo.tasks.map{ t=>t.task.toString}.mkString(sep = "; ")}${Console.RESET}""".stripMargin

  /**
    * Formats a MapDifference as a string
    * @param diff the MapDifference to format
    * @return A formatted string
    */
  override def connectorDiff(current: Map[String,String], provided: Map[String,String], diff: MapDifference[String,String]): String =
    s"""${Console.GREEN}
       |#current config:
       |${current.mkString("\n")}
       |
       |#provided config:
       |${provided.mkString("\n")}
       |
       |#entry value diffs:
       |${diff.entriesDiffering().asScala.mkString("\n")}
       |
       |#entries only in current config:
       |${diff.entriesOnlyOnLeft().asScala.mkString("\n")}
       |
       |#entries only in provided config:
       |${diff.entriesOnlyOnRight.asScala.mkString("\n")}""".stripMargin

  private def trace(t:Option[String], indent:String="") =
    t match {
      case Some(trace) => s"${indent}trace: ${Console.RED} ${trace}\n ${Console.RESET}"
      case None => ""
    }

  override def taskStatus(t:TaskStatus) =
    s"taskId: ${t.id}\n" +
    s"taskState: ${if (t.state.equals("RUNNING")) Console.GREEN else Console.RED}${t.state}${Console.RESET}\n" + trace(t.trace,"    ") +
    s"workerId: ${t.worker_id}\n"

  private def childTaskStatus(t:TaskStatus) =
    s"  - taskId: ${t.id}\n" +
    s"    taskState: ${if (t.state.equals("RUNNING")) Console.GREEN else Console.RED}${t.state}${Console.RESET}\n" + trace(t.trace,"    ") +
    s"    workerId: ${t.worker_id}\n"

  private def childTaskInfo(t:TaskInfo) =
    s"  - ${t.id.connector} task ${t.id.task}\n" +
    s"${childTaskInfoConfig(t.config)}\n\n"

  private def childTaskInfoConfig(config: Map[String, String]) =
    config.map(e => ("      " + e._1, e._2))
        .map(_.productIterator.mkString(": "))
        .mkString("\n")

  override def tasks(tasks: List[TaskInfo]) =
    s"""
       |${tasks.map(childTaskInfo).mkString("")}
     """.stripMargin

  override def connectorStatus(s:ConnectorTaskStatus): String =
    s"connectorState: ${if (s.connector.state.equals("RUNNING")) Console.GREEN else Console.RED} ${s.connector.state}${Console.RESET}\n"+
    s"workerId: ${s.connector.worker_id}\n" +
      trace(s.connector.trace) +
      s"numberOfTasks: ${s.tasks.length}\n"+
    s"tasks:\n"+
    s"${s.tasks.map(childTaskStatus).mkString("")}"

  override def connectorPlugins(s: Seq[ConnectorPlugins]): String = {
    s.map(s => s"Class name: ${s.`class`}, Type: ${s.`type`}, Version: ${s.version.getOrElse("")}").mkString("\n")
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
          s"${Console.GREEN} ${s.name} \n No validation errors. ${Console.YELLOW}${Console.RESET}"
        }
      }
      case false =>  s"${Console.YELLOW} ${s.toJson.prettyPrint} ${Console.RESET}"
    }
  }
}
