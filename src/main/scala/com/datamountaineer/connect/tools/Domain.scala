package com.datamountaineer.connect.tools

import spray.json.DefaultJsonProtocol

/**
  * Created by andrew@datamountaineer.com on 04/11/2016. 
  * kafka-connect-tools
  */
/** Equivalent of http://docs.confluent.io/2.1.0-alpha1/connect/userguide.html#statuses-errors */
case class ErrorMessage(error_code: Int, message: String)
/** A Task structure as embedded in e.g. http://docs.confluent.io/2.1.0-alpha1/connect/userguide.html#post--connectors */
case class Task(connector: String, task: Int)
/** A ConnectorInfo as e.g. http://docs.confluent.io/2.1.0-alpha1/connect/userguide.html#post--connectors */
case class ConnectorInfo(name: String, config: Map[String,String], tasks: List[Task])
/** A TasklessConnectorInfo as e.g. http://docs.confluent.io/2.1.0-alpha1/connect/userguide.html#post--connectors */
case class TasklessConnectorInfo(name: String, config: Map[String,String])

case class ConnectorStatus(state:String,worker_id:String, trace:Option[String])
case class TaskStatus(id:Int, state:String,worker_id:String,trace:Option[String])
case class ConnectorTaskStatus(name:String, connector: ConnectorStatus,  tasks: List[TaskStatus])
case class ConnectorPlugins(`class` :String, `type`: String, version: Option[String])
case class TaskInfo(id:TaskId, config:Map[String,String])
case class TaskId(connector:String, task:Int)

case class Definition(name: String, `type`: String, required: Boolean, default_value: Option[String],
                      importance: Option[String], group: Option[String], display_name: Option[String],
                      dependents: Option[Array[String]], order : Option[Int])
case class Values(name: String, value: Option[String], recommended_values: Array[String], errors: Array[String], visible: Boolean)
case class Configs(definition: Definition, value: Values)
case class ConnectorPluginsValidate(name:String, error_count: Int, groups: Array[String], configs: Array[Configs])

/** Implicits for JSON (de)serialization */
object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val task = jsonFormat2(Task)
  implicit val connectorinfo = jsonFormat3(ConnectorInfo)
  implicit val tasklessconnectorinfo = jsonFormat2(TasklessConnectorInfo)
  implicit val errormsg = jsonFormat2(ErrorMessage)
  implicit val connectorstatus = jsonFormat3(ConnectorStatus)
  implicit val taskstatus = jsonFormat4(TaskStatus)
  implicit val taskid = jsonFormat2(TaskId)
  implicit val taskinfo = jsonFormat2(TaskInfo)
  implicit val connectortaskstatus = jsonFormat3(ConnectorTaskStatus)
  implicit val connectorplugins = jsonFormat3(ConnectorPlugins)
  implicit val values = jsonFormat5(Values)
  implicit val definitions = jsonFormat9(Definition)
  implicit val configs = jsonFormat2(Configs)
  implicit val connectorpluginsvalidate = jsonFormat4(ConnectorPluginsValidate)
}