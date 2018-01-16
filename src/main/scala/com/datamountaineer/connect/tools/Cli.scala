package com.datamountaineer.connect.tools

import java.io.{PrintWriter, StringReader, StringWriter}
import java.util.Properties

import spray.json._
import DefaultJsonProtocol._
import scopt._

import scala.collection.JavaConverters

/** Enumeration of CLI commands */
object AppCommand extends Enumeration {
  type AppCommand = Value
  val NONE, LIST_ACTIVE, GET, DELETE, CREATE, RUN, DIFF, STATUS, PLUGINS, DESCRIBE, RESTART, PAUSE, RESUME, VALIDATE,
  TASK_LIST, TASK_STATUS, TASK_RESTART = Value
}
import com.datamountaineer.connect.tools.AppCommand._

object Formats extends Enumeration {
  type Formats = Value
  val PROPERTIES, JSON = Value
}
import com.datamountaineer.connect.tools.Formats._

/** Container for default program argument values */
object Defaults {
  /** Initial Kafka Connect REST service from environment variable or default one */
  val BaseUrl = scala.util.Properties.envOrElse("KAFKA_CONNECT_REST", "http://localhost:8083/")
  val Format = Formats.withName(scala.util.Properties.envOrElse("KAFKA_CONNECT_FORMAT", "PROPERTIES"))
}

/**
  * Holds interpreted program arguments
  *
  * @param cmd the AppCommand to perform
  * @param url the url of the REST service, defaults to Defaults.BaseUrl
  * @param format the format of the config, defaults to Defaults.Format (can be "PROPERTIES" or "JSON")
  * @param connectorName an optional connector name that is the subject of the command
  * @param taskId an optional task id that is the subject of the command
  */
case class Arguments(cmd: AppCommand = NONE, url: String = Defaults.BaseUrl, format: Formats = Defaults.Format,
                     connectorName: Option[String] = None,
                     taskId: Option[Int] = None)

/** Performs the action contained in the Arguments on RestKafkaConnectApi */
object ExecuteCommand {
  /**
    * Performs the action contained in the Arguments on RestKafkaConnectApi
    *
    * @param cfg an Arguments object that contains what to do
    * @return A Try that indicates success or failure
    */
  def apply(cfg: Arguments) = {
    if (cfg.connectorName.isEmpty)
      require(cfg.cmd == LIST_ACTIVE || cfg.cmd == PLUGINS)
    val api = new RestKafkaConnectApi(new java.net.URI(cfg.url))
    val fmt = new PropertiesFormatter()
    val cmd = cfg.cmd
    lazy val connectorName = cfg.connectorName.get
    lazy val taskId = cfg.taskId.get

    lazy val configuration =  coherentConfig(configToMap(allStdIn.toSeq, cfg.format), connectorName, cmd)

    val res = cmd match {
      case LIST_ACTIVE => api.activeConnectorNames.map(fmt.connectorNames).map(Some(_))
      case GET => api.connectorInfo(connectorName).map(fmt.connectorInfo).map(Some(_))
      case DELETE => api.delete(connectorName).map(_ => None)
      case CREATE => api.addConnector(connectorName, configuration).map(fmt.connectorInfo).map(Some(_))
      case RUN => api.updateConnector(connectorName, configuration).map(fmt.connectorInfo).map(Some(_))
      case DIFF => api.diffConnector(connectorName, configuration).map(t => fmt.connectorDiff(t._1, t._2, t._3)).map(Some(_))
      case STATUS => api.connectorStatus(connectorName).map(fmt.connectorStatus).map(Some(_))
      case PLUGINS => api.connectorPlugins.map(fmt.connectorPlugins).map(Some(_))
      case DESCRIBE => api.connectorPluginsDescribe(connectorName).map(fmt.connectorPluginsValidate(_)).map(Some(_))
      case VALIDATE => api.connectorPluginsValidate(connectorName, configuration).map(fmt.connectorPluginsValidate(_, true, configuration)).map(Some(_))
      case PAUSE => api.connectorPause(connectorName).map(fmt.connectorStatus).map(Some(_))
      case RESTART => api.connectorRestart(connectorName).map(fmt.connectorStatus).map(Some(_))
      case RESUME => api.connectorResume(connectorName).map(fmt.connectorStatus).map(Some(_))
      case TASK_LIST => api.tasks(connectorName).map(fmt.tasks).map(Some(_))
      case TASK_STATUS => api.taskStatus(connectorName, taskId).map(fmt.taskStatus).map(Some(_))
      case TASK_RESTART => api.taskRestart(connectorName, taskId).map(_ => None)
    }
    res.recover{
      case ApiErrorException(e) => Some(e)
      case e: Exception => val sw = new StringWriter(); e.printStackTrace(new PrintWriter(sw)); Some(sw.toString)
    }.foreach{
      case Some(v) => println(v)
      case None =>
    }
    res
  }

  /**
    * When the configuration contains "name=xxx" where xxx != connectorName, the connect herders go nuts.
    * Although the CLI is just a messenger, in this case it will alter the message for the good cause.
    *
    * @param configuration properties as map
    * @param connectorName name of the connector
    */
  def coherentConfig(configuration: Map[String,String], connectorName: String, cmd: AppCommand): Map[String,String] = {
    configuration.get("name") match {
      case Some(name) if (name != connectorName) && (!cmd.equals(VALIDATE)) => {
        System.err.println(Console.YELLOW + s"warning: changed `name=${name}` into `name=${connectorName}`")

        configuration.updated("name", connectorName)
      }
      case _ => configuration
    }
  }

  /**
    * Returns an iterator that reads stdin until EOF
    *
    * @return an Iterator that reads stdin
    */
  def allStdIn = Iterator.
    continually(scala.io.StdIn.readLine).
    takeWhile(x => {
      x != null
    })

  def configToMap(input: Seq[String], format: Formats): Map[String, String] = {
    format match {
      case PROPERTIES => propsToMap(input)
      case JSON => jsonToMap(input)
    }
  }

  /**
    * Translates .properties key values into a String->String map. Lines starting with # are ignored.
    *
    * @param properties the lines containing the properties
    * @return a map with key -> value
    */
  def propsToMap(properties: Seq[String]): Map[String, String] = {
    val joined = properties.mkString("\n")
    val props = new Properties()
    props.load(new StringReader(joined))
    JavaConverters.propertiesAsScalaMapConverter(props).asScala.toMap
  }

  def jsonToMap(json: Seq[String]): Map[String, String] = {
    val parsed = json.mkString("\n").parseJson
    parsed.convertTo[Map[String, String]]
  }

}

object Cli {
  /**
    * Translates program arguments into an Arguments object
    *
    * @param args the program arguments
    * @return an Arguments object
    */
  def parseProgramArgs(args: Array[String]) = {
    new OptionParser[Arguments]("kafka-connect-cli") {
      head("kafka-connect-cli", "1.0.6")
      help("help") text ("prints this usage text")

      opt[String]('e', "endpoint") action { (x, c) =>
        c.copy(url = x) } text(s"Kafka Connect REST URL, default is ${Defaults.BaseUrl}")

      opt[String]('f', "format") action { (x, c) =>
        c.copy(format = Formats.withName(x)) } text(s"Format of the config, default is ${Defaults.Format}. Valid options are 'PROPERTIES' and 'JSON'.")

      cmd("ps") action { (_, c) => c.copy(cmd = LIST_ACTIVE) } text "list active connectors names.\n" children()
      cmd("get") action { (_, c) => c.copy(cmd = GET) } text "get the configuration of the specified connector.\n" children()
      cmd("rm") action { (_, c) => c.copy(cmd = DELETE) } text "remove the specified connector.\n" children()
      cmd("create") action { (_, c) => c.copy(cmd = CREATE) } text "create the specified connector with the config from stdin; the connector cannot already exist.\n" children()
      cmd("run") action { (_, c) => c.copy(cmd = RUN) } text "create or update the specified connector with the config from stdin.\n" children()
      cmd("diff") action { (_, c) => c.copy(cmd = DIFF) } text "diff the specified connector with the config from stdin.\n" children()
      cmd("status") action { (_, c) => c.copy(cmd = STATUS) } text "get connector and it's task(s) state(s).\n" children()
      cmd("plugins") action { (_,c) => c.copy(cmd = PLUGINS) } text "list the available connector class plugins on the classpath.\n" children()
      cmd("describe") action { (_,c) => c.copy(cmd = DESCRIBE) } text "list the configurations for a connector class plugin on the classpath.\n" children()
      cmd("pause") action { (_,c) => c.copy(cmd = PAUSE) } text "pause the specified connector.\n" children()
      cmd("restart") action { (_,c) => c.copy(cmd = RESTART) } text "restart the specified connector.\n" children()
      cmd("resume") action { (_,c) => c.copy(cmd = RESUME) } text "resume the specified connector.\n" children()
      cmd("validate") action { (_,c) => c.copy(cmd = VALIDATE) } text "validate the connector config from stdin against a connector class plugin on the classpath.\n" children()
      cmd("task_ps") action { (_,c) => c.copy(cmd = TASK_LIST) } text "list the tasks belonging to a connector.\n" children()
      cmd("task_status") action { (_,c) => c.copy(cmd = TASK_STATUS) } text "get the status of a connector task.\n" children()
      cmd("task_restart") action { (_,c) => c.copy(cmd = TASK_RESTART) } text "restart the specified connector task.\n" children()

      arg[String]("<connector-name>") optional() action { (x, c) =>
        c.copy(connectorName = Some(x))
      } text ("connector name")

      arg[Int]("<task-id>") optional() action { (x, c) =>
        c.copy(taskId = Some(x))
      } text ("task id")

      checkConfig { c =>
        if (c.cmd == NONE) failure("Command expected.")
        else if ((c.cmd != LIST_ACTIVE  && c.cmd != PLUGINS && c.cmd != DESCRIBE) && c.connectorName.isEmpty) failure("Please specify the connector-name")
        else if ((c.cmd == TASK_LIST  || c.cmd == TASK_STATUS || c.cmd == TASK_RESTART) && c.connectorName.isEmpty) failure("Please specify the task-id")
        else success
      }
    }.parse(args, Arguments())
  }

  /**
    * Entry point
    *
    * @param args program arguments
    */
  def main(args: Array[String]): Unit = {
    parseProgramArgs(args) match {
      case Some(as) =>
        if (ExecuteCommand(as).isFailure) sys.exit(1)
      case None =>
        sys.exit(1)
    }
  }
}
