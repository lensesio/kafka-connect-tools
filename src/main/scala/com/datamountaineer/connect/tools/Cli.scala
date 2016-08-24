package com.datamountaineer.connect.tools

import java.io.{PrintWriter, StringWriter}
import scopt._

/** Enumeration of CLI commands */
object AppCommand extends Enumeration {
  type AppCommand = Value
  val NONE, LIST_ACTIVE, GET, DELETE, CREATE, RUN, STATUS = Value
}
import AppCommand._

/** Container for default program argument values */
object Defaults {
  /** Initial Kafka Connect REST service from environment variable or default one */
  val BaseUrl = scala.util.Properties.envOrElse("KAFKA_CONNECT_REST", "http://localhost:8083/")
}

/**
  * Holds interpreted program arguments
  *
  * @param cmd the AppCommand to perform
  * @param url the url of the REST service, defaults to Defaults.BaseUrl
  * @param connectorName an optional connector name that is the subject of the command
  */
case class Arguments(cmd: AppCommand= NONE, url: String = Defaults.BaseUrl, connectorName: Option[String] = None)

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
      require(cfg.cmd == LIST_ACTIVE)
    val api = new RestKafkaConnectApi(new java.net.URI(cfg.url))
    val fmt = new PropertiesFormatter()

    lazy val connectorName = cfg.connectorName.get
    lazy val configuration = coherentConfig(propsToMap(allStdIn.toSeq), connectorName)

    val res = cfg.cmd match {
      case LIST_ACTIVE => api.activeConnectorNames.map(fmt.connectorNames).map(Some(_))
      case GET => api.connectorInfo(connectorName).map(fmt.connectorInfo).map(Some(_))
      case DELETE => api.delete(connectorName).map(_ => None)
      case CREATE => api.addConnector(connectorName, configuration).map(fmt.connectorInfo).map(Some(_))
      case RUN => api.updateConnector(connectorName, configuration).map(fmt.connectorInfo).map(Some(_))
      case STATUS => api.connectorStatus(connectorName).map(fmt.connectorStatus).map(Some(_))
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
  def coherentConfig(configuration: Map[String,String], connectorName: String): Map[String,String] = {
    configuration.get("name") match {
      case Some(name) if name != connectorName => System.err.println(s"warning: changed `name=${name}` into `name=${connectorName}`")
        configuration.updated("name", connectorName)
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

  /** Regex that is used in propsToMap */
  lazy val keyValueRegex = "([^#].*)=(.*)".r

  /**
    * Translates .properties key values into a String->String map using a regex. Lines starting with # are ignored.
    *
    * @param properties the lines containing the properties
    * @return a map with key -> value
    */
  def propsToMap(properties: Seq[String]): Map[String, String] = properties.flatMap(_ match {
    case keyValueRegex(k, v) => Some((k.trim, v.trim))
    case _ => None
  }).toMap
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
      head("kafka-connect-cli", "0.4")
      help("help") text ("prints this usage text")

      opt[String]('e', "endpoint") action { (x, c) =>
        c.copy(url = x) } text(s"Kafka Connect REST URL, default is ${Defaults.BaseUrl}")

      cmd("ps") action { (_, c) => c.copy(cmd = LIST_ACTIVE) } text "list active connectors names.\n" children()
      cmd("get") action { (_, c) => c.copy(cmd = GET) } text "get the configuration of the specified connector.\n" children()
      cmd("rm") action { (_, c) => c.copy(cmd = DELETE) } text "remove the specified connector.\n" children()
      cmd("create") action { (_, c) => c.copy(cmd = CREATE) } text "create the specified connector with the .properties from stdin; the connector cannot already exist.\n" children()
      cmd("run") action { (_, c) => c.copy(cmd = RUN) } text "create or update the specified connector with the .properties from stdin.\n" children()
      cmd("status") action { (_, c) => c.copy(cmd = STATUS) } text "get connector and it's task(s) state(s).\n" children()

      arg[String]("<connector-name>") optional() action { (x, c) =>
        c.copy(connectorName = Some(x))
      } text ("connector name")

      checkConfig { c =>
        if (c.cmd == NONE) failure("Command expected.")
        else if (c.cmd != LIST_ACTIVE && c.connectorName.isEmpty) failure("Please specify the connector-name")
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
