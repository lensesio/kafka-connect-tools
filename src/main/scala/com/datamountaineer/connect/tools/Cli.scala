package com.datamountaineer.connect.tools

import java.io.{PrintWriter, StringWriter}
import scopt._

/** Enumeration of CLI commands */
object AppCommand extends Enumeration {
  type AppCommand = Value
  val NONE, LIST_ACTIVE, GET, DELETE, CREATE, RUN  = Value
}
import AppCommand._

/** Container for default program argument values */
object Defaults {
  val BaseUrl = "http://localhost:8083/"
}

/** Holds interpreted program arguments */
case class Arguments(cmd: AppCommand= NONE, url: String = Defaults.BaseUrl, connectorName: Option[String] = None)

/** Performs the action contained in the Arguments on RestKafkaConnectApi */
object ExecuteCommand {
  def apply(cfg: Arguments) = {
    val api = new RestKafkaConnectApi(new java.net.URI(cfg.url))
    val fmt = new PropertiesFormatter()

    lazy val configuration = propsToMap(allStdIn.toSeq)
    lazy val connector = cfg.connectorName.get

    val res = cfg.cmd match {
      case LIST_ACTIVE => api.activeConnectorNames.map(fmt.connectorNames).map(Some(_))
      case GET => api.connectorInfo(connector).map(fmt.connectorInfo).map(Some(_))
      case DELETE => api.delete(connector).map(_ => None)
      case CREATE => api.addConnector(connector, configuration).map(fmt.connectorInfo).map(Some(_))
      case RUN => api.updateConnector(connector, configuration).map(fmt.connectorInfo).map(Some(_))
    }
    res.recover{
      case ApiErrorException(e) => Some(e)
      case e: Exception => val sw = new StringWriter(); e.printStackTrace(new PrintWriter(sw)); Some(sw.toString) //the sad state of Java
    }.foreach{
      case Some(v) => println(v)
      case None =>
    }
    res
  }

  /** Returns an iterator that reads stdin until EOF */
  def allStdIn = Iterator.
    continually(io.StdIn.readLine).
    takeWhile(x => {
      x != null
    })

  /** Regex that is used in propsToMap */
  lazy val keyValueRegex = "([^#].*)=(.*)".r
  /** Translates .properties key values into a String->String map using a regex */
  def propsToMap(s: Seq[String]): Map[String, String] = s.flatMap(_ match {
    case keyValueRegex(k, v) => Some((k.trim, v.trim))
    case _ => None
  }).toMap
}

object Cli {
  /** Translates program arguments into an Arguments object */
  def parseProgramArgs(args: Array[String]) = {
    new OptionParser[Arguments]("kafconcli") {
      head("kafconcli", "1.0")
      help("help") text ("prints this usage text")

      opt[String]('e', "endpoint") action { (x, c) =>
        c.copy(url = x) } text(s"Kafka REST URL, default is ${Defaults.BaseUrl}")

      cmd("ps") action { (_, c) => c.copy(cmd = LIST_ACTIVE) } text "list active connectors names." children()
      cmd("get") action { (_, c) => c.copy(cmd = GET) } text "get information about the specified connector." children()
      cmd("rm") action { (_, c) => c.copy(cmd = DELETE) } text "remove the specified connector." children()
      cmd("create") action { (_, c) => c.copy(cmd = CREATE) } text "create the specified connector with the .properties from stdin; the connector cannot already exist." children()
      cmd("run") action { (_, c) => c.copy(cmd = RUN) } text "create or update the specified connector with the .properties from stdin." children()

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

  /** Entry point */
  def main(args: Array[String]): Unit = {
    parseProgramArgs(args) match {
      case Some(as) =>
        if (ExecuteCommand(as).isFailure) sys.exit(1)
      case None =>
        sys.exit(1)
    }
  }
}
