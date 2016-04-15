package com.datamountaineer.connect.tools

import scopt._

object AppCommand extends Enumeration {
  type AppCommand = Value
  val NONE, LIST, GET, DELETE, CREATE, RUN  = Value
}
import AppCommand._

object Defaults {
  val BaseUrl = "http://localhost:8083/"
}

case class Arguments(cmd: AppCommand= NONE, url: String = Defaults.BaseUrl, connectorNames: Seq[String] = Seq())

// Handles the AppCommand Arguments
object ExecuteCommand {
  def apply(cfg: Arguments) = {
    val api = new KafkaConnectApi(new java.net.URI(cfg.url))
    val fmt = new PropertiesFormatter()

    lazy val configuration = propsToMap(allStdIn.toSeq)

    cfg.cmd match {
      case LIST => println(fmt.connectorNames(api.activeConnectorNames))
      case DELETE => cfg.connectorNames.foreach(api.delete)
      case CREATE => println(cfg.connectorNames.map(api.addConnector(_, configuration)).map(fmt.connectorInfo).mkString("\n"))
      case RUN => println(cfg.connectorNames.map(api.updateConnector(_, configuration)).map(fmt.connectorInfo).mkString("\n"))
      case GET => println(cfg.connectorNames.map(api.connectorInfo).map(fmt.connectorInfo).mkString("\n"))
    }
  }

  // Returns an iterator that reads stdin until EOF.
  def allStdIn = Iterator.
    continually(io.StdIn.readLine).
    takeWhile(x => {
      x != null
    })

  // Translates .properties key values into a String->String map using a regex -- what can possibly go wrong?
  lazy val keyValueRegex = "([^#].*)=(.*)".r
  def propsToMap(s: Seq[String]): Map[String, String] = s.flatMap(_ match {
    case keyValueRegex(k, v) => Some((k.trim, v.trim))
    case _ => None
  }).toMap
}

// Entry point, translates arguments into a Config
object Cli {
  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Arguments]("kafconcli") {
      head("kafconcli", "1.0")
      help("help") text ("prints this usage text")

      opt[String]('e', "endpoint") action { (x, c) =>
        c.copy(url = x) } text(s"Kafka REST URL, default is ${Defaults.BaseUrl}")

      cmd("ls") action { (_, c) => c.copy(cmd = LIST) } text "list active connectors names." children()
      cmd("get") action { (_, c) => c.copy(cmd = GET) } text "get information about the specified connector(s)." children()
      cmd("rm") action { (_, c) => c.copy(cmd = DELETE) } text "remove the specified connector(s)." children()
      cmd("create") action { (_, c) => c.copy(cmd = CREATE) } text "create the specified connector with the .properties from stdin; the connector cannot already exist." children()
      cmd("run") action { (_, c) => c.copy(cmd = RUN) } text "create or update the specified connector with the .properties from stdin." children()

      arg[String]("<connector-name>...") unbounded() optional() action { (x, c) =>
        c.copy(connectorNames = c.connectorNames :+ x)
      } text ("connector name(s)")

      checkConfig { c =>
        if (c.cmd == NONE) failure("Command expected.")
        else if (c.cmd != LIST && c.connectorNames.length==0) failure("Please specify the connector-name(s)")
        else success
      }
    }

    parser.parse(args, Arguments()) match {
      case Some(as) =>
        ExecuteCommand(as)
      case None =>
    }
  }
}

