package com.github.rollulus

import scopt._

object AppCommand extends Enumeration {
  type AppCommand = Value
  val NONE, LIST, INFO, DELETE, CREATE, RUN  = Value
}
import AppCommand._

object Defaults {
  val BaseUrl = "http://localhost:8083/"
}

case class Config(cmd: AppCommand= NONE, url: String = Defaults.BaseUrl, connectorNames: Seq[String] = Seq())


object Go {
  def allStdIn = Iterator.
    continually(io.StdIn.readLine).
    takeWhile(x => {x != null})

  lazy val R = "([^#].*)=(.*)".r
  def propsToJson(s:Seq[String]) = "{" + s.map(_ match {
    case R(k,v) => s""""${k.trim}":"${v.trim}""""
    case _ => ""
  }).filterNot(_.isEmpty).mkString(", ") + "}"

  def apply(cfg: Config) = {
    val api = new KafkaConnectApi(new java.net.URI(cfg.url))
    val fmt = new PropertiesFormatter()

    lazy val configuration = propsToJson(allStdIn.toSeq)

    cfg.cmd match {
      case LIST => println(fmt.connectorNames(api.activeConnectorNames))
      case DELETE => cfg.connectorNames.foreach(api.delete)
      case CREATE => cfg.connectorNames.foreach(api.addConnector(_, configuration))
      case RUN => cfg.connectorNames.foreach(api.updateConnector(_, configuration))
      case INFO => println(cfg.connectorNames.map(api.connectorInfo).map(fmt.connectorInfo).mkString("\n"))
    }
  }
}

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Config]("kafconcli") {
      head("kafconcli", "1.0")
      help("help") text ("prints this usage text")

      opt[String]('e', "endpoint") action { (x, c) =>
        c.copy(url = x) } text(s"Kafka REST URL, default is ${Defaults.BaseUrl}")

      cmd("ls") action { (_, c) => c.copy(cmd = LIST) } text "list active connectors names." children()
      cmd("info") action { (_, c) => c.copy(cmd = INFO) } text "retrieve information for the specified connector(s)." children()
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

    parser.parse(args, Config()) match {
      case Some(config) =>
        Go(config)
      case None =>
    }
  }
}

