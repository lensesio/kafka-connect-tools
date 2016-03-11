package com.github.rollulus

import scopt._

object AppCommand extends Enumeration {
  type AppCommand = Value
  val NONE, LIST, INFO, DELETE = Value
}
import AppCommand._

object Defaults {
  val BaseUrl = "http://localhost:8083/"
}

case class Config(cmd: AppCommand= NONE, url: String = Defaults.BaseUrl)

object Go {
  def apply(cfg: Config) = {
    val api = new KafkaConnectApi(cfg.url)
    val fmt = new HumanFormatter()
    println(fmt.connectorInfo(api.connectorInfo("twitter-source")))
//    val ns = api.activeConnectorNames()
//    println(ns.mkString("\n"))
  }
}

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Config]("kafconcli") {
      head("kafconcli", "1.0")
      cmd("ls") action { (_, c) => c.copy(cmd = LIST) } text "list active connectors names" children()
      cmd("info") action { (_, c) => c.copy(cmd = INFO) } text "show information for the specified connector(s)" children()
      cmd("rm") action { (_, c) => c.copy(cmd = DELETE) } text "removes specified connector(s)" children()
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        // do stuff
        Go(config)

      case None =>
        // arguments are bad, error message will have been displayed
        println("fail")
    }
  }
}

