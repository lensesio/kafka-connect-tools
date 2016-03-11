package com.github.rollulus

import scopt._

object AppCommand extends Enumeration {
  type AppCommand = Value
  val NONE, LIST, INFO = Value
}

import AppCommand._

case class Config(cmd: AppCommand= NONE)


object Go {
  def apply(cfg: Config) = {
    val url = "http://localhost:8083/"
    val api = new KafkaConnectApi(url)
    println(api.connectorInfo("twitter-source"))
//    val ns = api.activeConnectorNames()
//    println(ns.mkString("\n"))
  }
}

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Config]("kafconcli") {
      head("kafconcli", "1.0")
      cmd("ls") action { (_, c) => c.copy(cmd =LIST) } text ("list active connectors names") children()
      cmd("info") action { (_, c) => c.copy(cmd = INFO) } text ("show information for the specified connector(s)") children()
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

