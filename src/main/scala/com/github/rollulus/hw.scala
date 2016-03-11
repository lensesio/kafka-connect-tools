import scopt._
import scalaj.http.{HttpResponse, Http}
import spray.json._
import DefaultJsonProtocol._

object AppCommand extends Enumeration {
  type AppCommand = Value
  val NONE, LIST, INFO = Value
}

import AppCommand._

case class Config(cmd: AppCommand= NONE)

case class Task(connector: String, task: Int)

case class ConnectorInfo(name: String, config: Map[String,String], tasks: List[Task])

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val task = jsonFormat2(Task)
  implicit val connectorinfo = jsonFormat3(ConnectorInfo)
}

class KafkaConnectApi(url: String) {
  val defaultHeaders = Seq("Accept" -> "application/json", "Content-Type" -> "application/json")

  private def req[T : JsonReader](endpoint: String): T = {
    Http(url + endpoint).headers(defaultHeaders).asString match {
      case resp if resp.is2xx => resp.body.parseJson.convertTo[T]
      case _ => throw new Exception("TODO")
    }
  }

  def activeConnectorNames(): Seq[String] = {
    req[List[String]]("connectors")
  }

  def connectorInfo(name: String) : ConnectorInfo = {
    import MyJsonProtocol._
    req[ConnectorInfo](s"connectors/${name}")
   // null
  }
}


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

