package com.github.rollulus

import scalaj.http.{HttpResponse, Http}
import spray.json._
import DefaultJsonProtocol._

case class ErrorMessage(error_code: Int, message: String)
case class Task(connector: String, task: Int)
case class ConnectorInfo(name: String, config: Map[String,String], tasks: List[Task])

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val task = jsonFormat2(Task)
  implicit val connectorinfo = jsonFormat3(ConnectorInfo)
  implicit val errormsg = jsonFormat2(ErrorMessage)
}

class KafkaConnectApi(url: String) {
  val defaultHeaders = Seq("Accept" -> "application/json", "Content-Type" -> "application/json")

  private def non2xxException(resp: HttpResponse[String]): Exception = {
    import MyJsonProtocol.errormsg
    // try to deserialize message from body
    try {
      val msg = resp.body.parseJson.convertTo[ErrorMessage]
      new Exception(s"${msg.message} (${msg.error_code})")
    } catch {
      case _: Throwable => new Exception(s"${resp.code} ${resp.statusLine}")
    }
  }

  private def req[T : JsonReader](endpoint: String): T = {
    Http(url + endpoint).headers(defaultHeaders).asString match {
      case resp if resp.is2xx => resp.body.parseJson.convertTo[T]
      case resp => throw non2xxException(resp)
    }
  }

  def activeConnectorNames(): Seq[String] = {
    req[List[String]]("connectors")
  }

  def connectorInfo(name: String) : ConnectorInfo = {
    import MyJsonProtocol.connectorinfo
    req[ConnectorInfo](s"connectors/${name}")
  }
}