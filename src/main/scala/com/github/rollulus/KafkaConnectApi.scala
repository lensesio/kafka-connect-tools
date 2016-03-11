package com.github.rollulus

import scalaj.http.{HttpResponse, Http}
import spray.json._
import DefaultJsonProtocol._

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