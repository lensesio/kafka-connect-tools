package com.datamountaineer.connect.tools

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

// http://docs.confluent.io/2.1.0-alpha1/connect/userguide.html#rest-interface
class KafkaConnectApi(url: java.net.URI) {
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

  //TODO: some day I know how to improve this
  private def req[T: JsonReader](endpoint: String, method: String = "GET", data: String = null): T = {
    val r = Http(url.resolve(endpoint).toString).headers(defaultHeaders)
    (r match {
      case _ if data != null => r.postData(data)
      case _ => r
    }).method(method).asString match {
      case resp if resp.is2xx => resp.body.parseJson.convertTo[T]
      case resp => throw non2xxException(resp)
    }
  }

  private def voidReq(endpoint: String, method: String = "GET"): Unit = {
    Http(url.resolve(endpoint).toString).method(method).headers(defaultHeaders).asString match {
      case resp if resp.is2xx =>
      case resp => throw non2xxException(resp)
    }
  }

  def activeConnectorNames(): Seq[String] = {
    req[List[String]]("/connectors")
  }

  def connectorInfo(name: String): ConnectorInfo = {
    import MyJsonProtocol.connectorinfo
    req[ConnectorInfo](s"/connectors/${name}")
  }

  def addConnector(name: String, config: String) : ConnectorInfo = {
    import MyJsonProtocol.connectorinfo
    req[ConnectorInfo](s"/connectors","POST",
      s"""{
         |  "name": "${name}",
         |  "config": ${config}
         |}""".stripMargin)
  }

  def updateConnector(name: String, config: String) : ConnectorInfo = {
    import MyJsonProtocol.connectorinfo
    req[ConnectorInfo](s"/connectors/${name}/config","PUT", config)
  }

  def delete(name: String) = {
    voidReq(s"/connectors/${name}","DELETE")
  }
}
