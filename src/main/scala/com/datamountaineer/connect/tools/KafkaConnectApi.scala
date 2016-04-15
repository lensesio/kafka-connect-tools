package com.datamountaineer.connect.tools

import scalaj.http.{Http, BaseHttp, HttpResponse}
import spray.json._
import DefaultJsonProtocol._
import scala.util.{Try, Success, Failure}

import spray.http._

case class ErrorMessage(error_code: Int, message: String)
case class Task(connector: String, task: Int)
case class ConnectorInfo(name: String, config: Map[String,String], tasks: List[Task])

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val task = jsonFormat2(Task)
  implicit val connectorinfo = jsonFormat3(ConnectorInfo)
  implicit val errormsg = jsonFormat2(ErrorMessage)
}

trait HttpClient {
  def request(url: java.net.URI, method: String, hdrs: Seq[(String, String)], reqBody: Option[String]): Try[(Int, Option[String])]
}

object ScalajHttpClient extends HttpClient {
  def request(url: java.net.URI, method: String, hdrs: Seq[(String, String)], reqBody: Option[String]): Try[(Int, Option[String])] = {
    try {
      val r = Http(url.toString).headers(hdrs)
      (reqBody match {
        case Some(body) => r.postData(body)
        case None => r
      }).method(method).asString match {
        case resp if resp.code == 204 => Success((resp.code, None))
        case resp => Success((resp.code, Some(resp.body)))
      }
    } catch {
      case e: Throwable => Failure(e)
    }
  }
}

// http://docs.confluent.io/2.1.0-alpha1/connect/userguide.html#rest-interface
class KafkaConnectApi(baseUrl: java.net.URI, httpClient: HttpClient = ScalajHttpClient) {
  val headers = Seq("Accept" -> "application/json", "Content-Type" -> "application/json")

  private def non2xxException(status: Int, respBody: Option[String]): Exception = {
    import MyJsonProtocol.errormsg
    // try to deserialize message from body
    try {
      val msg = respBody.get.parseJson.convertTo[ErrorMessage]
      new Exception(s"${msg.message} (${msg.error_code})")
    } catch {
      case _: Throwable => new Exception(s"${status}")
    }
  }

  // constructs url from endpoint and base, inserts headers, etc.
  private def req[T: JsonReader](endpoint: String, method: String = "GET", data: String = null): Option[T] = {
    httpClient.request(baseUrl.resolve(endpoint), method, headers, Option(data))
    match {
      case Success((status, respBody)) if (status >= 200 && status < 300) =>
        if (respBody.isDefined) Some(respBody.get.parseJson.convertTo[T]) else None
      case Success((status, respBody)) => throw non2xxException(status, respBody)
      case Failure(e) => throw e
    }
  }

  def activeConnectorNames(): Seq[String] = {
    req[List[String]]("/connectors").get
  }

  def connectorInfo(name: String): ConnectorInfo = {
    import MyJsonProtocol.connectorinfo
    req[ConnectorInfo](s"/connectors/${name}").get
  }

  def addConnector(name: String, config: String) : ConnectorInfo = {
    import MyJsonProtocol.connectorinfo
    req[ConnectorInfo](s"/connectors","POST",
      s"""{
         |  "name": "${name}",
         |  "config": ${config}
         |}""".stripMargin).get
  }

  def updateConnector(name: String, config: String) : ConnectorInfo = {
    import MyJsonProtocol.connectorinfo
    req[ConnectorInfo](s"/connectors/${name}/config","PUT", config).get
  }

  def delete(name: String) = {
    req[Unit](s"/connectors/${name}","DELETE")
  }
}
