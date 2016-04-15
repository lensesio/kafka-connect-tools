package com.datamountaineer.connect.tools

import scalaj.http.{Http, BaseHttp, HttpResponse}
import spray.json._
import DefaultJsonProtocol._
import scala.util.{Try, Success, Failure}

import spray.http._

case class ErrorMessage(error_code: Int, message: String)
case class Task(connector: String, task: Int)
case class ConnectorInfo(name: String, config: Map[String,String], tasks: List[Task])
case class TasklessConnectorInfo(name: String, config: Map[String,String])

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val task = jsonFormat2(Task)
  implicit val connectorinfo = jsonFormat3(ConnectorInfo)
  implicit val tasklessconnectorinfo = jsonFormat2(TasklessConnectorInfo)
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

case class ApiErrorException(e: String) extends Exception

trait KafkaConnectApi {
  def activeConnectorNames(): Try[Seq[String]]
  def connectorInfo(name: String): Try[ConnectorInfo]
  def addConnector(name: String, config: Map[String,String]) : Try[ConnectorInfo]
  def updateConnector(name: String, config: Map[String,String]) : Try[ConnectorInfo]
  def delete(name: String) : Try[Unit]
}

// http://docs.confluent.io/2.1.0-alpha1/connect/userguide.html#rest-interface
class RestKafkaConnectApi(baseUrl: java.net.URI, httpClient: HttpClient = ScalajHttpClient) extends KafkaConnectApi {
  val headers = Seq("Accept" -> "application/json", "Content-Type" -> "application/json")

  private def non2xxException(status: Int, respBody: Option[String]): Exception = {
    import MyJsonProtocol.errormsg
    // try to deserialize message from body
    try {
      val msg = respBody.get.parseJson.convertTo[ErrorMessage]
      new ApiErrorException(s"Error: the Kafka Connect API returned: ${msg.message} (${msg.error_code})")
    } catch {
      case _: Throwable => new Exception(s"Error: the Kafka Connect API returned status code ${status}")
    }
  }

  // constructs url from endpoint and base, inserts headers, etc.
  // throws!
  private def req[T: JsonReader](endpoint: String, method: String = "GET", data: String = null): Option[T] = {
    httpClient.request(baseUrl.resolve(endpoint), method, headers, Option(data))
    match {
      case Success((status, respBody)) if (status >= 200 && status < 300) =>
        if (respBody.isDefined) Some(respBody.get.parseJson.convertTo[T]) else None
      case Success((status, respBody)) => throw non2xxException(status, respBody)
      case Failure(e) => throw e
    }
  }

  def activeConnectorNames(): Try[Seq[String]] = {
    Try(req[List[String]]("/connectors").get)
  }

  def connectorInfo(name: String): Try[ConnectorInfo] = {
    import MyJsonProtocol.connectorinfo
    Try(req[ConnectorInfo](s"/connectors/${name}").get)
  }

  def addConnector(name: String, config: Map[String,String]) : Try[ConnectorInfo] = {
    import MyJsonProtocol._
    Try(req[ConnectorInfo](s"/connectors", "POST",
      TasklessConnectorInfo(name, config).toJson.toString).get)
  }

  def updateConnector(name: String, config: Map[String,String]) : Try[ConnectorInfo] = {
    import MyJsonProtocol._
    Try(req[ConnectorInfo](s"/connectors/${name}/config", "PUT",
      config.toJson.toString).get)
  }

  def delete(name: String) : Try[Unit] = {
    Try(req[Unit](s"/connectors/${name}","DELETE"))
  }
}
