package com.datamountaineer.connect.tools

import scalaj.http.{Http, BaseHttp, HttpResponse}
import spray.json._
import DefaultJsonProtocol._
import scala.util.{Try, Success, Failure}
import spray.http._

/** Equivalent of http://docs.confluent.io/2.1.0-alpha1/connect/userguide.html#statuses-errors */
case class ErrorMessage(error_code: Int, message: String)
/** A Task structure as embedded in e.g. http://docs.confluent.io/2.1.0-alpha1/connect/userguide.html#post--connectors */
case class Task(connector: String, task: Int)
/** A ConnectorInfo as e.g. http://docs.confluent.io/2.1.0-alpha1/connect/userguide.html#post--connectors */
case class ConnectorInfo(name: String, config: Map[String,String], tasks: List[Task])
/** A TasklessConnectorInfo as e.g. http://docs.confluent.io/2.1.0-alpha1/connect/userguide.html#post--connectors */
case class TasklessConnectorInfo(name: String, config: Map[String,String])

/** Implicits for JSON (de)serialization */
object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val task = jsonFormat2(Task)
  implicit val connectorinfo = jsonFormat3(ConnectorInfo)
  implicit val tasklessconnectorinfo = jsonFormat2(TasklessConnectorInfo)
  implicit val errormsg = jsonFormat2(ErrorMessage)
}

/** Allows one to do HTTP requests */
trait HttpClient {
  /**
    * Allows one to do HTTP requests. Returns a statuscode and optionally a body.
    * @param url url
    * @param method method
    * @param headers headers
    * @param reqBody an Optional request body
    * @return statuscode, Optional response body
    */
  def request(url: java.net.URI, method: String, headers: Seq[(String, String)], reqBody: Option[String]): Try[(Int, Option[String])]
}

/** Allows one to do HTTP requests using the Scalaj client */
object ScalajHttpClient extends HttpClient {
  /**
    * Allows one to do HTTP requests. Returns a statuscode and optionally a body.
    * @param url url
    * @param method method
    * @param headers headers
    * @param reqBody an Optional request body
    * @return statuscode, Optional response body
    */
  def request(url: java.net.URI, method: String, headers: Seq[(String, String)], reqBody: Option[String]): Try[(Int, Option[String])] = {
    try {
      val r = Http(url.toString).headers(headers)
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

/** An Exception to hold errors as yielded by the REST API */
case class ApiErrorException(e: String) extends Exception

/** Kafka Connect Api interface */
trait KafkaConnectApi {
  /**
    * Returns the names of the currently active connectors
    * @return A Seq with currently active connectors names as string
    */
  def activeConnectorNames(): Try[Seq[String]]

  /**
    * Returns a ConnectorInfo given the connector's name
    * @param name the connector's name
    * @return A ConnectorInfo Try
    */
  def connectorInfo(name: String): Try[ConnectorInfo]

  /**
    * Creates a new connector, initialized with a string->string map. The connector cannot already exist. Returns its ConnectorInfo
    * @param name the name of the connector to create
    * @param config a string->string map with its config
    * @return a ConnectorInfo Try
    */
  def addConnector(name: String, config: Map[String, String]): Try[ConnectorInfo]

  /**
    * Updates or creates a new connector, initializes it with a string->string map. The connector may or may not already exist. Returns its ConnectorInfo
    * @param name the name of the connector to update or create
    * @param config a string->string map with its config
    * @return a ConnectorInfo Try
    */
  def updateConnector(name: String, config: Map[String, String]): Try[ConnectorInfo]

  /**
    * Removes the specified connector
    * @param name the name of the connector to delete
    * @return A Try
    */
  def delete(name: String): Try[Unit]
}

/** Kafka Connect Api interface */
class RestKafkaConnectApi(baseUrl: java.net.URI, httpClient: HttpClient = ScalajHttpClient) extends KafkaConnectApi {
  val headers = Seq("Accept" -> "application/json", "Content-Type" -> "application/json")

  /**
    * Translates a statuscode, body into an appropriate Exception
    * @param status the HTTP status code
    * @param respBody the HTTP response body
    * @return An Exception
    */
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

  /**
    * Performs an HTTP request to some endpoint. Constructs url from endpoint and base, inserts headers, (de)serializes, etc.
    * @param endpoint the relative endpoint (i.e. path)
    * @param method the HTTP method
    * @param requestBody the request body
    * @tparam T the type to JSON deserialize the response into
    * @return A deserialized T Option
    */
  private def req[T: JsonReader](endpoint: String, method: String = "GET", requestBody: String = null): Option[T] = {
    httpClient.request(baseUrl.resolve(endpoint), method, headers, Option(requestBody))
    match {
      case Success((status, respBody)) if (status >= 200 && status < 300) =>
        if (respBody.isDefined) Some(respBody.get.parseJson.convertTo[T]) else None
      case Success((status, respBody)) => throw non2xxException(status, respBody)
      case Failure(e) => throw e
    }
  }

  /**
    * Returns the names of the currently active connectors
    * @return A Seq with currently active connectors names as string
    */
  def activeConnectorNames(): Try[Seq[String]] = {
    Try(req[List[String]]("/connectors").get)
  }

  /**
    * Returns a ConnectorInfo given the connector's name
    * @param name the connector's name
    * @return A ConnectorInfo Try
    */
  def connectorInfo(name: String): Try[ConnectorInfo] = {
    import MyJsonProtocol.connectorinfo
    Try(req[ConnectorInfo](s"/connectors/${name}").get)
  }

  /**
    * Creates a new connector, initialized with a string->string map. The connector cannot already exist. Returns its ConnectorInfo
    * @param name the name of the connector to create
    * @param config a string->string map with its config
    * @return a ConnectorInfo Try
    */
  def addConnector(name: String, config: Map[String,String]) : Try[ConnectorInfo] = {
    import MyJsonProtocol._
    Try(req[ConnectorInfo](s"/connectors", "POST",
      TasklessConnectorInfo(name, config).toJson.toString).get)
  }

  /**
    * Updates or creates a new connector, initializes it with a string->string map. The connector may or may not already exist. Returns its ConnectorInfo
    * @param name the name of the connector to update or create
    * @param config a string->string map with its config
    * @return a ConnectorInfo Try
    */
  def updateConnector(name: String, config: Map[String,String]) : Try[ConnectorInfo] = {
    import MyJsonProtocol._
    Try(req[ConnectorInfo](s"/connectors/${name}/config", "PUT",
      config.toJson.toString).get)
  }

  /**
    * Removes the specified connector
    * @param name the name of the connector to delete
    * @return A Try
    */
  def delete(name: String) : Try[Unit] = {
    Try(req[Unit](s"/connectors/${name}","DELETE"))
  }
}
