package com.datamountaineer.connect.tools

import scalaj.http.Http
import spray.json._

import scala.util.{Failure, Success, Try}
import DefaultJsonProtocol._
import MyJsonProtocol._
import com.google.common.collect.{MapDifference, Maps}
import scala.collection.JavaConverters._

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
      case c: java.net.ConnectException => {
        println(c.getMessage)
        Success(404, Some(""))
      }
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

  def connectorStatus(name:String):Try[ConnectorTaskStatus]
  def connectorPluginsDescribe(name: String) : Try[ConnectorPluginsValidate]
  def connectorPluginsValidate(name: String, config: Map[String, String]) : Try[ConnectorPluginsValidate]
  def connectorPlugins() : Try[List[ConnectorPlugins]]

  def connectorPause(name: String) : Try[ConnectorTaskStatus]
  def connectorRestart(name: String) : Try[ConnectorTaskStatus]
  def connectorResume(name: String) : Try[ConnectorTaskStatus]

  def taskRestart(connector: String, taskId: Int) : Try[Unit]
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
    // try to deserialize message from body
    try {
      val msg = respBody.get.parseJson.convertTo[ErrorMessage]
      new ApiErrorException(s"${Console.RED} Error: the Kafka Connect API returned: ${msg.message} (${msg.error_code}) ${Console.RESET}")
    } catch {
      case _: Throwable => new Exception(s"${Console.RED} Error: the Kafka Connect API returned status code ${status} ${Console.RESET}")
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
    import MyJsonProtocol._
    Try({
        val names = req[List[String]]("/connectors").get
        if (names.isEmpty) {
          println(s"${Console.YELLOW}No running connectors${Console.RESET}")
          names
        } else {
          names
        }
    })
  }

  /**
    * Returns a ConnectorInfo given the connector's name
    * @param name the connector's name
    * @return A ConnectorInfo Try
    */
  def connectorInfo(name: String): Try[ConnectorInfo] = {
    import MyJsonProtocol._
    Try(req[ConnectorInfo](s"/connectors/${name}").get)
  }

  /**
    * Creates a new connector, initialized with a string->string map. The connector cannot already exist. Returns its ConnectorInfo
    * @param name the name of the connector to create
    * @param config a string->string map with its config
    * @return a ConnectorInfo Try
    */
  def addConnector(name: String, config: Map[String,String]) : Try[ConnectorInfo] = {
    println("Validating connector properties before posting")
    connectorPluginsValidate(name, config)
    println(s"Connector properties valid. Creating connector $name")
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
    println("Validating connector properties before posting")
    connectorPluginsValidate(name, config)
    println(s"Connector properties valid. Creating connector $name")
    import MyJsonProtocol._
    Try(req[ConnectorInfo](s"/connectors/${name}/config", "PUT",
      config.toJson.toString).get)
  }

  /**
    * Diffs the connector config with the provided config
    * @param name the name of the connector to update or create
    * @param config a string->string map with its config
    * @return a ConnectorInfo Try
    */
  def diffConnector(name: String, config: Map[String,String]) : Try[(Map[String, String], Map[String, String], MapDifference[String,String])] = {
    println("Validating connector properties before diffing")
    connectorPluginsValidate(name, config)
    println(s"Connector properties valid. Diffing connector ${name} against provided config")
    connectorInfo(name).map(info => {
      (info.config, config, Maps.difference[String,String](info.config.asJava, config.asJava))
    })
  }

  /**
    * Removes the specified connector
    * @param name the name of the connector to delete
    * @return A Try
    */
  def delete(name: String) : Try[Unit] = {
    import MyJsonProtocol._
    Try(req[Unit](s"/connectors/${name}","DELETE"))
  }

  override def connectorRestart(name: String): Try[ConnectorTaskStatus] = {
    import MyJsonProtocol._
    Try(req[Unit](s"/connectors/${name}/restart", "POST"))
    println("Waiting for restart")
    Thread.sleep(3000)
    Try(req[ConnectorTaskStatus](s"/connectors/${name}/status").get)
  }

  override def connectorPause(name: String): Try[ConnectorTaskStatus] = {
    import MyJsonProtocol._
    Try(req[Unit](s"/connectors/${name}/pause", "PUT"))
    println("Waiting for pause")
    Thread.sleep(3000)
    Try(req[ConnectorTaskStatus](s"/connectors/${name}/status").get)
  }

  override def connectorResume(name: String): Try[ConnectorTaskStatus] = {
    import MyJsonProtocol._
    Try(req[Unit](s"/connectors/${name}/resume", "PUT"))
    println("Waiting for resume")
    Thread.sleep(3000)
    Try(req[ConnectorTaskStatus](s"/connectors/${name}/status").get)
  }

  /**
    * Get the Connector Classes loaded on the Classpath
    * @return A ConnectorPlugins
    * */
  def connectorPlugins() : Try[List[ConnectorPlugins]] = {
    import MyJsonProtocol._
    Try(req[List[ConnectorPlugins]](s"/connector-plugins").get)
  }

  /**
    * Get the Connector Classes loaded on the Classpath
    * @return A PluginProps
    * */
  def connectorPluginsDescribe(name: String) : Try[ConnectorPluginsValidate] = {
    import MyJsonProtocol._
    Try(req[ConnectorPluginsValidate](s"/connector-plugins/${name}/config/validate", "PUT", s"""{\"connector.class\": \"${name}\"}""").get)
  }


  def connectorPluginsValidate(name: String, config: Map[String, String]): Try[ConnectorPluginsValidate] = {
    import MyJsonProtocol._
    Try(req[ConnectorPluginsValidate](s"/connector-plugins/${name}/config/validate", "PUT", config.toJson.toString).get)
  }

  def connectorStatus(name:String):Try[ConnectorTaskStatus] = {
    import MyJsonProtocol._
    Try(req[ConnectorTaskStatus](s"/connectors/${name}/status").get)
  }

  def tasks(connector: String): Try[List[TaskInfo]] = {
    import MyJsonProtocol._
    Try(req[List[TaskInfo]](s"/connectors/$connector/tasks").get)
  }

  def taskStatus(connector: String, taskId: Int): Try[TaskStatus] = {
    import MyJsonProtocol._
    Try(req[TaskStatus](s"/connectors/$connector/tasks/$taskId/status").get)
  }

  def taskRestart(connector: String, taskId: Int): Try[Unit] = {
    import MyJsonProtocol._
    Try(req[Unit](s"/connectors/$connector/tasks/$taskId/restart", "POST"))
  }

}
