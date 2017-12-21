package com.datamountaineer.connect.tools

import com.datamountaineer.connect.tools.AppCommand._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Matchers}
import spray.json._
import DefaultJsonProtocol._
import com.google.common.collect.{MapDifference, Maps}
import scala.collection.JavaConverters._

import scala.util.{Success, Try}

class MainCliUnitTests extends FunSuite with Matchers with MockFactory {
  def split(s:String) = s.split(" ")

  test("Valid program arguments are parsed correctly") {
    Cli.parseProgramArgs(split("ps")) shouldEqual Some(Arguments(LIST_ACTIVE, Defaults.BaseUrl, Defaults.Format, None, None))
    Cli.parseProgramArgs(split("ps -e my_endpoint")) shouldEqual Some(Arguments(LIST_ACTIVE, "my_endpoint", Defaults.Format, None, None))
    Cli.parseProgramArgs(split("rm killit -e my_endpoint")) shouldEqual Some(Arguments(DELETE, "my_endpoint", Defaults.Format, Some("killit"), None))
    Cli.parseProgramArgs(split("get getit")) shouldEqual Some(Arguments(GET, Defaults.BaseUrl, Defaults.Format, Some("getit"), None))
    Cli.parseProgramArgs(split("create createit")) shouldEqual Some(Arguments(CREATE, Defaults.BaseUrl, Defaults.Format, Some("createit"), None))
    Cli.parseProgramArgs(split("run runit")) shouldEqual Some(Arguments(RUN, Defaults.BaseUrl, Defaults.Format, Some("runit"), None))
    Cli.parseProgramArgs(split("diff diffit")) shouldEqual Some(Arguments(DIFF, Defaults.BaseUrl, Defaults.Format, Some("diffit"), None))
    Cli.parseProgramArgs(split("plugins")) shouldEqual Some(Arguments(PLUGINS, Defaults.BaseUrl, Defaults.Format, None, None))
    Cli.parseProgramArgs(split("describe myconn")) shouldEqual Some(Arguments(DESCRIBE, Defaults.BaseUrl, Defaults.Format, Some("myconn"), None))
    Cli.parseProgramArgs(split("validate myconn")) shouldEqual Some(Arguments(VALIDATE, Defaults.BaseUrl, Defaults.Format, Some("myconn"), None))
    Cli.parseProgramArgs(split("task_ps myconn")) shouldEqual Some(Arguments(TASK_LIST, Defaults.BaseUrl, Defaults.Format, Some("myconn"), None))
    Cli.parseProgramArgs(split("task_status myconn 0")) shouldEqual Some(Arguments(TASK_STATUS, Defaults.BaseUrl, Defaults.Format, Some("myconn"), Some(0)))
    Cli.parseProgramArgs(split("task_restart myconn 0")) shouldEqual Some(Arguments(TASK_RESTART, Defaults.BaseUrl, Defaults.Format, Some("myconn"), Some(0)))

    Cli.parseProgramArgs(split("restart myconn")) shouldEqual Some(Arguments(RESTART, Defaults.BaseUrl, Defaults.Format, Some("myconn"), None))
    Cli.parseProgramArgs(split("pause myconn")) shouldEqual Some(Arguments(PAUSE, Defaults.BaseUrl, Defaults.Format, Some("myconn"), None))
    Cli.parseProgramArgs(split("resume myconn")) shouldEqual Some(Arguments(RESUME, Defaults.BaseUrl, Defaults.Format, Some("myconn"), None))
  }

  test("Invalid program arguments are rejected") {
    Cli.parseProgramArgs(split("fakecmd")) shouldEqual None
    Cli.parseProgramArgs(split("rm")) shouldEqual None
    Cli.parseProgramArgs(split("create good -j nonsense")) shouldEqual None
  }
}

class ExecuteCommandUnitTests extends FunSuite with Matchers with MockFactory {
  test("Config is parsed correctly") {
    ExecuteCommand.configToMap(Seq("foo = bar", "one = two"), Formats.PROPERTIES) shouldEqual Map(("foo", "bar"), ("one", "two"))
    ExecuteCommand.configToMap(Seq("{", "\"foo\": \"bar\",", "\"one\": \"two\"", "}"), Formats.JSON) shouldEqual Map(("foo", "bar"), ("one", "two"))
  }
}

class ApiUnitTests extends FunSuite with Matchers with MockFactory {
  val URL = new java.net.URI("http://localhost")

  val acceptHeader = "Accept" -> "application/json"
  val contentTypeHeader = "Content-Type" -> "application/json"

  // creates a HttpClient mock that verifies input and produces output
  def verifyingHttpClient(endpoint: String, method: String, status: Int, resp: Option[String], verifyReqBody: String => Unit = a => {}) = {
    new HttpClient {
      def request(url: java.net.URI, method: String, hdrs: Seq[(String, String)], reqBody: Option[String]): Try[(Int, Option[String])] = {
        url shouldEqual URL.resolve(endpoint)
        method shouldEqual method
        hdrs should contain allOf(acceptHeader, contentTypeHeader)
        reqBody.foreach(verifyReqBody)
        Success((status, resp))
      }
    }
  }

  test("activeConnectorNames") {
    new RestKafkaConnectApi(URL,
      verifyingHttpClient("/connectors", "GET", 200, Some("""["a","b"]"""))
    ).activeConnectorNames shouldEqual Success("a" :: "b" :: Nil)
  }

  test("connectorInfo") {
    new RestKafkaConnectApi(URL,
      verifyingHttpClient("/connectors/some", "GET", 200, Some("""{"name":"nom","config":{"k":"v"},"tasks":[{"connector":"c0","task":5}]}"""))
    ).connectorInfo("some") shouldEqual Success(ConnectorInfo("nom", Map("k" -> "v"), List(Task("c0", 5))))
  }

  test("addConnector") {
    val verifyBody = (s: String) => {
      val jobj = s.parseJson.asJsObject
      jobj.fields("name").convertTo[String] shouldBe "some"
      jobj.fields("config").convertTo[Map[String, String]] shouldBe Map("prop" -> "val")
    }
    new RestKafkaConnectApi(URL,
      verifyingHttpClient("/connectors", "POST", 200, Some("""{"name":"nom","config":{"k":"v"},"tasks":[{"connector":"c0","task":5}]}"""), verifyBody)
    ).addConnector("some", Map("prop" -> "val")) shouldEqual Success(ConnectorInfo("nom", Map("k" -> "v"), List(Task("c0", 5))))
  }

  test("diffConnector") {
    val curr = Map(("k", "v"))
    val provided = Map(("k", "d"), ("x", "y"))
    new RestKafkaConnectApi(URL,
      verifyingHttpClient("/connectors/some", "GET", 200, Some("""{"name":"nom","config":{"k":"v"},"tasks":[{"connector":"c0","task":5}]}"""))
    ).diffConnector("some", provided) shouldEqual Success((curr, provided, Maps.difference[String,String](curr.asJava, provided.asJava)))
  }

  test("updateConnector") {
    val verifyBody = (s: String) => {
      val jobj = s.parseJson.convertTo[Map[String, String]] shouldBe Map("prop" -> "val")
    }
    new RestKafkaConnectApi(URL,
      verifyingHttpClient("/connectors/nome/config", "PUT", 200, Some("""{"name":"nom","config":{"k":"v"},"tasks":[{"connector":"c0","task":5}]}"""), verifyBody)
    ).updateConnector("nome", Map("prop" -> "val")) shouldEqual Success(ConnectorInfo("nom", Map("k" -> "v"), List(Task("c0", 5))))
  }

  test("delete") {
    new RestKafkaConnectApi(URL,
      verifyingHttpClient("/connectors/nome", "DELETE", 200, None)
    ).delete("nome") shouldEqual Success()
  }

  test("plugins") {
    val ret = new RestKafkaConnectApi(URL, verifyingHttpClient("/connector-plugins", "GET", 200, Some("""[{"class": "andrew", "type": "class"}]"""))
    ).connectorPlugins()

    ret shouldEqual Success(List(ConnectorPlugins("andrew", "class", None)))
  }

  test("pauseConnector") {
    val mockedClient = mock[HttpClient]
    (mockedClient.request _).expects(URL.resolve("/connectors/nome/pause"), "PUT", Seq(acceptHeader, contentTypeHeader), None)
    (mockedClient.request _).expects(URL.resolve("/connectors/nome/status"), "GET",  Seq(acceptHeader, contentTypeHeader), None)
    new RestKafkaConnectApi(URL, mockedClient).connectorPause("nome")
  }

  test("resumeConnector") {
    val mockedClient = mock[HttpClient]
    (mockedClient.request _).expects(URL.resolve("/connectors/nome/resume"), "PUT", Seq(acceptHeader, contentTypeHeader), None)
    (mockedClient.request _).expects(URL.resolve("/connectors/nome/status"), "GET",  Seq(acceptHeader, contentTypeHeader), None)
    new RestKafkaConnectApi(URL, mockedClient).connectorResume("nome")
  }

  test("tasks") {
    new RestKafkaConnectApi(URL,
      verifyingHttpClient("/connectors/some/tasks", "GET", 200, Some("""[{"id":{"connector":"some","task":0},"config":{"k":"v"}}]"""), _ => ())
    ).tasks("some") shouldEqual Success(List(TaskInfo(TaskId("some", 0), Map("k" -> "v"))))
  }

  test("taskStatus") {
    new RestKafkaConnectApi(URL,
      verifyingHttpClient("/connectors/some/tasks/1/status", "GET", 200, Some("""{"state":"RUNNING","id":1,"worker_id":"10.0.0.9:8083"}"""), _ => ())
    ).taskStatus("some", 1) shouldEqual Success(TaskStatus(1, "RUNNING", "10.0.0.9:8083", None))
  }

  test("taskRestart") {
    new RestKafkaConnectApi(URL,
      verifyingHttpClient("/connectors/some/tasks/1/restart", "POST", 200, None, _ => ())
    ).taskRestart("some", 1) shouldEqual Success()
  }

  test("validate") {

    val config = """
      |{
      |    "name": "FileStreamSinkConnector",
      |    "error_count": 1,
      |    "groups": [
      |        "Common"
      |    ],
      |    "configs": [
      |         {
      |            "definition": {
      |                "name": "topics",
      |                "type": "LIST",
      |                "required": false,
      |                "default_value": "",
      |                "importance": "HIGH",
      |                "documentation": "",
      |                "group": "Common",
      |                "width": "LONG",
      |                "display_name": "Topics",
      |                "dependents": [],
      |                "order": 4
      |             },
      |            "value": {
      |                "name": "topics",
      |                "value": "test-topic",
      |                "recommended_values": [],
      |                "errors": [],
      |                "visible": true
      |            }
      |        }
      |   ]
      |}
    """.stripMargin

    import MyJsonProtocol._
    val jsonAst = config.parseJson
    val valid = jsonAst.convertTo[ConnectorPluginsValidate]

    val ret = new RestKafkaConnectApi(URL, verifyingHttpClient("/connector-plugins/myconn/config/validate", "PUT", 200, Some(config))
    ).connectorPluginsDescribe("myconn")

    ret.get.name shouldEqual valid.name
    ret.get.error_count shouldEqual valid.error_count
    ret.get.configs.head.definition.name shouldEqual valid.configs.head.definition.name
  }
}

class ExecuteCommandTest extends FunSuite with Matchers {
  test("properties") {
    val lines = List(
      "some.key=\\",
      "  val1,\\",
      "  val2"
    )

    val cmd = ExecuteCommand
    val props = cmd.propsToMap(lines)

    props.get("some.key") shouldEqual Some("val1,val2")
  }
}
