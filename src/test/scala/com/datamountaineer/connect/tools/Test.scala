package com.datamountaineer.connect.tools

import AppCommand._
import org.scalatest.{FunSuite, Matchers}
import org.scalamock.scalatest.MockFactory
import spray.json._
import spray.json.{JsonReader, DefaultJsonProtocol}
import DefaultJsonProtocol._

import scala.util.{Success, Try}

class MainCliUnitTests extends FunSuite with Matchers with MockFactory {
  def split(s:String) = s.split(" ")

  test("Valdid program arguments are parsed correctly") {
    Cli.parseProgramArgs(split("ps")) shouldEqual Some(Arguments(LIST_ACTIVE, Defaults.BaseUrl, None))
    Cli.parseProgramArgs(split("ps -e my_endpoint")) shouldEqual Some(Arguments(LIST_ACTIVE, "my_endpoint", None))
    Cli.parseProgramArgs(split("rm killit -e my_endpoint")) shouldEqual Some(Arguments(DELETE, "my_endpoint", Some("killit")))
    Cli.parseProgramArgs(split("get getit")) shouldEqual Some(Arguments(GET, Defaults.BaseUrl, Some("getit")))
    Cli.parseProgramArgs(split("create createit")) shouldEqual Some(Arguments(CREATE, Defaults.BaseUrl, Some("createit")))
    Cli.parseProgramArgs(split("run runit")) shouldEqual Some(Arguments(RUN, Defaults.BaseUrl, Some("runit")))
  }

  test("Invaldid program arguments are rejected") {
    Cli.parseProgramArgs(split("fakecmd")) shouldEqual None
    Cli.parseProgramArgs(split("rm")) shouldEqual None
    Cli.parseProgramArgs(split("create good -j nonense")) shouldEqual None
  }
}

class ApiUnitTests extends FunSuite with Matchers with MockFactory {
  val URL = new java.net.URI("http://localhost")

  val acceptHeader = "Accept" -> "application/json"
  val contentTypeHeader = "Content-Type" -> "application/json"

  // creates a HttpClient mock that verifies input and produces output
  def verifyingHttpClient(endpoint: String, method: String, status: Int, resp: Option[String], verifyBody: String => Unit = a => {}) = {
    new HttpClient {
      def request(url: java.net.URI, method: String, hdrs: Seq[(String, String)], reqBody: Option[String]): Try[(Int, Option[String])] = {
        url shouldEqual URL.resolve(endpoint)
        method shouldEqual method
        hdrs should contain allOf(acceptHeader, contentTypeHeader)
        reqBody.foreach(verifyBody)
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
}