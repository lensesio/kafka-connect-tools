package com.datamountaineer.connect.tools

import org.scalatest.{FunSuite, Matchers}
import org.scalamock.scalatest.MockFactory
import spray.json._
import spray.json.{JsonReader, DefaultJsonProtocol}
import DefaultJsonProtocol._

import scala.util.{Success, Try}

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
    new KafkaConnectApi(URL,
      verifyingHttpClient("/connectors", "GET", 200, Some("""["a","b"]"""))
    ).activeConnectorNames shouldEqual "a" :: "b" :: Nil
  }

  test("connectorInfo") {
    new KafkaConnectApi(URL,
      verifyingHttpClient("/connectors/some", "GET", 200, Some("""{"name":"nom","config":{"k":"v"},"tasks":[{"connector":"c0","task":5}]}"""))
    ).connectorInfo("some") shouldEqual ConnectorInfo("nom", Map("k" -> "v"), List(Task("c0", 5)))
  }

  test("addConnector") {
    val verifyBody = (s: String) => {
      val jobj = s.parseJson.asJsObject
      jobj.fields("name").convertTo[String] shouldBe "some"
      jobj.fields("config").convertTo[Map[String, String]] shouldBe Map("prop" -> "val")
    }
    new KafkaConnectApi(URL,
      verifyingHttpClient("/connectors", "POST", 200, Some("""{"name":"nom","config":{"k":"v"},"tasks":[{"connector":"c0","task":5}]}"""), verifyBody)
    ).addConnector("some", Map("prop" -> "val")) shouldEqual ConnectorInfo("nom", Map("k" -> "v"), List(Task("c0", 5)))
  }

  test("updateConnector") {
    val verifyBody = (s: String) => {
      val jobj = s.parseJson.convertTo[Map[String, String]] shouldBe Map("prop" -> "val")
    }
    new KafkaConnectApi(URL,
      verifyingHttpClient("/connectors/nome/config", "PUT", 200, Some("""{"name":"nom","config":{"k":"v"},"tasks":[{"connector":"c0","task":5}]}"""), verifyBody)
    ).updateConnector("nome", Map("prop" -> "val")) shouldEqual ConnectorInfo("nom", Map("k" -> "v"), List(Task("c0", 5)))
  }

  test("delete") {
    new KafkaConnectApi(URL,
      verifyingHttpClient("/connectors/nome", "DELETE", 200, None)
    ).delete("nome")
  }
}