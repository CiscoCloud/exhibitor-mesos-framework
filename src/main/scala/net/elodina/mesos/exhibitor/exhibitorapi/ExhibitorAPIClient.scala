package net.elodina.mesos.exhibitor.exhibitorapi

import java.io.{DataOutputStream, IOException}
import java.net.{HttpURLConnection, URL}

import org.apache.log4j.Logger
import play.api.libs.json._

import scala.io.Source
import scala.util.{Failure, Success, Try}

object ExhibitorAPIClient {
  private val logger = Logger.getLogger(ExhibitorAPIClient.getClass)

  private val getSystemStateURL = "exhibitor/v1/config/get-state"
  private val setConfigURL = "exhibitor/v1/config/set"
  private val getStatus = "exhibitor/v1/cluster/status"

  def getSystemState(baseUrl: String): SharedConfig = {
    val url = s"$baseUrl/$getSystemStateURL"
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      readResponse(connection, response => {
        import SharedConfig._
        (Json.parse(response) \ "config").validate[SharedConfig] match {
          case JsSuccess(config, _) => config
          case JsError(error) => throw new IllegalStateException(error.toString())
        }
      })
    } finally {
      connection.disconnect()
    }
  }

  def setConfig(config: SharedConfig, baseUrl: String) {
    logger.debug(s"Trying to save shared config: $config")

    val url = s"$baseUrl/$setConfigURL"
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Content-Type", "application/json")

      connection.setUseCaches(false)
      connection.setDoInput(true)
      connection.setDoOutput(true)
      connection.setReadTimeout(10000)

      val out = new DataOutputStream(connection.getOutputStream)
      out.writeBytes(Json.prettyPrint(Json.toJson(config)))
      out.flush()
      out.close()

      readResponse(connection, response => {
        import Result._
        Json.parse(response).validate[Result] match {
          case JsSuccess(result, _) => if (!result.succeeded) throw new IllegalStateException(result.message)
          case JsError(error) => throw new IllegalStateException(error.toString())
        }
      })
    } finally {
      connection.disconnect()
    }
  }

  def getClusterStatus(baseUrl: String): Seq[ExhibitorServerStatus] = {
    val url = s"$baseUrl/$getStatus"
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      readResponse(connection, response => {
        import ExhibitorServerStatus._
        Json.parse(response).validate[Seq[ExhibitorServerStatus]] match {
          case JsSuccess(serverStatuses, _) => serverStatuses
          case JsError(error) => throw new IllegalStateException(error.toString())
        }
      })
    } finally {
      connection.disconnect()
    }
  }

  private def readResponse[A](connection: HttpURLConnection, reader: String => A): A = {
    Try(Source.fromInputStream(connection.getInputStream).getLines().mkString) match {
      case Success(response) => reader(response)
      case Failure(e) =>
        if (connection.getResponseCode != 200) throw new IOException(connection.getResponseCode + " - " + connection.getResponseMessage)
        else throw e
    }
  }
}
