/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ly.stealth.mesos.exhibitor

import java.io.IOException
import java.net.{HttpURLConnection, URL, URLEncoder}

import scopt.OptionParser

import scala.io.Source

object Cli {
  def main(args: Array[String]) {
    try {
      exec(args)
    } catch {
      case e: Throwable =>
        System.err.println("Error: " + e.getMessage)
        sys.exit(1)
    }
  }

  def exec(sourceArgs: Array[String]) {
    var args = sourceArgs

    if (args.length == 0) {
      handleHelp()
      println()
      throw new RuntimeException("No command supplied")
    }

    val command = args(0)
    args = args.slice(1, args.length)

    command match {
      case "scheduler" => handleScheduler(args)
      case "add" => handleAdd(args)
    }
  }

  def handleHelp() {
    println("Usage: <command>\n")
  }

  def handleScheduler(args: Array[String]) {
    val parser = new OptionParser[Map[String, String]]("Scheduler") {
      opt[String]('m', "master").required().text("Mesos Master addresses.").action { (value, config) =>
        config.updated("master", value)
      }

      opt[String]('a', "api").optional().text("Binding host:port for http/artifact server.").action { (value, config) =>
        config.updated("api", value)
      }

      opt[String]('u', "user").required().text("Mesos user.").action { (value, config) =>
        config.updated("user", value)
      }
    }

    parser.parse(args, Map()) match {
      case Some(config) =>
        Config.master = config("master")
        Config.user = config("user")
        Config.api = config("api")

        Scheduler.start()
      case None => sys.exit(1)
    }
  }

  def handleAdd(args: Array[String]) {
    val parser = new OptionParser[Map[String, String]]("Scheduler") {
      opt[String]('i', "id").required().text("Server id.").action { (value, config) =>
        config.updated("id", value)
      }

      opt[String]('c', "cpu").optional().text("CPUs for server.").action { (value, config) =>
        config.updated("cpu", value)
      }

      opt[String]('m', "mem").optional().text("Memory for server.").action { (value, config) =>
        config.updated("mem", value)
      }

      opt[String]('a', "api").required().text("Binding host:port for http/artifact server.").action { (value, config) =>
        config.updated("api", value)
      }
    }

    parser.parse(args, Map()) match {
      case Some(config) =>
        Config.api = config("api")
        sendRequest("/add", config)
      case None => sys.exit(1)
    }
  }

  private[exhibitor] def sendRequest(uri: String, params: Map[String, String]) {
    def queryString(params: Map[String, String]): String = {
      var s = ""
      params.foreach { case (name, value) =>
        if (!s.isEmpty) s += "&"
        s += URLEncoder.encode(name, "utf-8")
        if (value != null) s += "=" + URLEncoder.encode(value, "utf-8")
      }
      s
    }

    val qs: String = queryString(params)
    val url: String = Config.api + (if (Config.api.endsWith("/")) "" else "/") + "api" + uri + "?" + qs

    val connection: HttpURLConnection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    var response: String = null
    try {
      try {
        response = Source.fromInputStream(connection.getInputStream).getLines().mkString
      }
      catch {
        case e: IOException =>
          if (connection.getResponseCode != 200) throw new IOException(connection.getResponseCode + " - " + connection.getResponseMessage)
          else throw e
      }
    } finally {
      connection.disconnect()
    }

    //TODO return something
  }
}
