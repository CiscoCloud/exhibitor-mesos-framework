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

import play.api.libs.json.{JsValue, Json}
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

  def exec(args: Array[String]) {
    if (args.length == 0) {
      handleHelp()
      println()
      throw new RuntimeException("No command supplied")
    }

    val command = args.head
    val commandArgs = args.tail

    command match {
      case "help" => if (commandArgs.isEmpty) handleHelp() else handleHelp(commandArgs.head)
      case "scheduler" => handleScheduler(commandArgs)
      case "add" => handleAdd(commandArgs)
      case "start" => handleStart(commandArgs)
      case "stop" => handleStop(commandArgs)
      case "remove" => handleRemove(commandArgs)
      case "status" => handleStatus(commandArgs)
      case "config" => handleConfig(commandArgs)
    }
  }

  def handleHelp(command: String = "") {
    command match {
      case "" =>
        println("Usage: <command>\n")
        printGenericHelp()
      case "scheduler" => Parsers.scheduler.showUsage
      case "add" => Parsers.add.showUsage
      case "start" => Parsers.start.showUsage
      case "stop" => Parsers.stop.showUsage
      case "remove" => Parsers.remove.showUsage
      case "status" => Parsers.status.showUsage
      case "config" => Parsers.config.showUsage
      case _ =>
        println(s"Unknown command: $command\n")
        printGenericHelp()
    }
  }

  def handleScheduler(args: Array[String]) {
    Parsers.scheduler.parse(args, Map()) match {
      case Some(config) =>
        resolveApi(config.get("api"))

        Config.master = config("master")
        Config.user = config("user")
        config.get("debug").foreach(debug => Config.debug = debug.toBoolean)

        Scheduler.start()
      case None => sys.exit(1)
    }
  }

  def handleAdd(args: Array[String]) {
    val id = getID(args, () => Parsers.add.showUsage)
    Parsers.add.parse(args.tail, Map("id" -> id)) match {
      case Some(config) =>
        resolveApi(config.get("api"))

        val server = sendRequest("/add", config).as[ExhibitorServer]
        printLine("Server added")
        printLine()
        printExhibitorServer(server)
      case None => sys.exit(1)
    }
  }

  def handleStart(args: Array[String]) {
    val id = getID(args, () => Parsers.start.showUsage)
    Parsers.start.parse(args.tail, Map("id" -> id)) match {
      case Some(config) =>
        resolveApi(config.get("api"))

        val server = sendRequest("/start", config).as[ExhibitorServer]
        printLine("Started server")
        printLine()
        printExhibitorServer(server)
      case None => sys.exit(1)
    }
  }

  def handleStop(args: Array[String]) {
    val id = getID(args, () => Parsers.stop.showUsage)
    Parsers.stop.parse(args.tail, Map("id" -> id)) match {
      case Some(config) =>
        resolveApi(config.get("api"))

        val server = sendRequest("/stop", config).as[ExhibitorServer]
        printLine(s"Stopped server ${server.id}")
      case None => sys.exit(1)
    }
  }

  def handleRemove(args: Array[String]) {
    val id = getID(args, () => Parsers.remove.showUsage)
    Parsers.remove.parse(args.tail, Map("id" -> id)) match {
      case Some(config) =>
        resolveApi(config.get("api"))

        val server = sendRequest("/remove", config).as[ExhibitorServer]
        printLine(s"Removed server ${server.id}")
      case None => sys.exit(1)
    }
  }

  def handleStatus(args: Array[String]) {
    Parsers.status.parse(args, Map()) match {
      case Some(config) =>
        resolveApi(config.get("api"))

        val cluster = sendRequest("/status", config).as[List[ExhibitorServer]]
        printCluster(cluster)
      case None => sys.exit(1)
    }
  }

  def handleConfig(args: Array[String]) {
    val id = getID(args, () => Parsers.config.showUsage)
    Parsers.config.parse(args.tail, Map("id" -> id)) match {
      case Some(config) =>
        resolveApi(config.get("api"))

        val server = sendRequest("/config", config).as[ExhibitorServer]
        printExhibitorServer(server)
      case None => sys.exit(1)
    }
  }

  private def getID(args: Array[String], usage: () => Unit): String = {
    args.headOption match {
      case Some(id) => id
      case None =>
        usage()
        sys.exit(1)
    }
  }

  private def resolveApi(apiOption: Option[String]) {
    if (Config.api != null) return

    if (apiOption.isDefined) {
      Config.api = apiOption.get
      return
    }

    if (System.getenv("EM_API") != null) {
      Config.api = System.getenv("EM_API")
      return
    }

    throw new IllegalArgumentException("Undefined API url. Please provide either a CLI --api option or EM_API env.")
  }

  private[exhibitor] def sendRequest(uri: String, params: Map[String, String]): JsValue = {
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

    Json.parse(response)
  }

  private def printLine(s: AnyRef = "", indent: Int = 0) = println("  " * indent + s)

  private def printGenericHelp() {
    printLine("Commands:")
    printLine("help       - print this message.", 1)
    printLine("help [cmd] - print command-specific help.", 1)
    printLine("scheduler  - start scheduler.", 1)
    printLine("status     - print cluster status.", 1)
    printLine("add        - add servers to cluster.", 1)
    printLine("config     - configure servers in cluster.", 1)
    printLine("start      - start servers in cluster.", 1)
    printLine("stop       - stop servers in cluster.", 1)
    printLine("remove     - remove servers in cluster.", 1)
  }

  private def printCluster(cluster: List[ExhibitorServer]) {
    printLine("cluster:")
    cluster.foreach(printExhibitorServer(_, 1))
  }

  private def printExhibitorServer(server: ExhibitorServer, indent: Int = 0) {
    printLine("server:", indent)
    printLine(s"id: ${server.id}", indent + 1)
    printLine(s"state: ${server.state}", indent + 1)
    if (!server.config.hostname.isEmpty && server.config.exhibitorConfig.get("port").isDefined) {
      printLine(s"endpoint: ${server.url}/exhibitor/v1/ui/index.html", indent + 1)
    }
    printTaskConfig(server.config, indent + 1)
    printLine()
  }

  private def printTaskConfig(config: TaskConfig, indent: Int) {
    printLine("exhibitor config:", indent)
    config.exhibitorConfig.foreach { case (k, v) =>
      printLine(s"$k: $v", indent + 1)
    }
    printLine("shared config overrides:", indent)
    config.sharedConfigOverride.foreach { case (k, v) =>
      printLine(s"$k: $v", indent + 1)
    }
    printLine(s"cpu: ${config.cpus}", indent)
    printLine(s"mem: ${config.mem}", indent)
  }

  private object Parsers {
    val scheduler = new OptionParser[Map[String, String]]("scheduler") {
      opt[String]('m', "master").required().text("Mesos Master addresses. Required.").action { (value, config) =>
        config.updated("master", value)
      }

      opt[String]('a', "api").optional().text("Binding host:port for http/artifact server. Optional if EM_API env is set.").action { (value, config) =>
        config.updated("api", value)
      }

      opt[String]('u', "user").required().text("Mesos user. Required.").action { (value, config) =>
        config.updated("user", value)
      }

      opt[Boolean]('d', "debug").optional().text("Debug mode. Optional. Defaults to false.").action { (value, config) =>
        config.updated("debug", value.toString)
      }
    }

    val add = new OptionParser[Map[String, String]]("add <id>") {
      opt[String]('c', "cpu").optional().text(s"CPUs for server. Optional.").action { (value, config) =>
        config.updated("cpu", value)
      }

      opt[String]('m', "mem").optional().text("Memory for server. Optional.").action { (value, config) =>
        config.updated("mem", value)
      }

      opt[String]('a', "api").optional().text("Binding host:port for http/artifact server. Optional if EM_API env is set.").action { (value, config) =>
        config.updated("api", value)
      }
    }

    val start = defaultParser("start <id>")

    val stop = defaultParser("stop <id>")

    val remove = defaultParser("remove <id>")

    val status = defaultParser("status")

    val config = new OptionParser[Map[String, String]]("config <id>") {
      opt[String]('a', "api").optional().text("Binding host:port for http/artifact server. Optional if EM_API env is set.").action { (value, config) =>
        config.updated("api", value)
      }

      opt[String]("configtype").optional().text("Config type to use: s3 or zookeeper. Optional.").action { (value, config) =>
        config.updated("configtype", value)
      }

      opt[String]("zkconfigconnect").optional().text("The initial connection string for ZooKeeper shared config storage. E.g: host1:2181,host2:2181... Optional.").action { (value, config) =>
        config.updated("zkconfigconnect", value)
      }

      opt[String]("zkconfigzpath").optional().text("The base ZPath that Exhibitor should use. E.g: /exhibitor/config. Optional.").action { (value, config) =>
        config.updated("zkconfigzpath", value)
      }

      opt[String]("s3credentials").optional().text("Credentials to use for s3backup or s3config. Optional.").action { (value, config) =>
        config.updated("s3credentials", value)
      }

      opt[String]("s3region").optional().text("Region for S3 calls (e.g. \"eu-west-1\"). Optional.").action { (value, config) =>
        config.updated("s3region", value)
      }

      opt[String]("s3config").optional().text("The bucket name and key to store the config (s3credentials may be provided as well). Argument is [bucket name]:[key]. Optional.").action { (value, config) =>
        config.updated("s3config", value)
      }

      opt[String]("s3configprefix").optional().text("When using AWS S3 shared config files, the prefix to use for values such as locks. Optional.").action { (value, config) =>
        config.updated("s3configprefix", value)
      }

      // shared configs
      opt[String]("zookeeper-install-directory").optional().text("Zookeeper install directory shared config. Optional.").action { (value, config) =>
        config.updated("zookeeper-install-directory", value)
      }

      opt[String]("zookeeper-data-directory").optional().text("Zookeeper data directory shared config. Optional.").action { (value, config) =>
        config.updated("zookeeper-data-directory", value)
      }
    }

    private def defaultParser(descr: String): OptionParser[Map[String, String]] = new OptionParser[Map[String, String]](descr) {
      opt[String]('a', "api").optional().text("Binding host:port for http/artifact server. Optional if EM_API env is set.").action { (value, config) =>
        config.updated("api", value)
      }
    }
  }

}
