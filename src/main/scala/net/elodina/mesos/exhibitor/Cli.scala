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

package net.elodina.mesos.exhibitor

import java.io.{IOException, PrintStream}
import java.net.{HttpURLConnection, URL, URLEncoder}

import net.elodina.mesos.exhibitor.exhibitorapi.ExhibitorServerStatus
import net.elodina.mesos.util.Repr
import play.api.libs.json.{JsValue, Json}
import scopt.OptionParser

import scala.concurrent.duration.Duration
import scala.io.Source
import scala.util.{Failure, Success, Try}

object Cli {
  private[exhibitor] var out: PrintStream = System.out

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
      printLine()
      throw CliError("No command supplied")
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
      case _ => throw CliError(s"Unknown command: $command\n")
    }
  }

  def handleHelp(command: String = "") {
    command match {
      case "" =>
        printLine("Usage: <command>\n")
        printGenericHelp()
      case "scheduler" => Parsers.scheduler.showUsage
      case "add" => Parsers.add.showUsage
      case "start" => Parsers.start.showUsage
      case "stop" => Parsers.stop.showUsage
      case "remove" => Parsers.remove.showUsage
      case "status" => Parsers.status.showUsage
      case "config" => Parsers.config.showUsage
      case _ =>
        printLine(s"Unknown command: $command\n")
        printGenericHelp()
    }
  }

  def handleScheduler(args: Array[String]) {
    Parsers.scheduler.parse(args, Map()) match {
      case Some(config) =>
        resolveApi(config.get(ConfigNames.API))

        Config.master = config(ConfigNames.MASTER)
        config.get(ConfigNames.USER).foreach(user => Config.user = user)
        config.get(ConfigNames.FRAMEWORK_NAME).foreach(name => Config.frameworkName = name)
        config.get(ConfigNames.FRAMEWORK_TIMEOUT).foreach(timeout => Config.frameworkTimeout = Duration(timeout))
        config.get(ConfigNames.FRAMEWORK_ROLE).foreach(role => Config.frameworkRole = role)
        config.get(ConfigNames.STORAGE).foreach(storage => Config.storage = storage)
        config.get(ConfigNames.ENSEMBLE_MODIFY_RETRIES).foreach(retries => Config.ensembleModifyRetries = retries.toInt)
        config.get(ConfigNames.ENSEMBLE_MODIFY_BACKOFF).foreach(backoff => Config.ensembleModifyBackoff = backoff.toLong)
        config.get(ConfigNames.DEBUG).foreach(debug => Config.debug = debug.toBoolean)

        Scheduler.start()
      case None => throw CliError("Invalid arguments")
    }
  }

  def handleAdd(args: Array[String]) {
    val id = getID(args, () => Parsers.add.showUsage)
    Parsers.add.parse(args.tail, Map(ConfigNames.ID -> id)) match {
      case Some(config) =>
        resolveApi(config.get(ConfigNames.API))

        val response = sendRequest("/add", config).as[ApiResponse]
        printLine(response.message)
        printLine()
        response.value.foreach(printCluster)
      case None => throw CliError("Invalid arguments")
    }
  }

  def handleStart(args: Array[String]) {
    val id = getID(args, () => Parsers.start.showUsage)
    Parsers.start.parse(args.tail, Map(ConfigNames.ID -> id)) match {
      case Some(config) =>
        resolveApi(config.get(ConfigNames.API))

        val response = sendRequest("/start", config).as[ApiResponse]
        if (!response.success) throw CliError(response.message)

        printLine(response.message)
        printLine()
        response.value.foreach(printCluster)
      case None => throw CliError("Invalid arguments")
    }
  }

  def handleStop(args: Array[String]) {
    val id = getID(args, () => Parsers.stop.showUsage)
    Parsers.stop.parse(args.tail, Map(ConfigNames.ID -> id)) match {
      case Some(config) =>
        resolveApi(config.get(ConfigNames.API))

        val response = sendRequest("/stop", config).as[ApiResponse]
        printLine(response.message)
        printLine()
      case None => throw CliError("Invalid arguments")
    }
  }

  def handleRemove(args: Array[String]) {
    val id = getID(args, () => Parsers.remove.showUsage)
    Parsers.remove.parse(args.tail, Map(ConfigNames.ID -> id)) match {
      case Some(config) =>
        resolveApi(config.get(ConfigNames.API))

        val response = sendRequest("/remove", config).as[ApiResponse]
        printLine(response.message)
        printLine()
      case None => throw CliError("Invalid arguments")
    }
  }

  def handleStatus(args: Array[String]) {
    Parsers.status.parse(args, Map()) match {
      case Some(config) =>
        resolveApi(config.get(ConfigNames.API))

        val clusterStatus = sendRequest("/status", config).as[ClusterStatusResponse]
        printClusterStatus(clusterStatus.value.get)
      case None => throw CliError("Invalid arguments")
    }
  }

  def handleConfig(args: Array[String]) {
    val id = getID(args, () => Parsers.config.showUsage)
    Parsers.config.parse(args.tail, Map(ConfigNames.ID -> id)) match {
      case Some(config) =>
        resolveApi(config.get(ConfigNames.API))

        val response = sendRequest("/config", config).as[ApiResponse]
        printLine(response.message)
        printLine()
        response.value.foreach(printCluster)
      case None => throw CliError("Invalid arguments")
    }
  }

  private def getID(args: Array[String], usage: () => Unit): String = {
    args.headOption match {
      case Some(ids) => Try(ids.split(",").map(Util.Range(_))) match {
        case Success(_) => ids
        case Failure(e) => throw CliError(s"Invalid id range: ${e.getMessage}")
      }
      case None =>
        usage()
        throw CliError("Argument required")
    }
  }

  private def resolveApi(apiOption: Option[String]) {
    if (Config.api != null) return

    if (apiOption.isDefined) {
      Config.api = apiOption.get
      return
    }

    if (System.getenv(ConfigNames.API_ENV) != null) {
      Config.api = System.getenv(ConfigNames.API_ENV)
      return
    }

    throw CliError(s"Undefined API url. Please provide either a CLI --api option or ${ConfigNames.API_ENV} env.")
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

  private def printLine(s: AnyRef = "", indent: Int = 0) = out.println("  " * indent + s)

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

  private def printConstraintExamples() {
    printLine("constraint examples:")
    printLine("like:slave0    - value equals 'slave0'", 1)
    printLine("unlike:slave0  - value is not equal to 'slave0'", 1)
    printLine("like:slave.*   - value starts with 'slave'", 1)
    printLine("unique         - all values are unique", 1)
    printLine("cluster        - all values are the same", 1)
    printLine("cluster:slave0 - value equals 'slave0'", 1)
    printLine("groupBy        - all values are the same", 1)
    printLine("groupBy:3      - all values are within 3 different groups", 1)
  }

  private def printCluster(cluster: Cluster) {
    printLine("cluster:")
    cluster.servers().foreach(s => printExhibitorServer(s, None, 1))
  }

  private def printClusterStatus(clusterStatus: ClusterStatus) {
    printLine("cluster:")
    clusterStatus.serverStatuses.foreach(ss =>
      printExhibitorServer(ss.server, ss.exhibitorClusterView, 1))
  }

  private def printExhibitorServer(server: Exhibitor, exhibitorClusterView: Option[Seq[ExhibitorServerStatus]], indent: Int = 0) {
    printLine("server:", indent)
    printLine(s"id: ${server.id}", indent + 1)
    printLine(s"state: ${server.state}", indent + 1)
    if (!server.config.hostname.isEmpty && server.config.exhibitorConfig.get(ConfigNames.PORT).isDefined) {
      printLine(s"endpoint: ${server.url}/exhibitor/v1/ui/index.html", indent + 1)
    }
    if (server.constraints.nonEmpty)
      printLine(s"constraints: ${Util.formatConstraints(server.constraints)}", indent + 1)
    printStickiness(server.stickiness, indent + 1)
    printTaskConfig(server.config, indent + 1)
    exhibitorClusterView.foreach(x => printExhibitorClusterStateView(x, indent + 1))

    printLine()
  }

  private def printExhibitorClusterStateView(statuses: Seq[ExhibitorServerStatus], indent: Int): Unit = {
    printLine("exhibitor cluster view:", indent)
    val statusString = statuses.sortBy(_.hostname).map {
      s =>
        val lof = if (s.isLeader) "L" else "F"
        s"[${s.hostname}, ${s.description}, ${s.code}, $lof]"
    }.mkString("; ")

    printLine(statusString, indent + 1)
  }

  private def printStickiness(stickiness: Stickiness, indent: Int) {
    var stickinessStr = "stickiness:"
    stickinessStr += " period: " + stickiness.period
    stickinessStr += stickiness.hostname.map(h => ", hostname:" + h).getOrElse("")
    stickinessStr += stickiness.stopTime.map(s => ", expires:" + Repr.dateTime(s)).getOrElse("")
    printLine(stickinessStr, indent)
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
    printLine(s"sharedConfigChangeBackoff: ${config.sharedConfigChangeBackoff}", indent)
    val ports = config.ports match {
      case Nil => "auto"
      case _ => config.ports.mkString(",")
    }
    printLine(s"port: $ports", indent)
  }

  private object Parsers {
    val scheduler = new CliOptionParser("scheduler") {
      opt[String]('m', ConfigNames.MASTER).required().text("Mesos Master addresses. Required.").action { (value, config) =>
        config.updated(ConfigNames.MASTER, value)
      }

      opt[String]('a', ConfigNames.API).optional().text(s"Binding host:port for http/artifact server. Optional if ${ConfigNames.API_ENV} env is set.").action { (value, config) =>
        config.updated(ConfigNames.API, value)
      }

      opt[String]('u', ConfigNames.USER).optional().text("Mesos user. Defaults to current system user.").action { (value, config) =>
        config.updated(ConfigNames.USER, value)
      }

      opt[String](ConfigNames.FRAMEWORK_NAME).optional().text("Mesos framework name. Defaults to exhibitor. Optional").action { (value, config) =>
        config.updated(ConfigNames.FRAMEWORK_NAME, value)
      }

      opt[Duration](ConfigNames.FRAMEWORK_TIMEOUT).optional().text("Mesos framework failover timeout. Allows to recover from failure before killing running tasks. Should be a parsable Scala Duration value. Defaults to 30 days. Optional").action { (value, config) =>
        config.updated(ConfigNames.FRAMEWORK_TIMEOUT, value.toString)
      }

      opt[String](ConfigNames.FRAMEWORK_ROLE).optional().text("Mesos framework role. Defaults to '*'. Optional").action { (value, config) =>
        config.updated(ConfigNames.FRAMEWORK_ROLE, value)
      }

      opt[String](ConfigNames.STORAGE).required().text("Storage for cluster state. Examples: file:exhibitor-mesos.json; zk:master:2181/exhibitor-mesos. Required.").action { (value, config) =>
        config.updated(ConfigNames.STORAGE, value)
      }

      opt[Int](ConfigNames.ENSEMBLE_MODIFY_RETRIES).optional().text("Number of retries to modify (add/remove server) ensemble. Defaults to 60. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ENSEMBLE_MODIFY_RETRIES, value.toString)
      }

      opt[Long](ConfigNames.ENSEMBLE_MODIFY_BACKOFF).optional().text("Backoff between retries to modify (add/remove server) ensemble in milliseconds. Defaults to 1000. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ENSEMBLE_MODIFY_BACKOFF, value.toString)
      }

      opt[Boolean]('d', ConfigNames.DEBUG).optional().text("Debug mode. Optional. Defaults to false.").action { (value, config) =>
        config.updated(ConfigNames.DEBUG, value.toString)
      }
    }

    val add = new CliOptionParser("add <id>") {
      override def showUsage {
        super.showUsage
        printLine()
        printConstraintExamples()
      }

      opt[String]('c', ConfigNames.CPU).optional().text(s"CPUs for server. Optional.").action { (value, config) =>
        config.updated(ConfigNames.CPU, value)
      }

      opt[String]('m', ConfigNames.MEM).optional().text("Memory for server. Optional.").action { (value, config) =>
        config.updated(ConfigNames.MEM, value)
      }

      opt[String](ConfigNames.CONSTRAINTS).optional().text("Constraints (hostname=like:master,rack=like:1.*). See below. Defaults to 'hostname=unique'. Optional.").action { (value, config) =>
        config.updated(ConfigNames.CONSTRAINTS, value)
      }

      opt[Long]('b', ConfigNames.SHARED_CONFIG_CHANGE_BACKOFF).optional().text("Backoff between checks whether the shared configuration changed in milliseconds. Defaults to 10000. Optional.").action { (value, config) =>
        config.updated(ConfigNames.SHARED_CONFIG_CHANGE_BACKOFF, value.toString)
      }

      opt[String]('a', ConfigNames.API).optional().text(s"Binding host:port for http/artifact server. Optional if ${ConfigNames.API_ENV} env is set.").action { (value, config) =>
        config.updated(ConfigNames.API, value)
      }

      opt[String](ConfigNames.PORT).optional().text("Port ranges to accept, when offer is issued. Optional").action { (value, config) =>
        config.updated(ConfigNames.PORT, value)
      }
    }

    val start = new CliOptionParser("start <id>") {
      opt[String]('a', ConfigNames.API).optional().text(s"Binding host:port for http/artifact server. Optional if ${ConfigNames.API_ENV} env is set.").action { (value, config) =>
        config.updated(ConfigNames.API, value)
      }

      opt[Duration](ConfigNames.TIMEOUT).optional().text("Time to wait for server to be started. Should be a parsable Scala Duration value. Defaults to 60s. Optional").action { (value, config) =>
        config.updated(ConfigNames.TIMEOUT, value.toString)
      }
    }

    val stop = defaultParser("stop <id>")

    val remove = defaultParser("remove <id>")

    val status = defaultParser("status")

    val config = new CliOptionParser("config <id>") {
      opt[String]('a', ConfigNames.API).optional().text(s"Binding host:port for http/artifact server. Optional if ${ConfigNames.API_ENV} env is set.").action { (value, config) =>
        config.updated(ConfigNames.API, value)
      }

      opt[String](ConfigNames.STICKINESS_PERIOD).optional().text("Stickiness period to preserve same node for Exhibitor server (5m, 10m, 1h).").action { (value, config) =>
        config.updated(ConfigNames.STICKINESS_PERIOD, value)
      }

      // Exhibitor configs
      opt[String](ConfigNames.SHARED_CONFIG_TYPE).optional().text("Config type to use: s3 or zookeeper. Optional.").action { (value, config) =>
        config.updated(ConfigNames.SHARED_CONFIG_TYPE, value)
      }

      opt[String](ConfigNames.SHARED_CONFIG_CHECK_MS).optional().text("Period (ms) to check for shared config updates. Optional.").action { (value, config) =>
        config.updated(ConfigNames.SHARED_CONFIG_CHECK_MS, value)
      }

      opt[String](ConfigNames.DEFAULT_SHARED_CONFIG).optional().text("Full path to a file that contains initial/default values for Exhibitor/ZooKeeper config values. The file is a standard property file. Optional.").action { (value, config) =>
        config.updated(ConfigNames.DEFAULT_SHARED_CONFIG, value)
      }

      opt[String](ConfigNames.HEADING_TEXT).optional().text("Extra text to display in UI header. Optional.").action { (value, config) =>
        config.updated(ConfigNames.HEADING_TEXT, value)
      }

      opt[String](ConfigNames.HOSTNAME).optional().text("Hostname to use for this JVM. Optional.").action { (value, config) =>
        config.updated(ConfigNames.HOSTNAME, value)
      }

      opt[String](ConfigNames.JQUERY_STYLE).optional().text("Styling used for the JQuery-based UI. Optional.").action { (value, config) =>
        config.updated(ConfigNames.JQUERY_STYLE, value)
      }

      opt[String](ConfigNames.LOGLINES).optional().text("Max lines of logging to keep in memory for display. Default is 1000. Optional.").action { (value, config) =>
        config.updated(ConfigNames.LOGLINES, value)
      }

      opt[String](ConfigNames.NODE_MODIFICATION).optional().text("If true, the Explorer UI will allow nodes to be modified (use with caution). Default is true. Optional.").action { (value, config) =>
        config.updated(ConfigNames.NODE_MODIFICATION, value)
      }

      opt[String](ConfigNames.PREFS_PATH).optional().text("Certain values (such as Control Panel values) are stored in a preferences file. By default, Preferences.userRoot() is used. Optional.").action { (value, config) =>
        config.updated(ConfigNames.PREFS_PATH, value)
      }

      opt[String](ConfigNames.SERVO).optional().text("true/false (default is false). If enabled, ZooKeeper will be queried once a minute for its state via the 'mntr' four letter word (this requires ZooKeeper 3.4.x+). Servo will be used to publish this data via JMX. Optional.").action { (value, config) =>
        config.updated(ConfigNames.SERVO, value)
      }

      opt[String](ConfigNames.TIMEOUT).optional().text("Connection timeout (ms) for ZK connections. Default is 30000. Optional.").action { (value, config) =>
        config.updated(ConfigNames.TIMEOUT, value)
      }

      // S3 options
      opt[String](ConfigNames.S3_CREDENTIALS).optional().text("Credentials to use for s3backup or s3config. Optional.").action { (value, config) =>
        config.updated(ConfigNames.S3_CREDENTIALS, value)
      }

      opt[String](ConfigNames.S3_REGION).optional().text("Region for S3 calls (e.g. \"eu-west-1\"). Optional.").action { (value, config) =>
        config.updated(ConfigNames.S3_REGION, value)
      }

      // Configuration Options for Type "s3"
      opt[String](ConfigNames.S3_CONFIG).optional().text("The bucket name and key to store the config (s3credentials may be provided as well). Argument is [bucket name]:[key]. Optional.").action { (value, config) =>
        config.updated(ConfigNames.S3_CONFIG, value)
      }

      opt[String](ConfigNames.S3_CONFIG_PREFIX).optional().text("When using AWS S3 shared config files, the prefix to use for values such as locks. Optional.").action { (value, config) =>
        config.updated(ConfigNames.S3_CONFIG_PREFIX, value)
      }

      // Configuration Options for Type "zookeeper"
      opt[String](ConfigNames.ZK_CONFIG_CONNECT).optional().text("The initial connection string for ZooKeeper shared config storage. E.g: host1:2181,host2:2181... Optional.").action { (value, config) =>
        config.updated(ConfigNames.ZK_CONFIG_CONNECT, value)
      }

      opt[String](ConfigNames.ZK_CONFIG_EXHIBITOR_PATH).optional().text("Used if the ZooKeeper shared config is also running Exhibitor. This is the URI path for the REST call. The default is: /. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ZK_CONFIG_EXHIBITOR_PATH, value)
      }

      opt[String](ConfigNames.ZK_CONFIG_EXHIBITOR_PORT).optional().text("Used if the ZooKeeper shared config is also running Exhibitor. This is the port that Exhibitor is listening on. IMPORTANT: if this value is not set it implies that Exhibitor is not being used on the ZooKeeper shared config. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ZK_CONFIG_EXHIBITOR_PORT, value)
      }

      opt[String](ConfigNames.ZK_CONFIG_POLL_MS).optional().text("The period in ms to check for changes in the config ensemble. The default is: 10000. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ZK_CONFIG_POLL_MS, value)
      }

      opt[String](ConfigNames.ZK_CONFIG_RETRY).optional().text("The retry values to use in the form sleep-ms:retry-qty. The default is: 1000:3. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ZK_CONFIG_RETRY, value)
      }

      opt[String](ConfigNames.ZK_CONFIG_ZPATH).optional().text("The base ZPath that Exhibitor should use. E.g: /exhibitor/config. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ZK_CONFIG_ZPATH, value)
      }

      // Backup Options
      opt[String](ConfigNames.FILE_SYSTEM_BACKUP).optional().text("If true, enables file system backup of ZooKeeper log files. Optional.").action { (value, config) =>
        config.updated(ConfigNames.FILE_SYSTEM_BACKUP, value)
      }

      opt[String](ConfigNames.S3_BACKUP).optional().text("If true, enables AWS S3 backup of ZooKeeper log files (s3credentials may be provided as well). Optional.").action { (value, config) =>
        config.updated(ConfigNames.S3_BACKUP, value)
      }

      // ACL Options
      opt[String](ConfigNames.ACL_ID).optional().text("Enable ACL for Exhibitor's internal ZooKeeper connection. This sets the ACL's ID. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ACL_ID, value)
      }

      opt[String](ConfigNames.ACL_PERMISSIONS).optional().text("Enable ACL for Exhibitor's internal ZooKeeper connection. This sets the ACL's Permissions - a comma list of possible permissions. If this isn't specified the permission is set to ALL. Values: read, write, create, delete, admin. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ACL_PERMISSIONS, value)
      }

      opt[String](ConfigNames.ACL_SCHEME).optional().text("Enable ACL for Exhibitor's internal ZooKeeper connection. This sets the ACL's Scheme. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ACL_SCHEME, value)
      }

      // shared configs
      opt[String](ConfigNames.LOG_INDEX_DIRECTORY).optional().text("The directory where indexed Zookeeper logs should be kept. Optional.").action { (value, config) =>
        config.updated(ConfigNames.LOG_INDEX_DIRECTORY, value)
      }

      opt[String](ConfigNames.ZOOKEEPER_INSTALL_DIRECTORY).optional().text("The directory where the Zookeeper server is installed. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ZOOKEEPER_INSTALL_DIRECTORY, value)
      }

      opt[String](ConfigNames.ZOOKEEPER_DATA_DIRECTORY).optional().text("The directory where Zookeeper snapshot data is stored. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ZOOKEEPER_DATA_DIRECTORY, value)
      }

      opt[String](ConfigNames.ZOOKEEPER_LOG_DIRECTORY).optional().text("The directory where Zookeeper transaction log data is stored. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ZOOKEEPER_LOG_DIRECTORY, value)
      }

      opt[String](ConfigNames.BACKUP_EXTRA).optional().text("Backup extra shared config. Optional.").action { (value, config) =>
        config.updated(ConfigNames.BACKUP_EXTRA, value)
      }

      opt[String](ConfigNames.ZOO_CFG_EXTRA).optional().text("Any additional properties to be added to the zoo.cfg file in form: key1\\\\=value1&key2\\\\=value2. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ZOO_CFG_EXTRA, value)
      }

      opt[String](ConfigNames.JAVA_ENVIRONMENT).optional().text("Script to write as the 'java.env' file which gets executed as a part of Zookeeper start script. Optional.").action { (value, config) =>
        config.updated(ConfigNames.JAVA_ENVIRONMENT, value)
      }

      opt[String](ConfigNames.LOG4J_PROPERTIES).optional().text("Contents of the log4j.properties file. Optional.").action { (value, config) =>
        config.updated(ConfigNames.LOG4J_PROPERTIES, value)
      }

      opt[String](ConfigNames.CLIENT_PORT).optional().text("The port that clients use to connect to Zookeeper. Defaults to 2181. Optional.").action { (value, config) =>
        config.updated(ConfigNames.CLIENT_PORT, value)
      }

      opt[String](ConfigNames.CONNECT_PORT).optional().text("The port that other Zookeeper instances use to connect to Zookeeper. Defaults to 2888. Optional.").action { (value, config) =>
        config.updated(ConfigNames.CONNECT_PORT, value)
      }

      opt[String](ConfigNames.ELECTION_PORT).optional().text("The port that other Zookeeper instances use for election. Defaults to 3888. Optional.").action { (value, config) =>
        config.updated(ConfigNames.ELECTION_PORT, value)
      }

      opt[String](ConfigNames.CHECK_MS).optional().text("The number of milliseconds between live-ness checks on Zookeeper server. Defaults to 30000. Optional.").action { (value, config) =>
        config.updated(ConfigNames.CHECK_MS, value)
      }

      opt[String](ConfigNames.CLEANUP_PERIOD_MS).optional().text("The number of milliseconds between Zookeeper log file cleanups. Defaults to 43200000. Optional.").action { (value, config) =>
        config.updated(ConfigNames.CLEANUP_PERIOD_MS, value)
      }

      opt[String](ConfigNames.CLEANUP_MAX_FILES).optional().text("The max number of Zookeeper log files to keep when cleaning up. Defaults to 3. Optional.").action { (value, config) =>
        config.updated(ConfigNames.CLEANUP_MAX_FILES, value)
      }

      opt[String](ConfigNames.BACKUP_MAX_STORE_MS).optional().text("Backup max store ms shared config. Optional.").action { (value, config) =>
        config.updated(ConfigNames.BACKUP_MAX_STORE_MS, value)
      }

      opt[String](ConfigNames.BACKUP_PERIOD_MS).optional().text("Backup period ms shared config. Optional.").action { (value, config) =>
        config.updated(ConfigNames.BACKUP_PERIOD_MS, value)
      }

      opt[String](ConfigNames.PORT).optional().text("Port ranges to accept, when offer is issued. Optional").action { (value, config) =>
        config.updated(ConfigNames.PORT, value)
      }
    }

    private def defaultParser(descr: String): OptionParser[Map[String, String]] = new CliOptionParser(descr) {
      opt[String]('a', ConfigNames.API).optional().text(s"Binding host:port for http/artifact server. Optional if ${ConfigNames.API_ENV} env is set.").action { (value, config) =>
        config.updated(ConfigNames.API, value)
      }
    }
  }

  class CliOptionParser(descr: String) extends OptionParser[Map[String, String]](descr) {
    override def showUsage {
      Cli.out.println(usage)
    }
  }

  case class CliError(message: String) extends RuntimeException(message)

}
