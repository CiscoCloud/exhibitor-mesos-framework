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

import java.io.{DataOutputStream, File, IOException}
import java.net.{HttpURLConnection, URL, URLClassLoader}
import java.nio.file.{Files, Paths}

import org.apache.log4j.Logger
import play.api.libs.json._

import scala.io.Source
import scala.util.{Failure, Success, Try}

class Exhibitor {
  private val logger = Logger.getLogger(classOf[Exhibitor])
  @volatile var server: AnyRef = null

  private var config: TaskConfig = null

  def url: String = s"http://${config.hostname}:${config.exhibitorConfig("port")}"

  def isStarted: Boolean = server != null

  def start(config: TaskConfig) {
    if (isStarted) throw new IllegalStateException("Already started")

    this.config = config

    Thread.currentThread().setContextClassLoader(Exhibitor.loader)

    server = Exhibitor.newServer(config.exhibitorConfig.toMap)

    logger.info("Starting Exhibitor Server")
    server.getClass.getMethod("start").invoke(server)

    addToEnsemble()
  }

  def addToEnsemble() {
    val sharedConfig = getSharedConfig(60)

    val updatedSharedConfig = config.sharedConfigOverride.foldLeft(sharedConfig) { case (conf, (key, value)) =>
      key match {
        case "zookeeper-install-directory" => conf.copy(zookeeperInstallDirectory = value)
        case "zookeeper-data-directory" => conf.copy(zookeeperDataDirectory = value)
        case invalid => throw new IllegalArgumentException(s"Unacceptable shared configuration parameter: $invalid")
      }
    }

    //TODO what if other exhibitor changes this property? ugh
    if (updatedSharedConfig.zookeeperInstallDirectory != "") {
      new File(updatedSharedConfig.zookeeperInstallDirectory).delete() //remove symlink if already exists
      Files.createSymbolicLink(Paths.get(updatedSharedConfig.zookeeperInstallDirectory), Paths.get(findZookeeperDist.toURI)) //create a new symlink
    }

    val updatedServersSpec = (s"S:${config.id}:${config.hostname}" :: updatedSharedConfig.serversSpec.split(",").foldLeft(List[String]()) { (servers, server) =>
      server.split(":") match {
        case Array(_, _, serverHost) if serverHost == config.hostname => servers
        case Array(_, _, serverHost) => server :: servers
        case _ => servers
      }
    }).mkString(",")

    ExhibitorAPI.setConfig(updatedSharedConfig.copy(serversSpec = updatedServersSpec), this.url)
  }

  private def getSharedConfig(retries: Int): SharedConfig = {
    def tryGetConfig(retriesLeft: Int, backoffMs: Long): SharedConfig = {
      Try(ExhibitorAPI.getSystemState(this.url)) match {
        case Success(cfg) =>
          if (cfg.zookeeperInstallDirectory == "") {
            if (retriesLeft > 0) {
              Thread.sleep(backoffMs)
              tryGetConfig(retriesLeft - 1, backoffMs)
            } else {
              logger.info(s"Failed to get non-default Exhibitor Shared Configuration within $retries retries. Using default.")
              cfg
            }
          } else cfg
        case Failure(e) =>
          logger.info("Exhibitor API not available.")
          if (retriesLeft > 0) {
            Thread.sleep(backoffMs)
            tryGetConfig(retriesLeft - 1, backoffMs)
          } else throw new IllegalStateException(s"Failed to get Exhibitor Shared Configuration within $retries retries")
      }
    }

    tryGetConfig(retries, 1000)
  }

  def await() {
    if (server != null)
      server.getClass.getMethod("join").invoke(server)
  }

  def stop() {
    if (server != null)
      server.getClass.getMethod("close").invoke(server)
    //TODO
    //for ( Closeable closeable : creator.getCloseables() )
    //{
    //  CloseableUtils.closeQuietly(closeable);
    //}
  }

  private def findZookeeperDist: File = {
    for (file <- new File(System.getProperty("user.dir")).listFiles()) {
      if (file.getName.matches(HttpServer.zookeeperMask) && file.isDirectory) return file
    }

    throw new IllegalStateException("Directory that matches " + HttpServer.zookeeperMask + " not found in in current dir")
  }
}

object Exhibitor {
  private lazy val loader = init

  private def init: ClassLoader = {
    new File(".").listFiles().find(file => file.getName.matches(HttpServer.exhibitorMask)) match {
      case None => throw new IllegalStateException("Exhibitor standalone jar not found")
      case Some(exhibitorDist) => URLClassLoader.newInstance(Array(exhibitorDist.toURI.toURL), getClass.getClassLoader)
    }
  }

  def newServer(props: Map[String, String]): AnyRef = {
    val params = props.flatMap { case (key, value) =>
      Array(s"--$key", value)
    }.toArray

    val exhibitorCreatorClass = loader.loadClass("com.netflix.exhibitor.standalone.ExhibitorCreator")
    val securityArgumentsClass = loader.loadClass("com.netflix.exhibitor.standalone.SecurityArguments")
    val exhibitorMainClass = loader.loadClass("com.netflix.exhibitor.application.ExhibitorMain")
    val backupProviderClass = loader.loadClass("com.netflix.exhibitor.core.backup.BackupProvider")
    val configProviderClass = loader.loadClass("com.netflix.exhibitor.core.config.ConfigProvider")
    val builderClass = loader.loadClass("com.netflix.exhibitor.core.ExhibitorArguments$Builder")
    val securityHandlerClass = loader.loadClass("org.mortbay.jetty.security.SecurityHandler")

    val exhibitorCreator = exhibitorCreatorClass.getConstructor(classOf[Array[String]]).newInstance(params).asInstanceOf[AnyRef]

    val securityFile = exhibitorCreatorClass.getMethod("getSecurityFile").invoke(exhibitorCreator)
    val realmSpec = exhibitorCreatorClass.getMethod("getRealmSpec").invoke(exhibitorCreator)
    val remoteAuthSpec = exhibitorCreatorClass.getMethod("getRemoteAuthSpec").invoke(exhibitorCreator)
    val securityArguments = securityArgumentsClass.getConstructor(classOf[String], classOf[String], classOf[String]).newInstance(securityFile, realmSpec, remoteAuthSpec).asInstanceOf[AnyRef]

    val backupProvider = exhibitorCreatorClass.getMethod("getBackupProvider").invoke(exhibitorCreator)
    val configProvider = exhibitorCreatorClass.getMethod("getConfigProvider").invoke(exhibitorCreator)
    val builder = exhibitorCreatorClass.getMethod("getBuilder").invoke(exhibitorCreator)
    val httpPort = exhibitorCreatorClass.getMethod("getHttpPort").invoke(exhibitorCreator)
    val securityHandler = exhibitorCreatorClass.getMethod("getSecurityHandler").invoke(exhibitorCreator)
    val exhibitorMain = exhibitorMainClass.getConstructor(backupProviderClass, configProviderClass, builderClass, Integer.TYPE, securityHandlerClass, securityArgumentsClass)
      .newInstance(backupProvider, configProvider, builder, httpPort, securityHandler, securityArguments).asInstanceOf[AnyRef]

    exhibitorMain
  }
}

case class Result(succeeded: Boolean, message: String)

object Result {
  implicit val reader = Json.reads[Result]
}

case class SharedConfig(logIndexDirectory: String, zookeeperInstallDirectory: String, zookeeperDataDirectory: String,
                        zookeeperLogDirectory: String, serversSpec: String, backupExtra: String, zooCfgExtra: Map[String, String],
                        javaEnvironment: String, log4jProperties: String, clientPort: Int, connectPort: Int,
                        electionPort: Int, checkMs: Long, cleanupPeriodMs: Long, cleanupMaxFiles: Int,
                        backupMaxStoreMs: Long, backupPeriodMs: Long, autoManageInstances: Int, autoManageInstancesSettlingPeriodMs: Long,
                        observerThreshold: Int, autoManageInstancesFixedEnsembleSize: Int, autoManageInstancesApplyAllAtOnce: Int)

object SharedConfig {
  implicit val reader = Json.reads[SharedConfig]

  // Exhibitor for some reason requires the values passed back to be strings, so have to define custom writer for it.
  implicit val writer = new Writes[SharedConfig] {
    def writes(sc: SharedConfig): JsValue = {
      Json.obj(
        "logIndexDirectory" -> sc.logIndexDirectory,
        "zookeeperInstallDirectory" -> sc.zookeeperInstallDirectory,
        "zookeeperDataDirectory" -> sc.zookeeperDataDirectory,
        "zookeeperLogDirectory" -> sc.zookeeperLogDirectory,
        "serversSpec" -> sc.serversSpec,
        "backupExtra" -> sc.backupExtra,
        "zooCfgExtra" -> sc.zooCfgExtra,
        "javaEnvironment" -> sc.javaEnvironment,
        "log4jProperties" -> sc.log4jProperties,
        "clientPort" -> sc.clientPort.toString,
        "connectPort" -> sc.connectPort.toString,
        "electionPort" -> sc.electionPort.toString,
        "checkMs" -> sc.checkMs.toString,
        "cleanupPeriodMs" -> sc.cleanupPeriodMs.toString,
        "cleanupMaxFiles" -> sc.cleanupMaxFiles.toString,
        "backupMaxStoreMs" -> sc.backupMaxStoreMs.toString,
        "backupPeriodMs" -> sc.backupPeriodMs.toString,
        "autoManageInstances" -> sc.autoManageInstances.toString,
        "autoManageInstancesSettlingPeriodMs" -> sc.autoManageInstancesSettlingPeriodMs.toString,
        "observerThreshold" -> sc.observerThreshold.toString,
        "autoManageInstancesFixedEnsembleSize" -> sc.autoManageInstancesFixedEnsembleSize.toString,
        "autoManageInstancesApplyAllAtOnce" -> sc.autoManageInstancesApplyAllAtOnce.toString
      )
    }
  }
}

object ExhibitorAPI {
  private val getSystemStateURL = "exhibitor/v1/config/get-state"
  private val setConfigURL = "exhibitor/v1/config/set"

  def getSystemState(baseUrl: String): SharedConfig = {
    val url = s"$baseUrl/$getSystemStateURL"
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      readResponse(connection, response => {
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
    val url = s"$baseUrl/$setConfigURL"
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Content-Type", "application/json")

      connection.setUseCaches(false)
      connection.setDoInput(true)
      connection.setDoOutput(true)

      val out = new DataOutputStream(connection.getOutputStream)
      out.writeBytes(Json.prettyPrint(Json.toJson(config)))
      out.flush()
      out.close()

      readResponse(connection, response => {
        Json.parse(response).validate[Result] match {
          case JsSuccess(result, _) => if (!result.succeeded) throw new IllegalStateException(result.message)
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
