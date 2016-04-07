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

import java.io.File
import java.net.URLClassLoader
import java.nio.file.{Files, Paths}

import net.elodina.mesos.exhibitor.exhibitorapi.{ExhibitorAPIClient, SharedConfig}
import org.apache.log4j.Logger

trait Server {
  def isStarted: Boolean

  def start(config: TaskConfig)

  def stop()

  def await()
}

class ExhibitorServer extends Server {
  private final val ZK_DATA_SANDBOX_DIR = new File("zkdata")
  private final val ZK_LOG_SANDBOX_DIR = new File("zklog")
  private final val ZK_LOG_INDEX_SANDBOX_DIR = new File("zklogindex")

  private val logger = Logger.getLogger(classOf[ExhibitorServer])
  @volatile var server: AnyRef = null

  private var config: TaskConfig = null
  private var sharedConfig: SharedConfig = null

  def url: String = s"http://${config.hostname}:${config.exhibitorConfig(ConfigNames.PORT)}"

  def isStarted: Boolean = server != null

  def start(config: TaskConfig) {
    if (isStarted) throw new IllegalStateException("Already started")

    this.config = config

    Thread.currentThread().setContextClassLoader(ExhibitorServer.loader)

    server = ExhibitorServer.newServer(config.exhibitorConfig.toMap)

    logger.info("Starting Exhibitor Server")
    server.getClass.getMethod("start").invoke(server)

    listenForConfigChanges()
  }

  def await() {
    if (server != null)
      server.getClass.getMethod("join").invoke(server)
  }

  def stop() {
    this.synchronized {
      if (server != null) {
        val shutdownSignaledField = server.getClass.getDeclaredField("shutdownSignaled")
        shutdownSignaledField.setAccessible(true)
        val shutdownSignaled = shutdownSignaledField.get(server)
        shutdownSignaled.getClass.getMethod("set", classOf[Boolean]).invoke(shutdownSignaled, true: java.lang.Boolean)
        server.getClass.getMethod("close").invoke(server)
      }

      server = null
    }
    //TODO
    //for ( Closeable closeable : creator.getCloseables() )
    //{
    //  CloseableUtils.closeQuietly(closeable);
    //}
  }

  private def listenForConfigChanges() {
    new Thread {
      override def run() {
        while (isStarted) {
          val newConfig = ExhibitorAPIClient.getSystemState(url)
          if (newConfig != sharedConfig) {
            logger.debug("Shared configuration changed, applying changes")
            sharedConfig = newConfig

            applyChanges()
          }

          Thread.sleep(config.sharedConfigChangeBackoff)
        }
      }
    }.start()
  }

  private def applyChanges() {
    createSymlinkIfNotEmpty(sharedConfig.zookeeperInstallDirectory, findZookeeperDist)

    if (sharedConfig.zookeeperDataDirectory == ExhibitorServer.ZK_DATA_SANDBOX_DIR)
      createSymlinkIfNotEmpty(sharedConfig.zookeeperDataDirectory, ZK_DATA_SANDBOX_DIR)
    else new File(sharedConfig.zookeeperDataDirectory).mkdirs()

    if (sharedConfig.zookeeperLogDirectory == ExhibitorServer.ZK_LOG_SANDBOX_DIR)
      createSymlinkIfNotEmpty(sharedConfig.zookeeperLogDirectory, ZK_LOG_SANDBOX_DIR)
    else new File(sharedConfig.zookeeperLogDirectory).mkdirs()

    if (sharedConfig.logIndexDirectory == ExhibitorServer.ZK_LOG_INDEX_SANDBOX_DIR)
      createSymlinkIfNotEmpty(sharedConfig.logIndexDirectory, ZK_LOG_INDEX_SANDBOX_DIR)
    else new File(sharedConfig.logIndexDirectory).mkdirs()
  }

  private def createSymlinkIfNotEmpty(link: String, target: File) {
    if (link != "") {
      logger.debug(s"Creating symbolic link $link to ${Paths.get(target.toURI)}")
      target.mkdirs() //create directories if they do not exist yet
      new File(link).delete() //remove symlink if already exists
      Files.createSymbolicLink(Paths.get(link), Paths.get(target.toURI)) //create a new symlink
    }
  }

  private def findZookeeperDist: File = {
    for (file <- new File(System.getProperty("user.dir")).listFiles()) {
      if (file.getName.matches(HttpServer.zookeeperMask) && file.isDirectory) return file
    }

    throw new IllegalStateException("Directory that matches " + HttpServer.zookeeperMask + " not found in in current dir")
  }
}

object ExhibitorServer {
  private val logger = Logger.getLogger(classOf[ExhibitorServer])
  private lazy val loader = initLoader

  final val ZK_DATA_SANDBOX_DIR = "/tmp/exhibitor-zkdata"
  final val ZK_LOG_SANDBOX_DIR = "/tmp/exhibitor-zklog"
  final val ZK_LOG_INDEX_SANDBOX_DIR = "/tmp/exhibitor-zklogindex"

  private def initLoader: ClassLoader = {
    new File(".").listFiles().find(file => file.getName.matches(HttpServer.exhibitorMask) && !file.getName.matches(HttpServer.jarMask)) match {
      case None => throw new IllegalStateException("Exhibitor standalone jar not found")
      case Some(exhibitorDist) => URLClassLoader.newInstance(Array(exhibitorDist.toURI.toURL), getClass.getClassLoader)
    }
  }

  def newServer(props: Map[String, String]): AnyRef = {
    val params = props.flatMap { case (key, value) =>
      Array(s"--$key", value)
    }.toArray
    logger.info(s"Exhibitor params: ${params.mkString(" ")}")

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
