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

import java.io.File
import java.net.URLClassLoader
import java.util

import org.apache.log4j.Logger

import scala.collection.JavaConversions._

class Exhibitor {
  private val logger = Logger.getLogger(classOf[Exhibitor])
  @volatile var server: AnyRef = null

  def isStarted: Boolean = server != null

  def start(props: util.Map[String, String]) {
    if (isStarted) throw new IllegalStateException("Already started")

    Thread.currentThread().setContextClassLoader(Exhibitor.loader)

    server = Exhibitor.newServer(props)

    logger.info("Starting Exhibitor Server")
    server.getClass.getMethod("start").invoke(server)
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
}

object Exhibitor {
  private lazy val loader = init

  private def init: ClassLoader = {
    new File(".").listFiles().find(file => file.getName.matches(HttpServer.exhibitorMask)) match {
      case None => throw new IllegalStateException("Exhibitor standalone jar not found")
      case Some(exhibitorDist) => URLClassLoader.newInstance(Array(exhibitorDist.toURI.toURL), getClass.getClassLoader)
    }
  }

  def newServer(props: util.Map[String, String]): AnyRef = {
    val params = props.toMap.flatMap { case (key, value) =>
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
