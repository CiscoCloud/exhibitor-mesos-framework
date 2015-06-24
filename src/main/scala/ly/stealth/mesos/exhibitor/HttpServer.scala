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

import java.io._
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.apache.log4j.Logger
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool

import scala.util.{Failure, Success, Try}

object HttpServer {
  private val logger = Logger.getLogger(HttpServer.getClass)
  private var server: Server = null

  val jarMask = "mesos-exhibitor.*\\.jar"
  val exhibitorMask = "exhibitor.*\\.jar"

  private[exhibitor] var jar: File = null
  private[exhibitor] var exhibitorDist: File = null

  def start(resolveDeps: Boolean = true) {
    if (server != null) throw new IllegalStateException("HttpServer already started")
    if (resolveDeps) this.resolveDeps()

    val threadPool = new QueuedThreadPool(16)
    threadPool.setName("Jetty")

    server = new Server(threadPool)
    val connector = new ServerConnector(server)
    connector.setPort(Config.httpServerPort)
    connector.setIdleTimeout(60 * 1000)

    val handler = new ServletContextHandler
    handler.addServlet(new ServletHolder(new Servlet()), "/")

    server.setHandler(handler)
    server.addConnector(connector)
    server.start()

    logger.info("started on port " + connector.getPort)
  }

  def stop() {
    if (server == null) throw new IllegalStateException("HttpServer not started")

    server.stop()
    server.join()
    server = null

    logger.info("HttpServer stopped")
  }

  private def resolveDeps() {
    for (file <- new File(".").listFiles()) {
      if (file.getName.matches(jarMask)) jar = file
      if (file.getName.matches(exhibitorMask)) exhibitorDist = file
    }

    if (jar == null) throw new IllegalStateException(jarMask + " not found in current dir")
    if (exhibitorDist == null) throw new IllegalStateException(exhibitorMask + " not found in in current dir")
  }

  class Servlet extends HttpServlet {
    override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
      Try(handle(request, response)) match {
        case Success(_) =>
        case Failure(e) =>
          logger.warn("", e)
          response.sendError(500, "" + e)
          throw e
      }
    }

    def handle(request: HttpServletRequest, response: HttpServletResponse) {
      val uri = request.getRequestURI
      if (uri.startsWith("/jar/")) downloadFile(HttpServer.jar, response)
      else if (uri.startsWith("/exhibitor/")) downloadFile(HttpServer.exhibitorDist, response)
      else if (uri.startsWith("/api")) handleApi(request, response)
      else response.sendError(404)
    }

    def downloadFile(file: File, response: HttpServletResponse) {
      response.setContentType("application/zip")
      response.setHeader("Content-Length", "" + file.length())
      response.setHeader("Content-Disposition", "attachment; filename=\"" + file.getName + "\"")
      Util.copyAndClose(new FileInputStream(file), response.getOutputStream)
    }

    def handleApi(request: HttpServletRequest, response: HttpServletResponse) {
      response.setContentType("application/json; charset=utf-8")
      var uri: String = request.getRequestURI.substring("/api".length)
      if (uri.startsWith("/")) uri = uri.substring(1)

      if (uri == "add") handleAddServer(request, response)
      else response.sendError(404)
    }

    private def handleAddServer(request: HttpServletRequest, response: HttpServletResponse) {
      val id = request.getParameter("id")
      val cpus = Option(request.getParameter("cpu"))
      val mem = Option(request.getParameter("mem"))

      val server = ExhibitorServer(id)
      cpus.foreach(cpus => server.cpus = cpus.toDouble)
      mem.foreach(mem => server.mem = mem.toDouble)

      Scheduler.cluster.servers += server
    }
  }

}