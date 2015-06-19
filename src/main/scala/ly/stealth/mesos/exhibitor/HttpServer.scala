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

import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool

class HttpServer(config: SchedulerConfig) {
  val threadPool = new QueuedThreadPool(16)
  threadPool.setName("Jetty")

  val server = new Server(threadPool)
  val connector = new ServerConnector(server)
  connector.setPort(config.httpServerPort)

  val handler = new ServletContextHandler
  handler.addServlet(new ServletHolder(new Servlet()), "/")

  server.setHandler(handler)
  server.addConnector(connector)
  server.start()

  def stop() {
    if (server == null) throw new IllegalStateException("!started")

    server.stop()
    server.join()
  }

  class Servlet extends HttpServlet {
    override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val uri = request.getRequestURI
      if (uri.startsWith("/resource/")) downloadFile(uri.split("/").last, response)
      else response.sendError(404)
    }

    def downloadFile(file: String, response: HttpServletResponse) {
      response.setContentType("application/zip")
      response.setHeader("Content-Disposition", "attachment; filename=\"" + new File(file).getName + "\"")
      Util.copyAndClose(new FileInputStream(file), response.getOutputStream)
    }
  }

}