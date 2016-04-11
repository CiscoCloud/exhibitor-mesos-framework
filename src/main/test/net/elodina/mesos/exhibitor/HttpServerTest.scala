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

import net.elodina.mesos.exhibitor.Cli.sendRequest
import net.elodina.mesos.exhibitor.Util.parseMap
import net.elodina.mesos.util.Period
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.mutable

class HttpServerTest extends MesosTestCase {
  @Before
  override def before() {
    super.before()
    Config.api = "http://localhost:8000"
    HttpServer.start(resolveDeps = false)
  }

  @After
  override def after() {
    HttpServer.stop()
    super.after()
  }

  @Test
  def addServer() {
    val response = sendRequest("/add", parseMap("id=0,cpu=0.6,mem=128,port=3000..8000")).as[ApiResponse]
    assertEquals(1, Scheduler.cluster.length())
    val server = Scheduler.cluster.servers().head

    assertEquals("0", server.id)
    assertEquals(0.6, server.config.cpus, 0.001)
    assertEquals(128, server.config.mem, 0.001)
    assertEquals(1, server.config.ports.size)
    assertEquals(3000, server.config.ports.head.start)
    assertEquals(8000, server.config.ports.head.end)
    assertTrue(response.message.contains("Added servers"))
    assert(response.success)
    assertNotEquals(None, response.value)

    ExhibitorTest.assertServerEquals(server, response.value.get.servers().head)
  }

  @Test
  def configServer() {
    sendRequest("/add", parseMap("id=0"))
    val response = sendRequest("/config", parseMap("id=0,zkconfigconnect=192.168.3.1:2181,zookeeper-install-directory=/tmp/zookeeper,stickiness-period=5m")).as[ApiResponse]

    val serverOpt = Scheduler.cluster.getServer("0")
    assertNotEquals(None, serverOpt)

    val server = serverOpt.get
    assertEquals("0", server.id)
    assertEquals(mutable.Map("zkconfigconnect" -> "192.168.3.1:2181"), server.config.exhibitorConfig)
    assertEquals(server.config.sharedConfigOverride(ConfigNames.ZOOKEEPER_INSTALL_DIRECTORY), "/tmp/zookeeper")
    assertEquals(new Period("5m").ms, server.stickiness.period.ms)

    assertTrue(response.success)
    assertTrue(response.message.contains("Updated configuration"))
    assertNotEquals(None, response.value)
    ExhibitorTest.assertServerEquals(server, response.value.get.servers().head)
  }

  @Test
  def clusterStatus() {
    sendRequest("/add", parseMap("id=0"))
    sendRequest("/add", parseMap("id=1"))
    sendRequest("/add", parseMap("id=2"))

    val response = sendRequest("/status", Map()).as[ClusterStatusResponse]
    assertTrue(response.success)
    assertNotEquals(None, response.value)
    assertEquals(3, response.value.get.servers.size)
    assertEquals(3, response.value.get.servers.map(_.id).distinct.size)
  }

  @Test
  def removeServer() {
    sendRequest("/add", parseMap("id=0"))
    sendRequest("/add", parseMap("id=1"))
    sendRequest("/add", parseMap("id=2"))

    sendRequest("/remove", parseMap("id=1"))
    assertEquals(2, Scheduler.cluster.length())
  }

  @Test
  def startStopServer() {
    sendRequest("/add", parseMap("id=0"))

    val startResponse = sendRequest("/start", parseMap("id=0,timeout=0ms")).as[ApiResponse]
    assertTrue(startResponse.success)
    assertTrue(startResponse.message.contains("scheduled"))
    assertNotEquals(None, startResponse.value)
    assertEquals(Exhibitor.Stopped, startResponse.value.get.servers().head.state)

    val stopResponse = sendRequest("/stop", parseMap("id=0")).as[ApiResponse]
    assertTrue(stopResponse.success)
    assertTrue(stopResponse.message.contains("Stopped servers"))
    assertNotEquals(None, stopResponse.value)
    assertEquals(Exhibitor.Added, stopResponse.value.get.servers().head.state)
  }
}
