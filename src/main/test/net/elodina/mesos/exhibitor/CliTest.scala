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

import java.io.{ByteArrayOutputStream, IOException, PrintStream}

import net.elodina.mesos.exhibitor.Cli.CliError
import net.elodina.mesos.util.Period
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.util.{Failure, Try}

class CliTest extends MesosTestCase {
  val out = new ByteArrayOutputStream()

  @Before
  override def before() {
    super.before()
    Config.api = "http://localhost:8000"
    HttpServer.start(resolveDeps = false)
    Cli.out = new PrintStream(out, true)
  }

  @After
  override def after() {
    Cli.out = System.out
    HttpServer.stop()
    super.after()
  }

  @Test
  def help() {
    exec("help")
    assertContains("Usage:")
    assertContains("scheduler")
    assertContains("start")
    assertContains("stop")

    "scheduler status add config remove start stop".split(" ").foreach { command =>
      exec(s"help $command")
      assertContains("Usage:")
    }

    exec("help ugh")
    assertContains("Unknown command:")
  }

  @Test
  def status() {
    Scheduler.cluster.defaultEnsemble().addServer(Exhibitor("0"))
    Scheduler.cluster.defaultEnsemble().addServer(Exhibitor("1"))
    Scheduler.cluster.defaultEnsemble().addServer(Exhibitor("2"))

    exec("status")
    assertContains("id: 0")
    assertContains("id: 1")
    assertContains("id: 2")
  }

  @Test
  def add() {
    exec("add 0 --cpu 1.5 --mem 2048 --constraints hostname=like:slave.* --configchangebackoff 5000 --port 3000..8000")
    assertContains("Added servers")
    assertContains("id: 0")
    assertContains("constraints: hostname=like:slave.*")
    assertContains("cpu: 1.5")
    assertContains("sharedConfigChangeBackoff: 5000")

    assertEquals(1, Scheduler.cluster.defaultEnsemble().length())
    val serverOpt = Scheduler.cluster.defaultEnsemble().getServer("0")
    assertNotEquals(None, serverOpt)

    val server = serverOpt.get
    assertEquals("0", server.id)
    assertEquals(1, server.constraints.size)
    val constraint = server.constraints.head
    assertEquals("hostname", constraint._1)
    assertEquals(1, constraint._2.size)
    assertEquals(Constraint("like:slave.*"), constraint._2.head)
    assertEquals(1.5, server.config.cpus, 0.001)
    assertEquals(2048, server.config.mem, 0.001)
    assertEquals(5000, server.config.sharedConfigChangeBackoff)
    assertEquals(1, server.config.ports.size)
    assertEquals(3000, server.config.ports.head.start)
    assertEquals(8000, server.config.ports.head.end)
  }

  @Test
  def config() {
    Scheduler.cluster.defaultEnsemble().addServer(Exhibitor("0"))

    exec("config 0 --zkconfigconnect 192.168.3.1:2181 --zookeeper-install-directory /tmp/zookeeper --port 33..55 --stickiness-period 5m " +
      "--failover-delay 30s --failover-max-delay 1h --failover-max-tries 5")
    val serverOpt = Scheduler.cluster.defaultEnsemble().getServer("0")
    assertNotEquals(None, serverOpt)
    assertEquals(1, serverOpt.get.config.ports.size)
    assertEquals(serverOpt.get.config.ports.head.start, 33)
    assertEquals(serverOpt.get.config.ports.head.end, 55)
    assertContains("zkconfigconnect: 192.168.3.1:2181")
    assertContains("zookeeper-install-directory: /tmp/zookeeper")
    assertEquals(serverOpt.get.stickiness.period.ms, new Period("5m").ms)
    assertEquals(serverOpt.get.failover.delay.ms, new Period("30s").ms)
    assertEquals(serverOpt.get.failover.maxDelay.ms, new Period("1h").ms)
    assertEquals(serverOpt.get.failover.maxTries, Some(5))
  }

  @Test
  def startStop() {
    val server0 = Exhibitor("0")
    server0.task = Exhibitor.Task("exhibitor-0-slave0-31000", "", "", Map(), "master")
    Scheduler.cluster.defaultEnsemble().addServer(server0)

    exec("start 0 --timeout 0ms")
    assertContains("scheduled")
    assertContains("0")
    assertEquals(server0.state, Exhibitor.Stopped)

    exec("stop 0")
    assertContains("Stopped servers 0")
    assertEquals(server0.state, Exhibitor.Added)
  }

  @Test
  def startStopTimeout() {
    val server0 = Exhibitor("0")
    server0.task = Exhibitor.Task("exhibitor-0-slave0-31000", "", "", Map(), "master")
    Scheduler.cluster.defaultEnsemble().addServer(server0)

    Try(exec("start 0 --timeout 1ms")) match {
      case Failure(e) if e.isInstanceOf[CliError] => assertTrue(e.getMessage, e.getMessage.contains("timed out"))
      case other => fail(other.toString)
    }

    assertEquals(server0.state, Exhibitor.Stopped)

    exec("stop 0")
    assertContains("Stopped servers 0")
    assertEquals(server0.state, Exhibitor.Added)
  }

  @Test
  def remove() {
    Scheduler.cluster.defaultEnsemble().addServer(Exhibitor("0"))
    exec("remove 0")

    assertContains("Removed servers 0")
    assertEquals(None, Scheduler.cluster.defaultEnsemble().getServer("0"))
  }

  @Test
  def usageErrors() {
    // no command
    Try(exec("")) match {
      case Failure(e) if e.isInstanceOf[CliError] => assertTrue(e.getMessage, e.getMessage.contains("No command supplied"))
      case other => fail(other.toString)
    }

    // no id
    Try(exec("add")) match {
      case Failure(e) if e.isInstanceOf[CliError] => assertTrue(e.getMessage, e.getMessage.contains("Argument required"))
      case other => fail(other.toString)
    }

    // invalid flag
    Try(exec("add 0 --unknown flag")) match {
      case Failure(e) if e.isInstanceOf[CliError] => assertTrue(e.getMessage, e.getMessage.contains("Invalid arguments"))
      case other => fail(other.toString)
    }

    // invalid flag
    Try(exec("unsupported 0")) match {
      case Failure(e) if e.isInstanceOf[CliError] => assertTrue(e.getMessage, e.getMessage.contains("Unknown command"))
      case other => fail(other.toString)
    }

    Config.api = null
    Try(exec("add 0")) match {
      case Failure(e) if e.isInstanceOf[CliError] => assertTrue(e.getMessage, e.getMessage.contains("Undefined API"))
      case other => fail(other.toString)
    }
    Config.api = "http://localhost:8000"
  }

  @Test
  def connectionRefused() {
    HttpServer.stop()
    try {
      Try(exec("add 0")) match {
        case Failure(e) if e.isInstanceOf[IOException] => assertTrue(e.getMessage, e.getMessage.contains("Connection refused"))
        case other => fail(other.toString)
      }
    } finally {
      HttpServer.start(resolveDeps = false)
    }
  }

  private def assertContains(s: String) = assertTrue("" + out, out.toString.contains(s))

  private def exec(cmd: String): Unit = {
    out.reset()
    Cli.exec(cmd.split(" ").filter(!_.isEmpty))
  }
}
