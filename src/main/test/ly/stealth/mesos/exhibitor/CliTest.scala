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

import java.io.{ByteArrayOutputStream, IOException, PrintStream}

import ly.stealth.mesos.exhibitor.Cli.CliError
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
    Scheduler.cluster.servers += ExhibitorServer("0")
    Scheduler.cluster.servers += ExhibitorServer("1")
    Scheduler.cluster.servers += ExhibitorServer("2")

    exec("status")
    assertContains("id: 0")
    assertContains("id: 1")
    assertContains("id: 2")
  }

  @Test
  def add() {
    exec("add 0 --cpu 1.5 --mem 2048 --constraints hostname=like:slave.* --configchangebackoff 5000")
    assertContains("Server added")
    assertContains("id: 0")
    assertContains("constraints: hostname=like:slave.*")
    assertContains("cpu: 1.5")
    assertContains("sharedConfigChangeBackoff: 5000")

    assertEquals(1, Scheduler.cluster.servers.size)
    val serverOpt = Scheduler.cluster.getServer("0")
    assertNotEquals(None, serverOpt)

    val server = serverOpt.get
    assertEquals("0", server.id)
    assertEquals(1, server.constraints.size)
    val constraint = server.constraints.head
    assertEquals("hostname", constraint._1)
    assertEquals(Constraint("like:slave.*"), constraint._2)
    assertEquals(1.5, server.config.cpus, 0.001)
    assertEquals(2048, server.config.mem, 0.001)
    assertEquals(5000, server.config.sharedConfigChangeBackoff)
  }

  @Test
  def config() {
    Scheduler.cluster.servers += ExhibitorServer("0")

    exec("config 0 --zkconfigconnect 192.168.3.1:2181 --zookeeper-install-directory /tmp/zookeeper")
    assertContains("zkconfigconnect: 192.168.3.1:2181")
    assertContains("zookeeper-install-directory: /tmp/zookeeper")
  }

  @Test
  def startStop() {
    val server0 = ExhibitorServer("0")
    server0.task = ExhibitorServer.Task("exhibitor-0-slave0-31000", "", "", Map())
    Scheduler.cluster.servers += server0

    exec("start 0")
    assertContains("Started server")
    assertContains("id: 0")
    assertEquals(server0.state, ExhibitorServer.Stopped)

    exec("stop 0")
    assertContains("Stopped server 0")
    assertEquals(server0.state, ExhibitorServer.Added)
  }

  @Test
  def remove() {
    Scheduler.cluster.servers += ExhibitorServer("0")
    exec("remove 0")

    assertContains("Removed server 0")
    assertEquals(None, Scheduler.cluster.getServer("0"))
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
