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

import java.util.{Date, UUID}

import com.google.protobuf.ByteString
import net.elodina.mesos.exhibitor.exhibitorapi.ExhibitorServerStatus
import org.apache.mesos.Protos
import org.apache.mesos.Protos.ContainerInfo.DockerInfo
import org.apache.mesos.Protos._
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsValue, Json, Writes, _}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration.Duration

case class Exhibitor(id: String) {
  private[exhibitor] var task: Exhibitor.Task = null

  val config = TaskConfig(new mutable.HashMap[String, String](), new mutable.HashMap[String, String](), id)

  private[exhibitor] val constraints: mutable.Map[String, List[Constraint]] = new mutable.HashMap[String, List[Constraint]]
  private[exhibitor] var state: Exhibitor.State = Exhibitor.Added

  private[exhibitor] var stickiness = Stickiness()
  private[exhibitor] var failover = new Failover()

  def createTask(offer: Offer): TaskInfo = {
    val port = getPort(offer).getOrElse(throw new IllegalStateException("No suitable port"))

    val name = s"${Config.frameworkName}-${this.id}"
    val id = Exhibitor.nextTaskId(this.id)
    this.config.exhibitorConfig.put(ConfigNames.PORT, port.toString)
    this.config.hostname = offer.getHostname
    val taskId = TaskID.newBuilder().setValue(id).build
    val executor = if (this.config.docker) newDockerExecutor(this.id) else newExecutor(this.id)

    TaskInfo.newBuilder().setName(name).setTaskId(taskId).setSlaveId(offer.getSlaveId)
      .setExecutor(executor)
      .setData(ByteString.copyFromUtf8(Json.stringify(Json.toJson(this.config))))
      .addResources(Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.config.cpus)))
      .addResources(Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.config.mem)))
      .addResources(Protos.Resource.newBuilder().setName("ports").setType(Protos.Value.Type.RANGES).setRanges(
        Protos.Value.Ranges.newBuilder().addRange(Protos.Value.Range.newBuilder().setBegin(port).setEnd(port))
      )).build
  }

  def matches(offer: Offer, now: Date = new Date(), otherAttributes: String => List[String] = _ => Nil): Option[String] = {
    val offerResources = offer.getResourcesList.toList.map(res => res.getName -> res).toMap

    if (getPort(offer).isEmpty) return Some("no suitable port")

    offerResources.get("cpus") match {
      case Some(cpusResource) => if (cpusResource.getScalar.getValue < config.cpus) return Some(s"cpus ${cpusResource.getScalar.getValue} < ${config.cpus}")
      case None => return Some("no cpus")
    }

    offerResources.get("mem") match {
      case Some(memResource) => if (memResource.getScalar.getValue < config.mem) return Some(s"mem ${memResource.getScalar.getValue} < ${config.mem}")
      case None => return Some("no mem")
    }

    if (!stickiness.allowsHostname(offer.getHostname, now))
      return Some("hostname != stickiness host")

    val offerAttributes = offer.getAttributesList.toList.foldLeft(Map("hostname" -> offer.getHostname)) { case (attributes, attribute) =>
      if (attribute.hasText) attributes.updated(attribute.getName, attribute.getText.getValue)
      else attributes
    }

    for ((name, constraints) <- constraints) {
      for (constraint <- constraints) {
        offerAttributes.get(name) match {
          case Some(attribute) => if (!constraint.matches(attribute, otherAttributes(name))) return Some(s"$name doesn't match $constraint")
          case None => return Some(s"no $name")
        }
      }
    }

    None
  }

  def waitFor(state: Exhibitor.State, timeout: Duration): Boolean = {
    var t = timeout.toMillis
    while (t > 0 && this.state != state) {
      val delay = Math.min(100, t)
      Thread.sleep(delay)
      t -= delay
    }

    this.state == state
  }

  def isReconciling: Boolean = this.state == Exhibitor.Reconciling

  private[exhibitor] def newExecutor(id: String): ExecutorInfo = {
    val java = "$(find jdk* -maxdepth 0 -type d)" // find non-recursively a directory starting with "jdk"
    val cmd = s"export PATH=$$MESOS_DIRECTORY/$java/bin:$$PATH && java -cp ${HttpServer.jar.getName}${if (Config.debug) " -Ddebug" else ""} net.elodina.mesos.exhibitor.Executor"

    val commandBuilder = CommandInfo.newBuilder()
    commandBuilder
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/exhibitor/" + HttpServer.exhibitorDist.getName))
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/zookeeper/" + HttpServer.zookeeperDist.getName).setExtract(true))
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jdk/" + HttpServer.jdkDist.getName).setExtract(true))
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jar/" + HttpServer.jar.getName))
      .setValue(cmd)

    this.config.exhibitorConfig.get(ConfigNames.S3_CREDENTIALS).foreach { creds =>
      commandBuilder
        .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/s3credentials/" + creds))
    }

    this.config.exhibitorConfig.get(ConfigNames.DEFAULT_SHARED_CONFIG).foreach { config =>
      commandBuilder
        .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/defaultconfig/" + config))
    }

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue(Exhibitor.nextExecutorId(id)))
      .setCommand(commandBuilder)
      .setName(s"exhibitor-$id")
      .build
  }

  private[exhibitor] def newDockerExecutor(id: String): ExecutorInfo = {
    val dockerBuilder = DockerInfo.newBuilder()
    dockerBuilder
      .setForcePullImage(false)
      .setNetwork(DockerInfo.Network.HOST)
      .setImage("elodina/exhibitor") //TODO versioning


    val container = ContainerInfo.newBuilder()
      .setType(ContainerInfo.Type.DOCKER)
      .setDocker(dockerBuilder.build())

    this.config.sharedConfigOverride.get(ConfigNames.ZOOKEEPER_DATA_DIRECTORY).map { dir =>
      if (dir != ExhibitorServer.ZK_DATA_SANDBOX_DIR)
        container.addVolumes(Volume.newBuilder().setHostPath(dir).setContainerPath(dir).setMode(Volume.Mode.RW))
    }

    this.config.sharedConfigOverride.get(ConfigNames.ZOOKEEPER_LOG_DIRECTORY).map { dir =>
      if (dir != ExhibitorServer.ZK_LOG_SANDBOX_DIR)
        container.addVolumes(Volume.newBuilder().setHostPath(dir).setContainerPath(dir).setMode(Volume.Mode.RW))
    }

    this.config.sharedConfigOverride.get(ConfigNames.LOG_INDEX_DIRECTORY).map { dir =>
      if (dir != ExhibitorServer.ZK_LOG_INDEX_SANDBOX_DIR)
        container.addVolumes(Volume.newBuilder().setHostPath(dir).setContainerPath(dir).setMode(Volume.Mode.RW))
    }

    val command = CommandInfo.newBuilder()
      .setShell(false)
      .setEnvironment(Environment.newBuilder().addVariables(Environment.Variable.newBuilder()
        .setName("MESOS_NATIVE_JAVA_LIBRARY")
        .setValue("/usr/local/lib/libmesos.so")))

    if (Config.debug) command.addArguments("-Ddebug")

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue(Exhibitor.nextExecutorId(id)))
      .setContainer(container)
      .setName(s"exhibitor-$id")
      .setCommand(command)
      .build()
  }

  private[exhibitor] def getPort(offer: Offer): Option[Long] = {
    val ports = Util.getRangeResources(offer, "ports").map(r => Util.Range(r.getBegin.toInt, r.getEnd.toInt))

    if (config.ports == Nil) ports.headOption.map(_.start)
    else ports.flatMap(range => config.ports.flatMap(range.overlap)).headOption.map(_.start)
  }

  def url: String = s"http://${config.hostname}:${config.exhibitorConfig(ConfigNames.PORT)}"

  def registerStart(hostname: String): Unit = {
    stickiness.registerStart(hostname)
    failover.resetFailures()
  }

  def registerStop(now: Date = new Date(), failed: Boolean = false): Unit = {
    if (!failed || failover.failures == 0) stickiness.registerStop(now)

    if (failed) failover.registerFailure(now)
    else failover.resetFailures()
  }
}

object Exhibitor {

  sealed trait State

  case object Added extends State

  case object Stopped extends State

  case object Staging extends State

  case object Running extends State

  case object Reconciling extends State

  case class Task(id: String, slaveId: String, executorId: String, attributes: Map[String, String], hostname: String)

  object Task {
    implicit val writer = Json.writes[Task]
    implicit val reader = Json.reads[Task]
  }

  def nextTaskId(serverId: String): String = s"${Config.frameworkName}-$serverId-${UUID.randomUUID()}"

  def nextExecutorId(serverId: String): String = s"${Config.frameworkName}-$serverId-${UUID.randomUUID()}"

  def idFromTaskId(taskId: String): String = taskId.dropRight(37).replace(Config.frameworkName + "-", "")

  implicit val writer = new Writes[Exhibitor] {
    def writes(es: Exhibitor): JsValue = {
      Json.obj(
        "id" -> es.id,
        "state" -> es.state.toString,
        "task" -> Option(es.task),
        "constraints" -> Util.formatConstraints(es.constraints),
        "config" -> es.config,
        "stickiness" -> es.stickiness,
        "failover" -> es.failover
      )
    }
  }

  implicit val reader = (
    (__ \ 'id).read[String] and
      (__ \ 'state).read[String] and
      (__ \ 'task).readNullable[Task] and
      (__ \ 'constraints).read[String].map(Constraint.parse) and
      (__ \ 'config).read[TaskConfig] and
      (__ \ 'stickiness).read[Stickiness] and
      (__ \ 'failover).read[Failover]) ((id, state, task, constraints, config, stickiness, failover) => {
    val server = Exhibitor(id)
    state match {
      case "Added" => server.state = Added
      case "Stopped" => server.state = Stopped
      case "Staging" => server.state = Staging
      case "Running" => server.state = Running
      case "Reconciling" => server.state = Reconciling
    }
    server.task = task.orNull
    constraints.foreach(server.constraints += _)
    config.exhibitorConfig.foreach(server.config.exhibitorConfig += _)
    config.sharedConfigOverride.foreach(server.config.sharedConfigOverride += _)
    server.config.cpus = config.cpus
    server.config.mem = config.mem
    server.config.sharedConfigChangeBackoff = config.sharedConfigChangeBackoff
    server.config.hostname = config.hostname
    server.config.ports = config.ports
    server.stickiness = stickiness
    server.failover = failover
    server
  })
}

/**
  * @param server               Exhibitor-on-mesos server instance
  * @param exhibitorClusterView a holder for Exhibitor's /status endpoint response - the view of the Exhibitor cluster
  *                             status from the particular node
  */
case class ExhibitorMesosStatus(server: Exhibitor, exhibitorClusterView: Option[Seq[ExhibitorServerStatus]])

object ExhibitorMesosStatus {
  implicit val format = Json.format[ExhibitorMesosStatus]
}

case class ClusterStatus(serverStatuses: Seq[ExhibitorMesosStatus]) {
  val servers = serverStatuses.map(_.server)
}

object ClusterStatus {
  implicit val format = Json.format[ClusterStatus]
}
