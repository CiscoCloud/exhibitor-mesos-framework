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

import com.google.protobuf.ByteString
import org.apache.mesos.Protos
import org.apache.mesos.Protos._
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsValue, Json, Writes, _}

import scala.collection.JavaConversions._
import scala.collection.mutable

case class TaskConfig(exhibitorConfig: mutable.Map[String, String], sharedConfigOverride: mutable.Map[String, String], id: String, var hostname: String = "", var sharedConfigChangeBackoff: Long = 10000, var cpus: Double = 0.2, var mem: Double = 256)

object TaskConfig {
  implicit val reader = (
    (__ \ 'exhibitorConfig).read[Map[String, String]].map(m => mutable.Map(m.toSeq: _*)) and
      (__ \ 'sharedConfigOverride).read[Map[String, String]].map(m => mutable.Map(m.toSeq: _*)) and
      (__ \ 'id).read[String] and
      (__ \ 'hostname).read[String] and
      (__ \ 'sharedConfigChangeBackoff).read[Long] and
      (__ \ 'cpu).read[Double] and
      (__ \ 'mem).read[Double])(TaskConfig.apply _)

  implicit val writer = new Writes[TaskConfig] {
    def writes(tc: TaskConfig): JsValue = {
      Json.obj(
        "exhibitorConfig" -> tc.exhibitorConfig.toMap[String, String],
        "sharedConfigOverride" -> tc.sharedConfigOverride.toMap[String, String],
        "id" -> tc.id,
        "hostname" -> tc.hostname,
        "cpu" -> tc.cpus,
        "mem" -> tc.mem,
        "sharedConfigChangeBackoff" -> tc.sharedConfigChangeBackoff
      )
    }
  }
}

case class ExhibitorServer(id: String) {
  private[exhibitor] var task: ExhibitorServer.Task = null

  val config = TaskConfig(new mutable.HashMap[String, String](), new mutable.HashMap[String, String](), id)

  private[exhibitor] val constraints: mutable.Map[String, List[Constraint]] = new mutable.HashMap[String, List[Constraint]]
  private[exhibitor] var state: ExhibitorServer.State = ExhibitorServer.Added

  def createTask(offer: Offer): TaskInfo = {
    val port = getPort(offer).getOrElse(throw new IllegalStateException("No suitable port"))

    val id = s"exhibitor-${this.id}-${offer.getHostname}-$port"
    this.config.exhibitorConfig.put("port", port.toString)
    this.config.hostname = offer.getHostname
    val taskId = TaskID.newBuilder().setValue(id).build
    TaskInfo.newBuilder().setName(taskId.getValue).setTaskId(taskId).setSlaveId(offer.getSlaveId)
      .setExecutor(newExecutor(id))
      .setData(ByteString.copyFromUtf8(Json.stringify(Json.toJson(this.config))))
      .addResources(Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.config.cpus)))
      .addResources(Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.config.mem)))
      .addResources(Protos.Resource.newBuilder().setName("ports").setType(Protos.Value.Type.RANGES).setRanges(
      Protos.Value.Ranges.newBuilder().addRange(Protos.Value.Range.newBuilder().setBegin(port).setEnd(port))
    )).build
  }

  def matches(offer: Offer, otherAttributes: String => List[String] = _ => Nil): Option[String] = {
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

  private[exhibitor] def newExecutor(id: String): ExecutorInfo = {
    val cmd = s"java -cp ${HttpServer.jar.getName}${if (Config.debug) " -Ddebug" else ""} ly.stealth.mesos.exhibitor.Executor"

    val commandBuilder = CommandInfo.newBuilder()
    commandBuilder
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/exhibitor/" + HttpServer.exhibitorDist.getName))
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/zookeeper/" + HttpServer.zookeeperDist.getName).setExtract(true))
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jar/" + HttpServer.jar.getName))
      .setValue(cmd)

    this.config.exhibitorConfig.get("s3credentials").foreach { creds =>
      commandBuilder
        .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/s3credentials/" + creds))
    }

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue(id))
      .setCommand(commandBuilder)
      .setName(s"exhibitor-$id")
      .build
  }

  private def getPort(offer: Offer): Option[Long] = {
    val ports = Util.getRangeResources(offer, "ports")
    ports.headOption.map(_.getBegin)
  }

  def url: String = s"http://${config.hostname}:${config.exhibitorConfig("port")}"
}

object ExhibitorServer {
  def idFromTaskId(taskId: String): String = {
    taskId.split("-") match {
      case Array(_, id, _, _) => id
      case _ => throw new IllegalArgumentException(taskId)
    }
  }

  sealed trait State

  case object Unknown extends State

  case object Added extends State

  case object Stopped extends State

  case object Staging extends State

  case object Running extends State

  implicit val writer = new Writes[ExhibitorServer] {
    def writes(es: ExhibitorServer): JsValue = {
      Json.obj(
        "id" -> es.id,
        "state" -> es.state.toString,
        "constraints" -> Util.formatConstraints(es.constraints),
        "config" -> es.config
      )
    }
  }

  implicit val reader = (
    (__ \ 'id).read[String] and
      (__ \ 'state).read[String] and
      (__ \ 'constraints).read[String].map(Constraint.parse) and
      (__ \ 'config).read[TaskConfig])((id, state, constraints, config) => {
    val server = ExhibitorServer(id)
    state match {
      case "Unknown" => server.state = Unknown
      case "Added" => server.state = Added
      case "Stopped" => server.state = Stopped
      case "Staging" => server.state = Staging
      case "Running" => server.state = Running
    }
    constraints.foreach(server.constraints += _)
    config.exhibitorConfig.foreach(server.config.exhibitorConfig += _)
    config.sharedConfigOverride.foreach(server.config.sharedConfigOverride += _)
    server.config.cpus = config.cpus
    server.config.mem = config.mem
    server.config.sharedConfigChangeBackoff = config.sharedConfigChangeBackoff
    server.config.hostname = config.hostname
    server
  })

  case class Task(id: String, slaveId: String, executorId: String, attributes: Map[String, String])

}
