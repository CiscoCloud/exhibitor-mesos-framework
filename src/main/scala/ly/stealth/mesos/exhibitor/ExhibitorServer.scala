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
import org.apache.mesos.Protos.{Offer, TaskID, TaskInfo}
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsValue, Json, Writes, _}

import scala.collection.mutable

case class TaskConfig(exhibitorConfig: mutable.Map[String, String], sharedConfigOverride: mutable.Map[String, String], id: String, var cpus: Double = 0.2, var mem: Double = 256)

object TaskConfig {
  def apply(exhibitorConfig: mutable.Map[String, String], sharedConfigOverride: mutable.Map[String, String], id: String): TaskConfig = new TaskConfig(exhibitorConfig: mutable.Map[String, String], sharedConfigOverride: mutable.Map[String, String], id: String)

  implicit val reader = (
    (__ \ 'exhibitorConfig).read[Map[String, String]].map(m => mutable.Map(m.toSeq: _*)) and
      (__ \ 'sharedConfigOverride).read[Map[String, String]].map(m => mutable.Map(m.toSeq: _*)) and
      (__ \ 'id).read[String])(TaskConfig.apply _)

  implicit val writer = new Writes[TaskConfig] {
    def writes(tc: TaskConfig): JsValue = {
      Json.obj(
        "exhibitorConfig" -> tc.exhibitorConfig.toMap[String, String],
        "sharedConfigOverride" -> tc.sharedConfigOverride.toMap[String, String],
        "id" -> tc.id
      )
    }
  }
}

case class ExhibitorServer(id: String) {
  val config = TaskConfig(new mutable.HashMap[String, String](), new mutable.HashMap[String, String](), id)

  private[exhibitor] var state: ExhibitorServer.State = ExhibitorServer.Added

  def createTask(offer: Offer): Option[TaskInfo] = {
    val cpus = Util.getScalarResources(offer, "cpus")
    val mem = Util.getScalarResources(offer, "mem")
    val portOpt = getPort(offer)

    if (cpus > this.config.cpus && mem > this.config.mem && portOpt.nonEmpty) {
      val id = s"exhibitor-${this.id}-${offer.getHostname}-${portOpt.get}"
      this.config.exhibitorConfig.put("port", portOpt.get.toString)
      val taskId = TaskID.newBuilder().setValue(id).build
      val taskInfo = TaskInfo.newBuilder().setName(taskId.getValue).setTaskId(taskId).setSlaveId(offer.getSlaveId)
        .setExecutor(Scheduler.newExecutor(id))
        .setData(ByteString.copyFromUtf8(Json.stringify(Json.toJson(this.config))))
        .addResources(Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.config.cpus)))
        .addResources(Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.config.mem)))
        .addResources(Protos.Resource.newBuilder().setName("ports").setType(Protos.Value.Type.RANGES).setRanges(
        Protos.Value.Ranges.newBuilder().addRange(Protos.Value.Range.newBuilder().setBegin(portOpt.get).setEnd(portOpt.get))
      )).build

      Some(taskInfo)
    } else None
  }

  private def getPort(offer: Offer): Option[Long] = {
    val ports = Util.getRangeResources(offer, "ports")
    ports.headOption.map(_.getBegin)
  }
}

object ExhibitorServer {
  def idFromTaskId(taskId: String): String = {
    taskId.split("-") match {
      case Array(_, id, _, _) => id
      case _ => throw new IllegalArgumentException(taskId)
    }
  }

  sealed trait State

  case object Added extends State

  case object Stopped extends State

  case object Staging extends State

  case object Running extends State

}
