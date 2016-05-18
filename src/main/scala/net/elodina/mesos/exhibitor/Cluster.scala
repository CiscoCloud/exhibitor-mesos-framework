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

import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection.mutable

case class Cluster(initialEnsembles: List[Ensemble] = Nil) {
  private val storage = Cluster.newStorage(Config.storage)
  private[exhibitor] var frameworkId: Option[String] = None

  private[exhibitor] val exhibitorEnsembles: mutable.Map[String, Ensemble] = mutable.Map()
  //add anything that was passed to constructor
  initialEnsembles.foreach(ensemble => exhibitorEnsembles += ensemble.id -> ensemble)

  def ensembles(): List[Ensemble] = this.exhibitorEnsembles.values.toList

  def ensemble(id: String): Option[Ensemble] = this.exhibitorEnsembles.get(id)

  def defaultEnsemble(): Ensemble = {
    this.exhibitorEnsembles.get("default") match {
      case Some(ensemble) => ensemble
      case None =>
        this.exhibitorEnsembles += "default" -> Ensemble("default")
        this.exhibitorEnsembles("default")
    }
  }

  def save() = storage.save(this)(Cluster.writer)

  def load() {
    storage.load(Cluster.reader).foreach { cluster =>
      this.frameworkId = cluster.frameworkId
      cluster.ensembles().foreach(ensemble => this.exhibitorEnsembles += ensemble.id -> ensemble)
    }
  }

  override def toString: String = ensembles().toString()
}

object Cluster {
  private def newStorage(storage: String): Storage[Cluster] = {
    storage.split(":", 2) match {
      case Array("file", fileName) => FileStorage(fileName)
      case Array("zk", zk) => ZkStorage(zk)
      case _ => throw new IllegalArgumentException(s"Unsupported storage: $storage")
    }
  }

  implicit val writer = new Writes[Cluster] {
    override def writes(o: Cluster): JsValue = Json.obj("frameworkid" -> o.frameworkId, "cluster" -> o.defaultEnsemble().servers())
  }

  implicit val reader = ((__ \ 'frameworkid).readNullable[String] and
    (__ \ 'cluster).read[List[Exhibitor]]) ((frameworkId, exhibitors) => {
    val cluster = Cluster(List(restoreEnsemble(exhibitors)))
    cluster.frameworkId = frameworkId
    cluster
  })

  private def restoreEnsemble(servers: List[Exhibitor]): Ensemble = {
    val exhibitorOpt = servers.headOption
    val clientPort = exhibitorOpt.flatMap(_.config.sharedConfigOverride.get(ConfigNames.CLIENT_PORT).map(_.toInt)).getOrElse(-1)
    val connectPort = exhibitorOpt.flatMap(_.config.sharedConfigOverride.get(ConfigNames.CONNECT_PORT).map(_.toInt)).getOrElse(-1)
    val electionPort = exhibitorOpt.flatMap(_.config.sharedConfigOverride.get(ConfigNames.ELECTION_PORT).map(_.toInt)).getOrElse(-1)

    Ensemble("default", clientPort, connectPort, electionPort, servers)
  }
}
