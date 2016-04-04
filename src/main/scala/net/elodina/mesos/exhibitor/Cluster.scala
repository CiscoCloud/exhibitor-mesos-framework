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

import scala.collection.mutable.ListBuffer

case class Cluster(initialServers: List[ExhibitorServer] = Nil) {
  private val storage = Cluster.newStorage(Config.storage)
  private[exhibitor] var frameworkId: Option[String] = None

  private[exhibitor] val exhibitorServers = new ListBuffer[ExhibitorServer]
  //add anything that was passed to constructor
  initialServers.foreach(exhibitorServers += _)

  def servers(): List[ExhibitorServer] = this.exhibitorServers.toList

  def servers(state: ExhibitorServer.State): List[ExhibitorServer] = servers().filter(_.state == state)

  def runningServers(): List[ExhibitorServer] = servers().filter(_.task != null)

  def getServer(id: String): Option[ExhibitorServer] = servers().find(_.id == id)

  def findWithState(state: ExhibitorServer.State): Option[ExhibitorServer] = servers().find(_.state == state)

  def addServer(server: ExhibitorServer): Boolean = {
    servers().find(_.id == server.id) match {
      case Some(_) => false
      case None =>
        exhibitorServers += server
        true
    }
  }

  def removeServer(server: ExhibitorServer): Boolean = {
    servers().find(_.id == server.id) match {
      case Some(_) =>
        exhibitorServers -= server
        true
      case None =>
        false
    }
  }

  def contains(id: String): Boolean = servers().exists(_.id == id)

  def length() = servers().length

  def clear() = exhibitorServers.clear()

  def expandIds(expr: String): List[String] = {
    if (expr == null || expr == "") throw new IllegalArgumentException("ID expression cannot be null or empty")
    else {
      expr.split(",").flatMap { part =>
        if (part == "*") return servers().map(_.id).toList
        else Util.Range(part).values.map(_.toString)
      }.distinct.sorted.toList
    }
  }

  def save() = storage.save(this)(Cluster.writer)

  def load() {
    storage.load(Cluster.reader).foreach { cluster =>
      this.frameworkId = cluster.frameworkId
      cluster.servers().foreach(this.exhibitorServers += _)
    }
  }

  def isReconciling: Boolean = servers().exists(_.isReconciling)

  override def toString: String = servers().toString()
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
    override def writes(o: Cluster): JsValue = Json.obj("frameworkid" -> o.frameworkId, "cluster" -> o.servers())
  }

  implicit val reader = ((__ \ 'frameworkid).readNullable[String] and
    (__ \ 'cluster).read[List[ExhibitorServer]]) ((frameworkId, servers) => {
    val cluster = Cluster(servers)
    cluster.frameworkId = frameworkId
    cluster
  })
}
