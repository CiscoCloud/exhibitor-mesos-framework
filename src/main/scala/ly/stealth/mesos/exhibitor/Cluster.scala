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

import play.api.libs.json._

import scala.collection.mutable.ListBuffer

case class Cluster(exhibitorServers: List[ExhibitorServer] = Nil) {
  private[exhibitor] val servers = new ListBuffer[ExhibitorServer]
  //add anything that was passed to constructor
  exhibitorServers.foreach(servers += _)

  def getServer(id: String): Option[ExhibitorServer] = servers.find(_.id == id)

  def addServer(server: ExhibitorServer): Boolean = {
    servers.find(_.id == server.id) match {
      case Some(_) => false
      case None =>
        servers += server
        true
    }
  }

  def expandIds(expr: String): List[String] = {
    if (expr == null || expr == "") throw new IllegalArgumentException("ID expression cannot be null or empty")
    else {
      expr.split(",").flatMap { part =>
        if (part == "*") return servers.map(_.id).toList
        else Util.Range(part).values.map(_.toString)
      }.distinct.sorted.toList
    }
  }

  override def toString: String = servers.toString()
}

object Cluster {
  implicit val writer = new Writes[Cluster] {
    override def writes(o: Cluster): JsValue = Json.obj("cluster" -> o.servers.toList)
  }

  implicit val reader = Reads.at[List[ExhibitorServer]](__ \ 'cluster).map(Cluster(_))
}
