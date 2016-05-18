package net.elodina.mesos.exhibitor

import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection.mutable.ListBuffer

case class Ensemble(id: String, var clientPort: Int = -1, var connectPort: Int = -1, var electionPort: Int = -1,
                    initialServers: List[Exhibitor] = Nil) {

  private[exhibitor] val exhibitorServers: ListBuffer[Exhibitor] = new ListBuffer[Exhibitor]
  initialServers.foreach(exhibitorServers += _)

  def updatePorts(reservation: Reservation) {
    clientPort = reservation.clientPort
    connectPort = reservation.connectPort
    electionPort = reservation.electionPort
  }

  def servers(): List[Exhibitor] = this.exhibitorServers.toList

  def servers(state: Exhibitor.State): List[Exhibitor] = servers().filter(_.state == state)

  def runningServers(): List[Exhibitor] = servers().filter(_.task != null)

  def getServer(id: String): Option[Exhibitor] = servers().find(_.id == id)

  def findWithState(state: Exhibitor.State): Option[Exhibitor] = servers().find(_.state == state)

  def addServer(server: Exhibitor): Boolean = {
    servers().find(_.id == server.id) match {
      case Some(_) => false
      case None =>
        exhibitorServers += server
        true
    }
  }

  def removeServer(server: Exhibitor): Boolean = {
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
        if (part == "*") return servers().map(_.id)
        else Util.Range(part).values.map(_.toString)
      }.distinct.sorted.toList
    }
  }

  def isReconciling: Boolean = servers().exists(_.isReconciling)
}

object Ensemble {
  implicit val writer = new Writes[Ensemble] {
    override def writes(o: Ensemble): JsValue = Json.obj(
      "id" -> o.id,
      "clientPort" -> o.clientPort,
      "connectPort" -> o.connectPort,
      "electionPort" -> o.electionPort,
      "servers" -> o.servers()
    )
  }

  implicit val reader = (
    (__ \ 'id).read[String] and
      (__ \ 'clientPort).read[Int] and
      (__ \ 'connectPort).read[Int] and
      (__ \ 'electionPort).read[Int] and
      (__ \ 'servers).read[List[Exhibitor]]) (Ensemble.apply _)
}
