package net.elodina.mesos.exhibitor

import org.apache.mesos.Protos.{Offer, Resource, Value}
import scala.collection.JavaConversions._

import scala.collection.mutable.ListBuffer

case class Reservation(cpu: Double = 0, mem: Double = 0, uiPort: Int = -1, connectPort: Int = -1, clientPort: Int = -1, electionPort: Int = -1) {
  def toResources: List[Resource] = {
    def cpusResource(value: Double): Resource = {
      Resource.newBuilder
        .setName("cpus")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder.setValue(value))
        .setRole("*")
        .build()
    }

    def memResource(value: Double): Resource = {
      Resource.newBuilder
        .setName("mem")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder.setValue(value))
        .setRole("*")
        .build()
    }

    def portsResource(values: List[Long]): Resource = {
      val ranges = Value.Ranges.newBuilder
      values.map(value => ranges.addRange(Value.Range.newBuilder().setBegin(value).setEnd(value)))
      Resource.newBuilder
        .setName("ports")
        .setType(Value.Type.RANGES)
        .setRanges(ranges)
        .setRole("*")
        .build()
    }

    val resources: ListBuffer[Resource] = new ListBuffer[Resource]()

    if (cpu > 0) resources += cpusResource(cpu)
    if (mem > 0) resources += memResource(mem)

    resources += portsResource(List[Long](uiPort, clientPort, connectPort, electionPort).filter(_ != -1))
    resources.toList
  }
}

object Reservation {
  def reserve(offer: Offer, ensemble: Ensemble, server: Exhibitor): Either[String, Reservation] = {
    val resources = offer.getResourcesList.toList.map(res => res.getName -> res).toMap

    resources.get("cpus") match {
      case Some(cpusResource) => if (cpusResource.getScalar.getValue < server.config.cpus) return Left(s"cpus ${cpusResource.getScalar.getValue} < ${server.config.cpus}")
      case None => return Left("no cpus")
    }

    resources.get("mem") match {
      case Some(memResource) => if (memResource.getScalar.getValue < server.config.mem) return Left(s"mem ${memResource.getScalar.getValue} < ${server.config.mem}")
      case None => return Left("no mem")
    }

    val ports = Util.getRangeResources(offer, "ports").map(r => Util.Range(r.getBegin.toInt, r.getEnd.toInt))

    val clientPort = reservePort(ports, getPreferredSharedPort(ensemble.clientPort, server.config.sharedConfigOverride.get(ConfigNames.CLIENT_PORT)), Nil) match {
      case Some(port) => port
      case None => return Left("No suitable client port")
    }

    val connectPort = reservePort(ports, getPreferredSharedPort(ensemble.connectPort, server.config.sharedConfigOverride.get(ConfigNames.CONNECT_PORT)), List(clientPort)) match {
      case Some(port) => port
      case None => return Left("No suitable connect port")
    }

    val electionPort = reservePort(ports, getPreferredSharedPort(ensemble.electionPort, server.config.sharedConfigOverride.get(ConfigNames.ELECTION_PORT)), List(clientPort, connectPort)) match {
      case Some(port) => port
      case None => return Left("No suitable election port")
    }

    val uiPort = reservePort(ports, Some(server.config.ports).filter(_.nonEmpty), List(clientPort, connectPort, electionPort)) match {
      case Some(port) => port
      case None => return Left("No suitable UI port")
    }

    Right(Reservation(server.config.cpus, server.config.mem, uiPort, connectPort, clientPort, electionPort))
  }

  private[exhibitor] def reservePort(ports: List[Util.Range], preferredRange: Option[List[Util.Range]], occupiedPorts: List[Int]): Option[Int] = {
    preferredRange match {
      case Some(preferred) =>
        ports.flatMap(range => preferred.flatMap(range.overlap)).flatMap(_.values).diff(occupiedPorts).headOption
      case None => ports.flatMap(_.values).diff(occupiedPorts).headOption
    }
  }

  private[exhibitor] def reservePort(ports: List[Util.Range], preferredPort: Int, occupiedPorts: List[Int]): Option[Int] = {
    preferredPort match {
      case -1 => reservePort(ports, None, occupiedPorts)
      case preferred => reservePort(ports, Some(List(Util.Range(preferred))), occupiedPorts)
    }
  }

  private[exhibitor] def getPreferredSharedPort(ensembleValue: Int, serverValue: => Option[String]): Int = {
    Option(ensembleValue).filter(_ != -1)
      .orElse(serverValue.map(_.toInt))
      .getOrElse(-1)
  }
}
