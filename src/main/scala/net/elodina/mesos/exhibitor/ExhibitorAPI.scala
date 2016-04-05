package net.elodina.mesos.exhibitor

import play.api.libs.functional.syntax._
import play.api.libs.json._

case class Result(succeeded: Boolean, message: String)

object Result {
  implicit val reader = Json.reads[Result]
}

// have to use this to overcome 22 fields limitation
case class Ports(client: Int, connect: Int, election: Int)

object Ports {
  implicit val reader = (
    (__ \ 'clientPort).read[Int] and
      (__ \ 'connectPort).read[Int] and
      (__ \ 'electionPort).read[Int]) (Ports.apply _)
}

case class SharedConfig(logIndexDirectory: String, zookeeperInstallDirectory: String, zookeeperDataDirectory: String,
                        zookeeperLogDirectory: String, serversSpec: String, backupExtra: String, zooCfgExtra: Map[String, String],
                        javaEnvironment: String, log4jProperties: String, ports: Ports, checkMs: Long, cleanupPeriodMs: Long, cleanupMaxFiles: Int,
                        backupMaxStoreMs: Long, backupPeriodMs: Long, autoManageInstances: Int, autoManageInstancesSettlingPeriodMs: Long,
                        observerThreshold: Int, autoManageInstancesFixedEnsembleSize: Int, autoManageInstancesApplyAllAtOnce: Int)

object SharedConfig {
  implicit val reader = (
    (__ \ 'logIndexDirectory).read[String] and
      (__ \ 'zookeeperInstallDirectory).read[String] and
      (__ \ 'zookeeperDataDirectory).read[String] and
      (__ \ 'zookeeperLogDirectory).read[String] and
      (__ \ 'serversSpec).read[String] and
      (__ \ 'backupExtra).read[String] and
      (__ \ 'zooCfgExtra).read[Map[String, String]] and
      (__ \ 'javaEnvironment).read[String] and
      (__ \ 'log4jProperties).read[String] and
      Ports.reader and
      (__ \ 'checkMs).read[Long] and
      (__ \ 'cleanupPeriodMs).read[Long] and
      (__ \ 'cleanupMaxFiles).read[Int] and
      (__ \ 'backupMaxStoreMs).read[Long] and
      (__ \ 'backupPeriodMs).read[Long] and
      (__ \ 'autoManageInstances).read[Int] and
      (__ \ 'autoManageInstancesSettlingPeriodMs).read[Long] and
      (__ \ 'observerThreshold).read[Int] and
      (__ \ 'autoManageInstancesFixedEnsembleSize).read[Int] and
      (__ \ 'autoManageInstancesApplyAllAtOnce).read[Int]) (SharedConfig.apply _)

  // Exhibitor for some reason requires the values passed back to be strings, so have to define custom writer for it.
  implicit val writer = new Writes[SharedConfig] {
    def writes(sc: SharedConfig): JsValue = {
      Json.obj(
        "logIndexDirectory" -> sc.logIndexDirectory,
        "zookeeperInstallDirectory" -> sc.zookeeperInstallDirectory,
        "zookeeperDataDirectory" -> sc.zookeeperDataDirectory,
        "zookeeperLogDirectory" -> sc.zookeeperLogDirectory,
        "serversSpec" -> sc.serversSpec,
        "backupExtra" -> sc.backupExtra,
        "zooCfgExtra" -> sc.zooCfgExtra,
        "javaEnvironment" -> sc.javaEnvironment,
        "log4jProperties" -> sc.log4jProperties,
        "clientPort" -> sc.ports.client.toString,
        "connectPort" -> sc.ports.connect.toString,
        "electionPort" -> sc.ports.election.toString,
        "checkMs" -> sc.checkMs.toString,
        "cleanupPeriodMs" -> sc.cleanupPeriodMs.toString,
        "cleanupMaxFiles" -> sc.cleanupMaxFiles.toString,
        "backupMaxStoreMs" -> sc.backupMaxStoreMs.toString,
        "backupPeriodMs" -> sc.backupPeriodMs.toString,
        "autoManageInstances" -> sc.autoManageInstances.toString,
        "autoManageInstancesSettlingPeriodMs" -> sc.autoManageInstancesSettlingPeriodMs.toString,
        "observerThreshold" -> sc.observerThreshold.toString,
        "autoManageInstancesFixedEnsembleSize" -> sc.autoManageInstancesFixedEnsembleSize.toString,
        "autoManageInstancesApplyAllAtOnce" -> sc.autoManageInstancesApplyAllAtOnce.toString
      )
    }
  }
}

case class ExhibitorServerStatus(hostname: String, isLeader: Boolean, description: String, code: Int)

object ExhibitorServerStatus {
  implicit val reader = (
    (__ \ 'hostname).read[String] and
      (__ \ 'isLeader).read[Boolean] and
      (__ \ 'description).read[String] and
      (__ \ 'code).read[Int]) (ExhibitorServerStatus.apply _)

  implicit val writer = new Writes[ExhibitorServerStatus] {
    def writes(ess: ExhibitorServerStatus): JsValue = {
      Json.obj(
        "hostname" -> ess.hostname,
        "isLeader" -> ess.isLeader,
        "description" -> ess.description,
        "code" -> ess.code)
    }
  }
}
