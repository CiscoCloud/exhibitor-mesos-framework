package ly.stealth.mesos.exhibitor

import java.util

import ly.stealth.mesos.exhibitor.Util.Str
import org.apache.log4j.Logger
import org.apache.mesos.{SchedulerDriver, MesosSchedulerDriver}
import org.apache.mesos.Protos._
import scopt.OptionParser

import scala.collection.JavaConversions._

case class SchedulerConfig(master: String = "master", httpServerHost: String = "master", httpServerPort: Int = 6666,
                            user: String = "root")

object Scheduler extends org.apache.mesos.Scheduler {
  private val logger = Logger.getLogger(this.getClass)

  private def parseSchedulerConfig(args: Array[String]): SchedulerConfig = {
    val parser = new OptionParser[SchedulerConfig]("Scheduler") {
      opt[String]('m', "master").required().text("Mesos Master addresses.").action { (value, config) =>
        config.copy(master = value)
      }

      opt[String]('h', "http.server.host").optional().text("Binding host for http/artifact server.").action { (value, config) =>
        config.copy(httpServerHost = value)
      }

      opt[Int]('p', "http.server.port").optional().text("Binding port for http/artifact server.").action { (value, config) =>
        config.copy(httpServerPort = value)
      }

      opt[String]('u', "user").required().text("Mesos user.").action { (value, config) =>
        config.copy(user = value)
      }
    }

    parser.parse(args, SchedulerConfig()) match {
      case Some(config) => config
      case None => sys.exit(1)
    }
  }

  def main(args: Array[String]) {
    val schedulerConfig = parseSchedulerConfig(args)

    val server = new HttpServer(schedulerConfig)

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser(schedulerConfig.user)
    frameworkBuilder.setName("Exhibitor")

    val driver = new MesosSchedulerDriver(this, frameworkBuilder.build, schedulerConfig.master)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        if (driver != null) driver.stop()
      }
    })

    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    server.stop()
    sys.exit(status)
  }

  override def registered(driver: SchedulerDriver, id: FrameworkID, master: MasterInfo) {
    logger.info("[registered] framework:" + Str.id(id.getValue) + " master:" + Str.master(master))
  }

  override def offerRescinded(driver: SchedulerDriver, id: OfferID) {
    logger.info("[offerRescinded] " + Str.id(id.getValue))
  }

  override def disconnected(driver: SchedulerDriver) {
    logger.info("[disconnected]")
  }

  override def reregistered(driver: SchedulerDriver, master: MasterInfo) {
    logger.info("[reregistered] master:" + Str.master(master))
  }

  override def slaveLost(driver: SchedulerDriver, id: SlaveID) {
    logger.info("[slaveLost] " + Str.id(id.getValue))
  }

  override def error(driver: SchedulerDriver, message: String) {
    logger.info("[error] " + message)
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    logger.info("[statusUpdate] " + Str.taskStatus(status))
  }

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {
    logger.info("[frameworkMessage] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " data: " + new String(data))
  }

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {
    logger.info("[resourceOffers]\n" + Str.offers(offers))
  }

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {
    logger.info("[executorLost] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " status:" + status)
  }
}