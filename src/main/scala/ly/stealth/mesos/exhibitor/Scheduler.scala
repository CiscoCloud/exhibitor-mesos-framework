package ly.stealth.mesos.exhibitor

import java.util

import ly.stealth.mesos.exhibitor.Util.Str
import org.apache.log4j.Logger
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, Protos, SchedulerDriver}
import scopt.OptionParser

import scala.collection.JavaConversions._
import scala.collection.mutable

case class SchedulerConfig(master: String = "master", httpServerHost: String = "master", httpServerPort: Int = 6666,
                           user: String = "root", cpuPerTask: Double = 1, memPerTask: Double = 256,
                           hosts: List[String] = Nil)

object Scheduler extends org.apache.mesos.Scheduler {
  private val logger = Logger.getLogger(this.getClass)

  private var schedulerConfig: SchedulerConfig = null

  private val runningHosts: mutable.Set[String] = mutable.Set()
  private val tasks: mutable.Map[TaskID, String] = mutable.HashMap()

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

      opt[Double]('c', "cpu.per.task").optional().text("CPUs per task.").action { (value, config) =>
        config.copy(cpuPerTask = value)
      }

      opt[Double]('r', "mem.per.task").optional().text("Memory per task.").action { (value, config) =>
        config.copy(memPerTask = value)
      }

      opt[String]('z', "host").required().text("Zookeeper host").action { (value, config) =>
        config.copy(hosts = config.hosts :+ value)
      }
    }

    parser.parse(args, SchedulerConfig()) match {
      case Some(config) => config
      case None => sys.exit(1)
    }
  }

  def main(args: Array[String]) {
    schedulerConfig = parseSchedulerConfig(args)

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

    status.getState match {
      case TaskState.TASK_LOST | TaskState.TASK_FINISHED | TaskState.TASK_FAILED |
           TaskState.TASK_KILLED | TaskState.TASK_ERROR => synchronized {
        val hostOpt = tasks.get(status.getTaskId)
        tasks -= status.getTaskId
        hostOpt match {
          case Some(host) => runningHosts -= host
          case None => logger.warn("Received status update for unknown host. Is everything ok?")
        }
      }
      case _ =>
    }
  }

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {
    logger.info("[frameworkMessage] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " data: " + new String(data))
  }

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {
    logger.info("[resourceOffers]\n" + Str.offers(offers))

    val hostsToStart = schedulerConfig.hosts.filter(host => !runningHosts.contains(host)).toSet

    offers.foreach { offer =>
      hostsToStart.find(_ == offer.getHostname) match {
        case Some(host) =>
          createTask(host, offer) match {
            case Some(taskInfo) =>
              tasks += taskInfo.getTaskId -> host
              runningHosts += host
              driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(taskInfo), Filters.newBuilder().setRefuseSeconds(1).build)
            case None =>
              logger.warn(s"Host $host needs a task to be launched but lacks resources")
              driver.declineOffer(offer.getId)
          }
        case None =>
          logger.debug(s"Host ${offer.getHostname} is not present in host list, skipping")
          driver.declineOffer(offer.getId)
      }
    }
  }

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {
    logger.info("[executorLost] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " status:" + status)
  }

  private def createTask(host: String, offer: Offer): Option[TaskInfo] = {
    val cpus = Util.getScalarResources(offer, "cpus")
    val mem = Util.getScalarResources(offer, "mem")
    val ports = Util.getRangeResources(offer, "ports")

    val portOpt = ports.headOption.map(_.getBegin)

    if (cpus > schedulerConfig.cpuPerTask && mem > schedulerConfig.memPerTask && portOpt.nonEmpty) {
      val id = s"exhibitor-$host-${portOpt.get}"
      val taskId = TaskID.newBuilder().setValue(id).build
      val taskInfo = TaskInfo.newBuilder().setName(taskId.getValue).setTaskId(taskId).setSlaveId(offer.getSlaveId)
        .setExecutor(this.createExecutor(id, portOpt.get))
        .addResources(Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(schedulerConfig.cpuPerTask)))
        .addResources(Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(schedulerConfig.memPerTask)))
        .addResources(Protos.Resource.newBuilder().setName("ports").setType(Protos.Value.Type.RANGES).setRanges(
        Protos.Value.Ranges.newBuilder().addRange(Protos.Value.Range.newBuilder().setBegin(portOpt.get).setEnd(portOpt.get))
      )).build

      Some(taskInfo)
    } else None
  }

  private def createExecutor(id: String, port: Long): ExecutorInfo = {
    ???
  }
}