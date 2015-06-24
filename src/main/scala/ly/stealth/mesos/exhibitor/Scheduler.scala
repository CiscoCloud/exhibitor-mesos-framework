package ly.stealth.mesos.exhibitor

import java.util

import com.google.protobuf.ByteString
import ly.stealth.mesos.exhibitor.Util.Str
import org.apache.log4j.Logger
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}

import scala.collection.JavaConversions._

object Scheduler extends org.apache.mesos.Scheduler {
  private val logger = Logger.getLogger(this.getClass)

  private[exhibitor] val cluster = Cluster()

  def start() {
    logger.info(s"Starting ${getClass.getSimpleName}:\n$Config")

    HttpServer.start()

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser(Config.user)
    frameworkBuilder.setName("Exhibitor")

    val driver = new MesosSchedulerDriver(this, frameworkBuilder.build, Config.master)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        if (driver != null) driver.stop()
      }
    })

    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    HttpServer.stop()
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

    onServerStatus(driver, status)
  }

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {
    logger.info("[frameworkMessage] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " data: " + new String(data))
  }

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {
    logger.info("[resourceOffers]\n" + Str.offers(offers))

    offers.foreach { offer =>
      cluster.servers.find(_.state == ExhibitorServer.Stopped) match {
        case Some(server) =>
          server.createTask(offer) match {
            case Some(taskInfo) =>
              server.state = ExhibitorServer.Staging
              driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(taskInfo), Filters.newBuilder().setRefuseSeconds(1).build)
            case None =>
              logger.warn(s"Host ${offer.getHostname} lacks resources for a task to be launched")
              driver.declineOffer(offer.getId)
          }
        case None =>
          logger.debug(s"All servers are running")
          driver.declineOffer(offer.getId)
      }
    }
  }

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {
    logger.info("[executorLost] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " status:" + status)
  }

  private def onServerStatus(driver: SchedulerDriver, status: TaskStatus) {
    val server = cluster.getServer(ExhibitorServer.idFromTaskId(status.getTaskId.getValue))

    status.getState match {
      case TaskState.TASK_RUNNING =>
        onServerStarted(server, driver, status)
      case TaskState.TASK_LOST | TaskState.TASK_FINISHED |
           TaskState.TASK_FAILED | TaskState.TASK_KILLED |
           TaskState.TASK_ERROR =>
        onServerStopped(server, status)
      case _ => logger.warn("Got unexpected task state: " + status.getState)
    }
  }

  private def onServerStarted(serverOpt: Option[ExhibitorServer], driver: SchedulerDriver, status: TaskStatus) {
    serverOpt match {
      case Some(server) => server.state = ExhibitorServer.Running
      case None =>
        logger.info(s"Got ${status.getState} for unknown/stopped server, killing task ${status.getTaskId}")
        driver.killTask(status.getTaskId)
    }
  }

  private def onServerStopped(serverOpt: Option[ExhibitorServer], status: TaskStatus) {
    serverOpt match {
      case Some(server) => server.state = ExhibitorServer.Stopped
      case None => logger.info(s"Got ${status.getState} for unknown/stopped server with task ${status.getTaskId}")
    }
  }

  private[exhibitor] def newExecutor(id: String): ExecutorInfo = {
    val commandBuilder = CommandInfo.newBuilder()
    commandBuilder
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/exhibitor/" + HttpServer.exhibitorDist.getName))
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jar/" + HttpServer.jar.getName))
      .setValue(s"java -cp ${HttpServer.jar.getName} ly.stealth.mesos.exhibitor.Executor")

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue(id))
      .setCommand(commandBuilder)
      .setName(s"exhibitor-$id")
      .build
  }

  private[exhibitor] def taskData(port: Long): ByteString = {
    ByteString.copyFromUtf8(s"configtype=file,port=$port") //TODO this should come from outside
  }
}