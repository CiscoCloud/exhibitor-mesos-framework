package ly.stealth.mesos.exhibitor

import java.util

import ly.stealth.mesos.exhibitor.Util.Str
import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object Scheduler extends org.apache.mesos.Scheduler {
  private val logger = Logger.getLogger(this.getClass)

  private[exhibitor] val cluster = Cluster()

  def start() {
    initLogging()
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
    logger.debug("[resourceOffers]\n" + Str.offers(offers))

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
      case Some(server) =>
        server.state = ExhibitorServer.Running
        this.synchronized {
          //TODO maybe should add it only once? not sure how often we receive Running statuses
          logger.info(s"Adding server ${server.id} to ensemble")
          addToEnsemble(server)
        }
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

  private def addToEnsemble(server: ExhibitorServer) {
    val retries = 60 //TODO this should probably be configurable

    def tryAddToEnsemble(retriesLeft: Int, backoffMs: Long) {
      val sharedConfig = getSharedConfig(server, retriesLeft)

      val updatedSharedConfig = server.config.sharedConfigOverride.foldLeft(sharedConfig) { case (conf, (key, value)) =>
        key match {
          case "zookeeper-install-directory" => conf.copy(zookeeperInstallDirectory = value)
          case "zookeeper-data-directory" => conf.copy(zookeeperDataDirectory = value)
          case invalid => throw new IllegalArgumentException(s"Unacceptable shared configuration parameter: $invalid")
        }
      }

      val updatedServersSpec = (s"S:${server.config.id}:${server.config.hostname}" :: updatedSharedConfig.serversSpec.split(",").foldLeft(List[String]()) { (servers, srv) =>
        srv.split(":") match {
          case Array(_, _, serverHost) if serverHost == server.config.hostname => servers
          case Array(_, _, serverHost) => srv :: servers
          case _ => servers
        }
      }).mkString(",")

      Try(ExhibitorAPI.setConfig(updatedSharedConfig.copy(serversSpec = updatedServersSpec), server.url)) match {
        case Success(_) =>
        case Failure(e) =>
          logger.debug(s"Failed to save Exhibitor Shared Configuration: ${e.getMessage}")
          if (retriesLeft > 0) {
            logger.debug("Retrying...")
            Thread.sleep(backoffMs)
            tryAddToEnsemble(retries - 1, backoffMs)
          } else throw new IllegalStateException(s"Failed to save Exhibitor Shared Configuration after $retries retries")
      }
    }

    tryAddToEnsemble(retries, 1000)
  }

  private def getSharedConfig(server: ExhibitorServer, retries: Int): SharedConfig = {
    def tryGetConfig(retriesLeft: Int, backoffMs: Long): SharedConfig = {
      Try(ExhibitorAPI.getSystemState(server.url)) match {
        case Success(cfg) =>
          if (cfg.zookeeperInstallDirectory == "") {
            if (retriesLeft > 0) {
              Thread.sleep(backoffMs)
              tryGetConfig(retriesLeft - 1, backoffMs)
            } else {
              logger.info(s"Failed to get non-default Exhibitor Shared Configuration after $retries retries. Using default.")
              cfg
            }
          } else cfg
        case Failure(e) =>
          logger.info("Exhibitor API not available.")
          if (retriesLeft > 0) {
            Thread.sleep(backoffMs)
            tryGetConfig(retriesLeft - 1, backoffMs)
          } else throw new IllegalStateException(s"Failed to get Exhibitor Shared Configuration after $retries retries")
      }
    }

    tryGetConfig(retries, 1000)
  }

  private def initLogging() {
    System.setProperty("org.eclipse.jetty.util.log.class", classOf[JettyLog4jLogger].getName)

    BasicConfigurator.resetConfiguration()

    val root = Logger.getRootLogger
    root.setLevel(Level.INFO)

    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.I0Itec.zkclient").setLevel(Level.WARN)

    val logger = Logger.getLogger(Scheduler.getClass)
    logger.setLevel(if (Config.debug) Level.DEBUG else Level.INFO)
    Logger.getLogger(ExhibitorAPI.getClass).setLevel(if (Config.debug) Level.DEBUG else Level.INFO)

    val layout = new PatternLayout("%d [%t] %-5p %c %x - %m%n")

    val appender: Appender = new ConsoleAppender(layout)

    root.addAppender(appender)
  }

  class JettyLog4jLogger extends org.eclipse.jetty.util.log.Logger {
    private var logger: Logger = Logger.getLogger("Jetty")

    def this(logger: Logger) {
      this()
      this.logger = logger
    }

    def isDebugEnabled: Boolean = logger.isDebugEnabled

    def setDebugEnabled(enabled: Boolean) = logger.setLevel(if (enabled) Level.DEBUG else Level.INFO)

    def getName: String = logger.getName

    def getLogger(name: String): org.eclipse.jetty.util.log.Logger = new JettyLog4jLogger(Logger.getLogger(name))

    def info(s: String, args: AnyRef*) = logger.info(format(s, args))

    def info(s: String, t: Throwable) = logger.info(s, t)

    def info(t: Throwable) = logger.info("", t)

    def debug(s: String, args: AnyRef*) = logger.debug(format(s, args))

    def debug(s: String, t: Throwable) = logger.debug(s, t)

    def debug(t: Throwable) = logger.debug("", t)

    def warn(s: String, args: AnyRef*) = logger.warn(format(s, args))

    def warn(s: String, t: Throwable) = logger.warn(s, t)

    def warn(s: String) = logger.warn(s)

    def warn(t: Throwable) = logger.warn("", t)

    def ignore(t: Throwable) = logger.info("Ignored", t)
  }

  private def format(s: String, args: AnyRef*): String = {
    var result: String = ""
    var i: Int = 0

    for (token <- s.split("\\{\\}")) {
      result += token
      if (args.length > i) result += args(i)
      i += 1
    }

    result
  }
}