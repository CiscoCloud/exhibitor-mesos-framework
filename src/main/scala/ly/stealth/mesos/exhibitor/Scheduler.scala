package ly.stealth.mesos.exhibitor

import java.util
import java.util.concurrent.TimeUnit
import java.util.{Collections, Date}

import ly.stealth.mesos.exhibitor.Util.Str
import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object Scheduler extends org.apache.mesos.Scheduler {
  private val logger = Logger.getLogger(this.getClass)

  private[exhibitor] val cluster = Cluster()
  private var driver: SchedulerDriver = null

  def start() {
    initLogging()
    logger.info(s"Starting ${getClass.getSimpleName}:\n$Config")

    cluster.load()
    HttpServer.start()

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser(Config.user)
    cluster.frameworkId.foreach(id => frameworkBuilder.setId(FrameworkID.newBuilder().setValue(id)))
    frameworkBuilder.setName(Config.frameworkName)
    frameworkBuilder.setFailoverTimeout(Config.frameworkTimeout.toUnit(TimeUnit.SECONDS))
    frameworkBuilder.setCheckpoint(true)

    val driver = new MesosSchedulerDriver(this, frameworkBuilder.build, Config.master)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        HttpServer.stop()
      }
    })

    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    sys.exit(status)
  }

  override def registered(driver: SchedulerDriver, id: FrameworkID, master: MasterInfo) {
    logger.info("[registered] framework:" + Str.id(id.getValue) + " master:" + Str.master(master))

    cluster.frameworkId = Some(id.getValue)
    cluster.save()

    this.driver = driver
    reconcileTasks(force = true)
  }

  override def offerRescinded(driver: SchedulerDriver, id: OfferID) {
    logger.info("[offerRescinded] " + Str.id(id.getValue))
  }

  override def disconnected(driver: SchedulerDriver) {
    logger.info("[disconnected]")

    this.driver = null
  }

  override def reregistered(driver: SchedulerDriver, master: MasterInfo) {
    logger.info("[reregistered] master:" + Str.master(master))

    this.driver = driver
    reconcileTasks(force = true)
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

    onResourceOffers(offers.toList)
  }

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {
    logger.info("[executorLost] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " status:" + status)
  }

  private def onResourceOffers(offers: List[Offer]) {
    offers.foreach { offer =>
      acceptOffer(offer).foreach { declineReason =>
        driver.declineOffer(offer.getId)
        logger.info(s"Declined offer:\n  $declineReason")
      }
    }

    reconcileTasks()
    Scheduler.cluster.save()
  }

  private[exhibitor] def acceptOffer(offer: Offer): Option[String] = {
    cluster.servers.filter(_.state == ExhibitorServer.Stopped).toList match {
      case Nil => Some("all servers are running")
      case servers =>
        val reason = servers.flatMap { server =>
          server.matches(offer, otherTasksAttributes) match {
            case Some(declineReason) => Some(s"server ${server.id}: $declineReason")
            case None =>
              launchTask(server, offer)
              return None
          }
        }.mkString(", ")

        if (reason.isEmpty) None else Some(reason)
    }
  }

  private def launchTask(server: ExhibitorServer, offer: Offer) {
    val task = server.createTask(offer)
    val taskId = task.getTaskId.getValue
    val attributes = offer.getAttributesList.toList.filter(_.hasText).map(attr => attr.getName -> attr.getText.getValue).toMap

    server.task = ExhibitorServer.Task(taskId, task.getSlaveId.getValue, task.getExecutor.getExecutorId.getValue, attributes)
    server.state = ExhibitorServer.Staging
    driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(task), Filters.newBuilder().setRefuseSeconds(1).build)

    logger.info(s"Starting server ${server.id}: launching task $taskId for offer ${offer.getId.getValue}")
  }

  private def onServerStatus(driver: SchedulerDriver, status: TaskStatus) {
    val server = cluster.getServer(ExhibitorServer.idFromTaskId(status.getTaskId.getValue))

    status.getState match {
      case TaskState.TASK_RUNNING =>
        new Thread {
          override def run() {
            onServerStarted(server, driver, status)
          }
        }.start()
      case TaskState.TASK_LOST | TaskState.TASK_FAILED | TaskState.TASK_ERROR =>
        onServerFailed(server, status)
      case TaskState.TASK_FINISHED | TaskState.TASK_KILLED => logger.info(s"Task ${status.getTaskId.getValue} has finished")
      case _ => logger.warn("Got unexpected task state: " + status.getState)
    }

    Scheduler.cluster.save()
  }

  private def onServerStarted(serverOpt: Option[ExhibitorServer], driver: SchedulerDriver, status: TaskStatus) {
    serverOpt match {
      case Some(server) =>
        this.synchronized {
          if (server.state != ExhibitorServer.Running) {
            logger.info(s"Adding server ${server.id} to ensemble")
            server.state = ExhibitorServer.Running
            addToEnsemble(server)
          }
        }
      case None =>
        logger.info(s"Got ${status.getState} for unknown/stopped server, killing task ${status.getTaskId}")
        driver.killTask(status.getTaskId)
    }
  }

  private def onServerFailed(serverOpt: Option[ExhibitorServer], status: TaskStatus) {
    serverOpt match {
      case Some(server) => server.state = ExhibitorServer.Stopped
      case None => logger.info(s"Got ${status.getState} for unknown/stopped server with task ${status.getTaskId}")
    }
  }

  private[exhibitor] def stopServer(id: String): Option[ExhibitorServer] = {
    cluster.getServer(id).map { server =>
      if (server.state == ExhibitorServer.Staging || server.state == ExhibitorServer.Running)
        driver.killTask(TaskID.newBuilder().setValue(server.task.id).build())

      server.state = ExhibitorServer.Added
      server.task = null
      server
    }
  }

  private[exhibitor] def removeServer(id: String): Option[ExhibitorServer] = {
    cluster.getServer(id).map { server =>
      stopServer(id)

      cluster.servers -= server
      removeFromEnsemble(server)
      server
    }
  }

  private def addToEnsemble(server: ExhibitorServer) {
    def tryAddToEnsemble(retriesLeft: Int) {
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
          // ignore duplicate ids or unknown instances
          case Array(_, id, _) if id == server.id || !cluster.contains(id) => servers
          case Array(_, _, _) => srv :: servers
          case _ => servers
        }
      }).sorted.mkString(",")

      Try(ExhibitorAPI.setConfig(updatedSharedConfig.copy(serversSpec = updatedServersSpec), server.url)) match {
        case Success(_) =>
        case Failure(e) =>
          logger.debug(s"Failed to save Exhibitor Shared Configuration: ${e.getMessage}")
          if (retriesLeft > 0) {
            logger.debug("Retrying...")
            Thread.sleep(Config.ensembleModifyBackoff)
            tryAddToEnsemble(retriesLeft - 1)
          } else throw new IllegalStateException(s"Failed to save Exhibitor Shared Configuration after ${Config.ensembleModifyRetries} retries")
      }
    }

    tryAddToEnsemble(Config.ensembleModifyRetries)
  }

  private def removeFromEnsemble(server: ExhibitorServer) {
    def tryRemoveFromEnsemble(aliveServer: ExhibitorServer, retriesLeft: Int) {
      val sharedConfig = getSharedConfig(aliveServer, retriesLeft)

      val updatedServersSpec = sharedConfig.serversSpec.split(",").foldLeft(List[String]()) { (servers, srv) =>
        srv.split(":") match {
          case Array(_, _, serverHost) if serverHost == server.config.hostname => servers
          case Array(_, _, serverHost) => srv :: servers
          case _ => servers
        }
      }.sorted.mkString(",")

      Try(ExhibitorAPI.setConfig(sharedConfig.copy(serversSpec = updatedServersSpec), aliveServer.url)) match {
        case Success(_) =>
        case Failure(e) =>
          logger.debug(s"Failed to save Exhibitor Shared Configuration: ${e.getMessage}")
          if (retriesLeft > 0) {
            logger.debug("Retrying...")
            Thread.sleep(Config.ensembleModifyBackoff)
            tryRemoveFromEnsemble(aliveServer, retriesLeft - 1)
          } else throw new IllegalStateException(s"Failed to save Exhibitor Shared Configuration after ${Config.ensembleModifyRetries} retries")
      }
    }

    cluster.servers.find(_.state == ExhibitorServer.Running) match {
      case Some(aliveServer) => tryRemoveFromEnsemble(aliveServer, Config.ensembleModifyRetries)
      case None => logger.info(s"Server ${server.id} was the last alive in the cluster, no need to deregister it from ensemble.")
    }
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

  private[exhibitor] def otherTasksAttributes(name: String): List[String] = {
    def value(server: ExhibitorServer, name: String): Option[String] = {
      if (name == "hostname") Option(server.config.hostname)
      else server.task.attributes.get(name)
    }

    cluster.servers.filter(_.task != null).flatMap(value(_, name)).toList
  }

  private[exhibitor] val RECONCILE_DELAY = 10 seconds
  private[exhibitor] val RECONCILE_MAX_TRIES = 3

  private[exhibitor] var reconciles = 0
  private[exhibitor] var reconcileTime = new Date(0)

  private[exhibitor] def reconcileTasks(force: Boolean = false, now: Date = new Date()) {
    if (now.getTime - reconcileTime.getTime >= RECONCILE_DELAY.toMillis) {
      if (!cluster.isReconciling) reconciles = 0
      reconciles += 1
      reconcileTime = now

      if (reconciles > RECONCILE_MAX_TRIES) {
        cluster.servers.filter(s => s.isReconciling && s.task != null).foreach { server =>
          logger.info(s"Reconciling exceeded $RECONCILE_MAX_TRIES tries for server ${server.id}, sending killTask for task ${server.task.id}")
          driver.killTask(TaskID.newBuilder().setValue(server.task.id).build())
          server.task = null
        }
      } else {
        val statuses = cluster.servers.filter(_.task != null).flatMap { server =>
          if (force || server.isReconciling) {
            server.state = ExhibitorServer.Reconciling
            logger.info(s"Reconciling $reconciles/$RECONCILE_MAX_TRIES state of server ${server.id}, task ${server.task.id}")
            Some(TaskStatus.newBuilder()
              .setTaskId(TaskID.newBuilder().setValue(server.task.id))
              .setState(TaskState.TASK_STAGING)
              .build)
          } else None
        }.toList

        if (force || statuses.nonEmpty) driver.reconcileTasks(if (force) Collections.emptyList() else statuses)
      }
    }
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