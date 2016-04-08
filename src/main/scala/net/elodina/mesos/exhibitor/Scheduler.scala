package net.elodina.mesos.exhibitor

import java.util
import java.util.concurrent.TimeUnit
import java.util.{Collections, Date}

import net.elodina.mesos.exhibitor.Util.Str
import net.elodina.mesos.exhibitor.exhibitorapi._
import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object Scheduler extends org.apache.mesos.Scheduler {
  private val logger = Logger.getLogger(this.getClass)
  private val ensembleLock = new Object()

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
    cluster.servers(Exhibitor.Stopped) match {
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

  private def launchTask(server: Exhibitor, offer: Offer) {
    val task = server.createTask(offer)
    val taskId = task.getTaskId.getValue
    val attributes = offer.getAttributesList.toList.filter(_.hasText).map(attr => attr.getName -> attr.getText.getValue).toMap

    server.task = Exhibitor.Task(taskId, task.getSlaveId.getValue, task.getExecutor.getExecutorId.getValue, attributes)
    server.state = Exhibitor.Staging
    driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(task), Filters.newBuilder().setRefuseSeconds(1).build)

    logger.info(s"Starting server ${server.id}: launching task $taskId for offer ${offer.getId.getValue}")
  }

  private def onServerStatus(driver: SchedulerDriver, status: TaskStatus) {
    val server = cluster.getServer(Exhibitor.idFromTaskId(status.getTaskId.getValue))

    status.getState match {
      case TaskState.TASK_RUNNING =>
        new Thread {
          override def run() {
            onServerStarted(server, driver, status)
          }
        }.start()
      case TaskState.TASK_LOST | TaskState.TASK_FAILED | TaskState.TASK_ERROR =>
        onServerFailed(server, status)
      case TaskState.TASK_FINISHED | TaskState.TASK_KILLED =>
        onServerFinished(server, status)
      case _ => logger.warn("Got unexpected task state: " + status.getState)
    }

    Scheduler.cluster.save()
  }

  private def onServerStarted(serverOpt: Option[Exhibitor], driver: SchedulerDriver, status: TaskStatus) {
    serverOpt match {
      case Some(server) =>
        this.synchronized {
          if (server.state != Exhibitor.Running) {
            server.state = Exhibitor.Running
            addToEnsemble(server).onFailure { case t =>
              logger.info(s"Failed to add server ${server.id} to ensemble, force fail")
              if (server.task != null) {
                driver.sendFrameworkMessage(
                  ExecutorID.newBuilder().setValue(server.task.executorId).build(),
                  SlaveID.newBuilder().setValue(server.task.slaveId).build(),
                  "fail".getBytes)
              }
            }
          }
        }
      case None =>
        logger.info(s"Got ${status.getState} for unknown/stopped server, killing task ${status.getTaskId}")
        driver.killTask(status.getTaskId)
    }
  }

  private def onServerFailed(serverOpt: Option[Exhibitor], status: TaskStatus) {
    serverOpt match {
      case Some(server) =>
        server.state = Exhibitor.Stopped
        server.task = null
        server.config.hostname = ""
      case None => logger.info(s"Got ${status.getState} for unknown/stopped server with task ${status.getTaskId}")
    }
  }

  private def onServerFinished(serverOpt: Option[Exhibitor], status: TaskStatus) {
    serverOpt match {
      case Some(server) =>
        server.state = Exhibitor.Added
        server.task = null
        server.config.hostname = ""
        logger.info(s"Task ${status.getTaskId.getValue} has finished")
      case None => logger.info(s"Got ${status.getState} for unknown/stopped server with task ${status.getTaskId}")
    }
  }

  private[exhibitor] def stopServer(id: String): Option[Exhibitor] = {
    cluster.getServer(id).map { server =>
      if (server.state == Exhibitor.Staging || server.state == Exhibitor.Running)
        driver.killTask(TaskID.newBuilder().setValue(server.task.id).build())

      server.state = Exhibitor.Added
      server
    }
  }

  private[exhibitor] def removeServer(id: String): Option[Exhibitor] = {
    cluster.getServer(id).map { server =>
      stopServer(id)

      cluster.removeServer(server)
      removeFromEnsemble(server).onFailure { case t =>
        logger.info(s"Failed to remove server ${server.id} from ensemble")
      }
      server
    }
  }

  private def addToEnsemble(server: Exhibitor): Future[Unit] = {
    def tryAddToEnsemble(retriesLeft: Int) {
      getSharedConfig(server) match {
        case (Some(sharedConfig), None) =>
          trySaveSharedConfig(sharedConfig, retriesLeft)
        case (Some(sharedConfig), Some(failureMessage)) =>
          if (retriesLeft > 0) {
            logger.debug(s"$failureMessage: retrying...")
            Thread.sleep(Config.ensembleModifyBackoff)
            tryAddToEnsemble(retriesLeft - 1)
          } else {
            logger.info(s"Failed to get non-default Exhibitor Shared Configuration after ${Config.ensembleModifyRetries} retries. Using default.")
            trySaveSharedConfig(sharedConfig, retriesLeft)
          }
        case (None, Some(failureMessage)) =>
          if (retriesLeft > 0) {
            logger.debug(s"$failureMessage: retrying...")
            Thread.sleep(Config.ensembleModifyBackoff)
            tryAddToEnsemble(retriesLeft - 1)
          } else throw new IllegalStateException(failureMessage)
      }
    }

    def trySaveSharedConfig(sharedConfig: SharedConfig, retriesLeft: Int) {
      val updatedSharedConfig = server.config.sharedConfigOverride.foldLeft(sharedConfig) { case (conf, (key, value)) =>
        key match {
          case ConfigNames.ZOOKEEPER_INSTALL_DIRECTORY => conf.copy(zookeeperInstallDirectory = value)
          case ConfigNames.ZOOKEEPER_DATA_DIRECTORY => conf.copy(zookeeperDataDirectory = value)
          case ConfigNames.ZOOKEEPER_LOG_DIRECTORY => conf.copy(zookeeperLogDirectory = value)
          case ConfigNames.LOG_INDEX_DIRECTORY => conf.copy(logIndexDirectory = value)
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

      Try(ExhibitorAPIClient.setConfig(updatedSharedConfig.copy(serversSpec = updatedServersSpec), server.url)) match {
        case Success(_) => logger.info(s"Successfully added server ${server.id} to ensemble")
        case Failure(e) =>
          logger.debug(s"Failed to save Exhibitor Shared Configuration: ${e.getMessage}")
          if (retriesLeft > 0) {
            logger.debug("Retrying...")
            Thread.sleep(Config.ensembleModifyBackoff)
            trySaveSharedConfig(sharedConfig, retriesLeft - 1)
          } else throw e
      }
    }

    Future {
      ensembleLock.synchronized {
        logger.info(s"Adding server ${server.id} to ensemble")
        tryAddToEnsemble(Config.ensembleModifyRetries)
      }
    }
  }

  private def removeFromEnsemble(server: Exhibitor): Future[Unit] = {
    def tryRemoveFromEnsemble(aliveServer: Exhibitor, retriesLeft: Int) {
      getSharedConfig(server) match {
        case (Some(sharedConfig), None) =>
          trySaveSharedConfig(sharedConfig, aliveServer, retriesLeft)
        case (Some(sharedConfig), Some(failureMessage)) =>
          if (retriesLeft > 0) {
            logger.debug(s"$failureMessage: retrying...")
            Thread.sleep(Config.ensembleModifyBackoff)
            tryRemoveFromEnsemble(aliveServer, retriesLeft - 1)
          } else {
            logger.info(s"Failed to get non-default Exhibitor Shared Configuration after ${Config.ensembleModifyRetries} retries. Using default.")
            trySaveSharedConfig(sharedConfig, aliveServer, retriesLeft)
          }
        case (None, Some(failureMessage)) =>
          if (retriesLeft > 0) {
            logger.debug(s"$failureMessage: retrying...")
            Thread.sleep(Config.ensembleModifyBackoff)
            tryRemoveFromEnsemble(aliveServer, retriesLeft - 1)
          } else throw new IllegalStateException(failureMessage)
      }
    }

    def trySaveSharedConfig(sharedConfig: SharedConfig, aliveServer: Exhibitor, retriesLeft: Int) {
      val updatedServersSpec = sharedConfig.serversSpec.split(",").foldLeft(List[String]()) { (servers, srv) =>
        srv.split(":") match {
          case Array(_, _, serverHost) if serverHost == server.config.hostname => servers
          case Array(_, _, serverHost) => srv :: servers
          case _ => servers
        }
      }.sorted.mkString(",")

      Try(ExhibitorAPIClient.setConfig(sharedConfig.copy(serversSpec = updatedServersSpec), aliveServer.url)) match {
        case Success(_) => logger.info(s"Successfully removed server ${server.id} from ensemble")
        case Failure(e) =>
          logger.debug(s"Failed to save Exhibitor Shared Configuration: ${e.getMessage}")
          if (retriesLeft > 0) {
            logger.debug("Retrying...")
            Thread.sleep(Config.ensembleModifyBackoff)
            trySaveSharedConfig(sharedConfig, aliveServer, retriesLeft - 1)
          } else throw e
      }
    }

    Future {
      ensembleLock.synchronized {
        cluster.findWithState(Exhibitor.Running) match {
          case Some(aliveServer) => tryRemoveFromEnsemble(aliveServer, Config.ensembleModifyRetries)
          case None => logger.info(s"Server ${server.id} was the last alive in the cluster, no need to deregister it from ensemble.")
        }
      }
    }
  }

  private def getSharedConfig(server: Exhibitor): (Option[SharedConfig], Option[String]) = {
    Try(ExhibitorAPIClient.getSystemState(server.url)) match {
      case Success(cfg) =>
        if (cfg.zookeeperInstallDirectory != "") Some(cfg) -> None
        else Some(cfg) -> Some("Failed to get non-default Exhibitor Shared Configuration")
      case Failure(e) =>
        None -> Some("Exhibitor API not available.")
    }
  }

  private[exhibitor] def otherTasksAttributes(name: String): List[String] = {
    def value(server: Exhibitor, name: String): Option[String] = {
      if (name == "hostname") Option(server.config.hostname)
      else server.task.attributes.get(name)
    }

    cluster.runningServers().flatMap(value(_, name))
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

      cluster.servers().foreach(s => if (s.task == null && s.state == Exhibitor.Running) s.state = Exhibitor.Stopped)

      if (reconciles > RECONCILE_MAX_TRIES) {
        cluster.servers().filter(s => s.isReconciling && s.task != null).foreach { server =>
          logger.info(s"Reconciling exceeded $RECONCILE_MAX_TRIES tries for server ${server.id}, sending killTask for task ${server.task.id}")
          driver.killTask(TaskID.newBuilder().setValue(server.task.id).build())
        }
      } else {
        val statuses = cluster.runningServers().flatMap { server =>
          if (force || server.isReconciling) {
            server.state = Exhibitor.Reconciling
            logger.info(s"Reconciling $reconciles/$RECONCILE_MAX_TRIES state of server ${server.id}, task ${server.task.id}")
            Some(TaskStatus.newBuilder()
              .setTaskId(TaskID.newBuilder().setValue(server.task.id))
              .setState(TaskState.TASK_STAGING)
              .build)
          } else None
        }

        if (force || statuses.nonEmpty) driver.reconcileTasks(if (force) Collections.emptyList() else statuses)
      }
    }
  }

  /**
    * Get Exhibitor cluster view for each of the exhibitor-on-mesos servers
    */
  def getClusterStatus: ClusterStatus = {
    val mesosServerStatuses =
      cluster.servers().map { server =>
        val clusterViewOpt =
          server.state match {
            case Exhibitor.Running =>
              Try(ExhibitorAPIClient.getClusterStatus(server.url)) match {
                case Success(exhibitorClusterStateView) =>
                  Some(exhibitorClusterStateView)
                case Failure(e) =>
                  logger.error(s"Failed to get exhibitor cluster view for server ${server.id}", e)
                  None
              }
            case _ =>
              logger.debug(s"Server ${server.id} is in state ${server.id}, only RUNNING servers may request " +
                s"exhibitor API to get cluster state")
              None
          }
        ExhibitorMesosStatus(server, clusterViewOpt)
      }

    ClusterStatus(mesosServerStatuses)
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
    Logger.getLogger(ExhibitorAPIClient.getClass).setLevel(if (Config.debug) Level.DEBUG else Level.INFO)

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