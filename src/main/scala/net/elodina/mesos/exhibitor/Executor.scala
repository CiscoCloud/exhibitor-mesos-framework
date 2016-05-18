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

package net.elodina.mesos.exhibitor

import java.io.{PrintWriter, StringWriter}
import java.net.InetAddress

import com.google.protobuf.ByteString
import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{ExecutorDriver, MesosExecutorDriver}
import play.api.libs.json.Json

object Executor extends org.apache.mesos.Executor {
  private val logger = Logger.getLogger(Executor.getClass)
  private[exhibitor] var exhibitor: Server = new ExhibitorServer

  def main(args: Array[String]) {
    initLogging()

    val driver = new MesosExecutorDriver(Executor)
    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1

    sys.exit(status)
  }

  def registered(driver: ExecutorDriver, executor: ExecutorInfo, framework: FrameworkInfo, slave: SlaveInfo) {
    logger.info("[registered] framework:" + Util.Str.framework(framework) + " slave:" + Util.Str.slave(slave))
  }

  def reregistered(driver: ExecutorDriver, slave: SlaveInfo) {
    logger.info("[reregistered] " + Util.Str.slave(slave))
  }

  def disconnected(driver: ExecutorDriver) {
    logger.info("[disconnected]")
  }

  def launchTask(driver: ExecutorDriver, task: TaskInfo) {
    logger.info("[launchTask] " + Util.Str.task(task))

    new Thread {
      override def run() {
        setName("Exhibitor")

        try {
          exhibitor.start(Json.parse(task.getData.toStringUtf8).as[TaskConfig])
          driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId).setState(TaskState.TASK_RUNNING)
            .setData(ByteString.copyFromUtf8(InetAddress.getLocalHost.getHostName)).build)
          exhibitor.await()

          driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId).setState(TaskState.TASK_FINISHED).build)
        } catch {
          case t: Throwable =>
            logger.error("", t)
            sendTaskFailed(driver, task, t)
        } finally {
          stopExhibitor()
          driver.stop()
        }
      }
    }.start()
  }

  def killTask(driver: ExecutorDriver, id: TaskID) {
    logger.info("[killTask] " + id.getValue)
    stopExhibitor()
  }

  def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]) {
    logger.info("[frameworkMessage] " + new String(data))

    handleMessage(driver, new String(data))
  }

  def shutdown(driver: ExecutorDriver) {
    logger.info("[shutdown]")
    stopExhibitor()
  }

  def error(driver: ExecutorDriver, message: String) {
    logger.info("[error] " + message)
  }

  private def initLogging() {
    System.setProperty("log4j.ignoreTCL", "true") // fix  log4j class loading issue
    BasicConfigurator.resetConfiguration()

    val root = Logger.getRootLogger
    root.setLevel(Level.INFO)

    val logger = Logger.getLogger(Executor.getClass.getPackage.getName)
    logger.setLevel(if (System.getProperty("debug") != null) Level.DEBUG else Level.INFO)

    val layout = new PatternLayout("%d [%t] %-5p %c %x - %m%n")
    root.addAppender(new ConsoleAppender(layout))
  }

  private def sendTaskFailed(driver: ExecutorDriver, task: TaskInfo, t: Throwable) {
    val stackTrace = new StringWriter()
    t.printStackTrace(new PrintWriter(stackTrace, true))

    driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId).setState(TaskState.TASK_FAILED)
      .setMessage("" + stackTrace).build)
  }

  private[exhibitor] def stopExhibitor(async: Boolean = false) {
    def triggerStop() {
      if (exhibitor.isStarted) exhibitor.stop()
    }

    if (async) {
      new Thread() {
        override def run() {
          setName("ExecutorStopper")
          triggerStop()
        }
      }
    } else triggerStop()
  }

  private def handleMessage(driver: ExecutorDriver, message: String) {
    if (message == "fail") driver.stop()
  }
}
