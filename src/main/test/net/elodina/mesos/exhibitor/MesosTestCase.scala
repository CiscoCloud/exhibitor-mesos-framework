/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.{File, FileWriter}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Collections, UUID}

import com.google.protobuf.ByteString
import org.apache.log4j.BasicConfigurator
import org.apache.mesos.Protos.Value.{Scalar, Text}
import org.apache.mesos.Protos._
import org.apache.mesos.{ExecutorDriver, SchedulerDriver}
import org.junit.{After, Before, Ignore}
import play.api.libs.json.Json

import scala.collection.JavaConversions._
import scala.collection.mutable

@Ignore
class MesosTestCase {
  var schedulerDriver: TestSchedulerDriver = null
  var executorDriver: TestExecutorDriver = null

  @Before
  def before() {
    BasicConfigurator.configure()

    val storageFile = File.createTempFile(getClass.getSimpleName, null)
    storageFile.delete()
    Config.storage = s"file:${storageFile.getAbsolutePath}"

    Config.api = "http://localhost:7000"
    Scheduler.cluster.clear()

    schedulerDriver = newSchedulerDriver
    Scheduler.registered(schedulerDriver, frameworkId(), master())

    executorDriver = newExecutorDriver
    Executor.exhibitor = new TestExhibitor()

    def createTempFile(name: String, content: String): File = {
      val file = File.createTempFile(getClass.getSimpleName, name)

      val writer = new FileWriter(file)
      try {
        writer.write(content)
      }
      finally {
        writer.close()
      }

      file.deleteOnExit()
      file
    }

    HttpServer.jar = createTempFile("executor.jar", "executor")
    HttpServer.exhibitorDist = createTempFile("exhibitor-1.5.5.jar", "exhibitor")
    HttpServer.zookeeperDist = createTempFile("zookeeper-3.4.6.tar.gz", "zookeeper")
    HttpServer.jdkDist = createTempFile("jdk-8u45-linux-x64.tar.gz", "java")
  }

  @After
  def after() {
    Scheduler.disconnected(schedulerDriver)

    Executor.exhibitor.stop()
    Executor.exhibitor = new Exhibitor
    BasicConfigurator.resetConfiguration()
  }

  val LOCALHOST_IP: Int = 2130706433

  def frameworkId(id: String = "" + UUID.randomUUID()): FrameworkID = FrameworkID.newBuilder().setValue(id).build()

  def taskId(id: String = "" + UUID.randomUUID()): TaskID = TaskID.newBuilder().setValue(id).build()

  def master(
              id: String = "" + UUID.randomUUID(),
              ip: Int = LOCALHOST_IP,
              port: Int = 5050,
              hostname: String = "master"
              ): MasterInfo = {
    MasterInfo.newBuilder()
      .setId(id)
      .setIp(ip)
      .setPort(port)
      .setHostname(hostname)
      .build()
  }

  def offer(
             id: String = "" + UUID.randomUUID(),
             frameworkId: String = "" + UUID.randomUUID(),
             slaveId: String = "" + UUID.randomUUID(),
             hostname: String = "host",
             cpus: Double = 0,
             mem: Long = 0,
             ports: String = null,
             attributes: String = null
             ): Offer = {
    val builder = Offer.newBuilder()
      .setId(OfferID.newBuilder().setValue(id))
      .setFrameworkId(FrameworkID.newBuilder().setValue(frameworkId))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId))

    builder.setHostname(hostname)

    val cpusResource = Resource.newBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(cpus))
      .build
    builder.addResources(cpusResource)

    val memResource = Resource.newBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(0.0 + mem))
      .build
    builder.addResources(memResource)

    def ranges(s: String): util.List[Value.Range] = {
      if (s.isEmpty) return Collections.emptyList()

      s.split(",").toList
        .map(s => Util.Range(s.trim))
        .map(r => Value.Range.newBuilder().setBegin(r.start).setEnd(r.end).build())
    }

    val portsResource = Resource.newBuilder()
      .setName("ports")
      .setType(Value.Type.RANGES)
      .setRanges(Value.Ranges.newBuilder().addAllRange(ranges(if (ports != null) ports else "31000..32000")))
      .build

    builder.addResources(portsResource)

    if (attributes != null) {
      val map = Util.parseMap(attributes)
      for ((k, v) <- map) {
        val attribute = Attribute.newBuilder()
          .setType(Value.Type.TEXT)
          .setName(k)
          .setText(Text.newBuilder().setValue(v))
          .build
        builder.addAttributes(attribute)
      }
    }

    builder.build()
  }

  def task(
            id: String = "" + UUID.randomUUID(),
            name: String = "Task",
            slaveId: String = "" + UUID.randomUUID(),
            data: String = Json.stringify(Json.toJson(TaskConfig(new mutable.HashMap[String, String](), new mutable.HashMap[String, String](), UUID.randomUUID().toString)))
            ): TaskInfo = {
    val builder = TaskInfo.newBuilder()
      .setName(id)
      .setTaskId(TaskID.newBuilder().setValue(id))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId))

    if (data != null) builder.setData(ByteString.copyFromUtf8(data))

    builder.build()
  }

  def taskStatus(
                  id: String = "" + UUID.randomUUID(),
                  state: TaskState
                  ): TaskStatus = {
    TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue(id))
      .setState(state)
      .build()
  }

  private def newSchedulerDriver: TestSchedulerDriver = new TestSchedulerDriver()

  private def newExecutorDriver: TestExecutorDriver = new TestExecutorDriver()

  class TestSchedulerDriver extends SchedulerDriver {
    var status: Status = Status.DRIVER_RUNNING

    val declinedOffers: util.List[String] = new util.ArrayList[String]()
    val acceptedOffers: util.List[String] = new util.ArrayList[String]()

    val launchedTasks: util.List[TaskInfo] = new util.ArrayList[TaskInfo]()
    val killedTasks: util.List[String] = new util.ArrayList[String]()
    val reconciledTasks: util.List[String] = new util.ArrayList[String]()

    def declineOffer(id: OfferID): Status = {
      declinedOffers.add(id.getValue)
      status
    }

    def declineOffer(id: OfferID, filters: Filters): Status = {
      declinedOffers.add(id.getValue)
      status
    }

    def launchTasks(offerId: OfferID, tasks: util.Collection[TaskInfo]): Status = {
      acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def launchTasks(offerId: OfferID, tasks: util.Collection[TaskInfo], filters: Filters): Status = {
      acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def launchTasks(offerIds: util.Collection[OfferID], tasks: util.Collection[TaskInfo]): Status = {
      for (offerId <- offerIds) acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def launchTasks(offerIds: util.Collection[OfferID], tasks: util.Collection[TaskInfo], filters: Filters): Status = {
      for (offerId <- offerIds) acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def stop(): Status = throw new UnsupportedOperationException

    def stop(failover: Boolean): Status = throw new UnsupportedOperationException

    def killTask(id: TaskID): Status = {
      killedTasks.add(id.getValue)
      status
    }

    def requestResources(requests: util.Collection[Request]): Status = throw new UnsupportedOperationException

    def sendFrameworkMessage(executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]): Status = throw new UnsupportedOperationException

    def join(): Status = throw new UnsupportedOperationException

    def reconcileTasks(statuses: util.Collection[TaskStatus]): Status = {
      reconciledTasks.addAll(statuses.map(_.getTaskId.getValue))
      status
    }

    def reviveOffers(): Status = throw new UnsupportedOperationException

    def run(): Status = throw new UnsupportedOperationException

    def abort(): Status = throw new UnsupportedOperationException

    def start(): Status = throw new UnsupportedOperationException
  }

  class TestExecutorDriver extends ExecutorDriver {
    var status: Status = Status.DRIVER_RUNNING

    private val _statusUpdates: util.List[TaskStatus] = new util.concurrent.CopyOnWriteArrayList[TaskStatus]()

    def statusUpdates: List[TaskStatus] = _statusUpdates.toList

    def start(): Status = {
      status = Status.DRIVER_RUNNING
      status
    }

    def stop(): Status = {
      status = Status.DRIVER_STOPPED
      status
    }

    def abort(): Status = {
      status = Status.DRIVER_ABORTED
      status
    }

    def join(): Status = {
      status
    }

    def run(): Status = {
      status = Status.DRIVER_RUNNING
      status
    }

    def sendStatusUpdate(status: TaskStatus): Status = {
      _statusUpdates.synchronized {
        _statusUpdates.add(status)
        _statusUpdates.notify()
      }

      this.status
    }

    def waitForStatusUpdates(count: Int): Unit = {
      _statusUpdates.synchronized {
        while (_statusUpdates.size() < count)
          _statusUpdates.wait()
      }
    }

    def sendFrameworkMessage(message: Array[Byte]): Status = throw new UnsupportedOperationException
  }

}

class TestExhibitor extends IExhibitor {
  var failOnStart: Boolean = false
  private val started: AtomicBoolean = new AtomicBoolean(false)
  var config: TaskConfig = null

  def isStarted: Boolean = started.get()

  def start(config: TaskConfig) {
    if (failOnStart) throw new RuntimeException("failOnStart")
    started.set(true)
    this.config = config
  }

  def stop() {
    started.set(false)
    started.synchronized {
      started.notify()
    }
  }

  def await() {
    started.synchronized {
      while (started.get)
        started.wait()
    }
  }
}
