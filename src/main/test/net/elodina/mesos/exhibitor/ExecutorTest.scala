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

import java.util.UUID

import org.apache.mesos.Protos.{Status, TaskState}
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class ExecutorTest extends MesosTestCase {
  @Test(timeout = 5000)
  def startServerSuccess() {
    Executor.launchTask(executorDriver, task())
    executorDriver.waitForStatusUpdates(1)
    assertEquals(1, executorDriver.statusUpdates.size)

    val status = executorDriver.statusUpdates.last
    assertEquals(TaskState.TASK_RUNNING, status.getState)
    assertTrue(Executor.exhibitor.isStarted)

    Executor.exhibitor.stop()
    executorDriver.waitForStatusUpdates(2)

    assertEquals(2, executorDriver.statusUpdates.size)
    val finishedStatus = executorDriver.statusUpdates.last
    assertEquals(TaskState.TASK_FINISHED, finishedStatus.getState)
    assertFalse(Executor.exhibitor.isStarted)
  }

  @Test
  def startServerFailure() {
    Executor.exhibitor.asInstanceOf[TestExhibitor].failOnStart = true
    Executor.launchTask(executorDriver, task())

    executorDriver.waitForStatusUpdates(1)
    assertEquals(1, executorDriver.statusUpdates.size)

    val status = executorDriver.statusUpdates.last
    assertEquals(TaskState.TASK_FAILED, status.getState)
    assertFalse(Executor.exhibitor.isStarted)
  }

  @Test
  def stopExecutor() {
    Executor.exhibitor.start(TaskConfig(new mutable.HashMap[String, String](), new mutable.HashMap[String, String](), UUID.randomUUID().toString))
    assertTrue(Executor.exhibitor.isStarted)
    assertEquals(Status.DRIVER_RUNNING, executorDriver.status)

    Executor.stopExecutor()
    assertFalse(Executor.exhibitor.isStarted)
    //TODO
    //    assertEquals(Status.DRIVER_STOPPED, executorDriver.status)

    Executor.stopExecutor() // no error
    //    assertEquals(Status.DRIVER_STOPPED, executorDriver.status)
  }

  @Test(timeout = 5000)
  def launchTask() {
    Executor.launchTask(executorDriver, task())
    executorDriver.waitForStatusUpdates(1)
    assertTrue(Executor.exhibitor.isStarted)
  }

  @Test(timeout = 5000)
  def killTask() {
    Executor.exhibitor.start(TaskConfig(new mutable.HashMap[String, String](), new mutable.HashMap[String, String](), UUID.randomUUID().toString))
    Executor.killTask(executorDriver, taskId())

    Executor.exhibitor.await()
    assertFalse(Executor.exhibitor.isStarted)
  }

  @Test
  def shutdown() {
    Executor.exhibitor.start(TaskConfig(new mutable.HashMap[String, String](), new mutable.HashMap[String, String](), UUID.randomUUID().toString))
    Executor.shutdown(executorDriver)
    assertFalse(Executor.exhibitor.isStarted)
  }
}
