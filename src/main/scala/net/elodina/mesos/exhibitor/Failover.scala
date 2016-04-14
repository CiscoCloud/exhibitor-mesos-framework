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

import java.util.Date

import play.api.libs.functional.syntax._
import play.api.libs.json._

import net.elodina.mesos.util.Period

case class Failover(var delay: Period = new Period("1m"), var maxDelay: Period = new Period("10m")) {
  var maxTries: Option[Int] = None

  @volatile var failures: Int = 0
  @volatile var failureTime: Option[Date] = None

  def currentDelay: Period = {
    if (failures == 0) return new Period("0")

    val multiplier = 1 << (failures - 1)
    val d = delay.ms * multiplier

    if (d > maxDelay.ms) maxDelay else new Period(delay.value * multiplier + delay.unit)
  }

  def delayExpires: Date = {
    if (failures == 0) new Date(0)
    else new Date(failureTime.get.getTime + currentDelay.ms)
  }

  def isWaitingDelay(now: Date = new Date()): Boolean = delayExpires.getTime > now.getTime

  def isMaxTriesExceeded: Boolean = {
    maxTries.exists(failures >= _)
  }

  def registerFailure(now: Date = new Date()): Unit = {
    failures += 1
    failureTime = Some(now)
  }

  def resetFailures(): Unit = {
    failures = 0
    failureTime = None
  }
}

object Failover {

  implicit val writer = new Writes[Failover] {
    def writes(f: Failover): JsValue = {
      Json.obj(
        "delay" -> f.delay.toString,
        "maxDelay" -> f.maxDelay.toString,
        "maxTries" -> f.maxTries,
        "failures" -> f.failures,
        "failureTime" -> f.failureTime.map(Util.Str.simpleDateFormat.format)
      )
    }
  }

  implicit val reader = (
    (__ \ 'delay).read[String] and
      (__ \ 'maxDelay).read[String] and
      (__ \ 'maxTries).readNullable[Int] and
      (__ \ 'failures).read[Int] and
      (__ \ 'failureTime).readNullable[String])((delay, maxDelay, maxTries, failures, failureTime) => {
    val failover = new Failover(new Period(delay), new Period(maxDelay))
    failover.maxTries = maxTries
    failover.failures = failures
    failover.failureTime = failureTime.map(Util.Str.simpleDateFormat.parse)

    failover
  })

}
