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

import play.api.libs.functional.syntax._
import play.api.libs.json._

import java.util.Date

import net.elodina.mesos.util.Period

case class Stickiness(var period: Period = new Period("10m")) {

  @volatile var hostname: Option[String] = None
  @volatile var stopTime: Option[Date] = None

  def expires: Option[Date] =
    stopTime.map(stop => new Date(stop.getTime + period.ms))

  def registerStart(hostname: String): Unit = {
    this.hostname = Some(hostname)
    this.stopTime = None
  }

  def registerStop(now: Date = new Date()): Unit = {
    this.stopTime = Some(now)
  }

  def allowsHostname(hostname: String, now: Date = new Date()): Boolean = {
    this.hostname.isEmpty || stopTime.forall(stop => now.getTime - stop.getTime >= period.ms) || this.hostname.exists(_ == hostname)
  }
}

object Stickiness {

  implicit val writer = new Writes[Stickiness] {
    def writes(s: Stickiness): JsValue = {
      Json.obj(
        "period" -> s.period.toString,
        "stopTime" -> s.stopTime.map(Util.Str.simpleDateFormat.format),
        "hostname" -> s.hostname
      )
    }
  }

  implicit val reader = (
    (__ \ 'period).read[String] and
      (__ \ 'stopTime).readNullable[String] and
      (__ \ 'hostname).readNullable[String])((period, stopTime, hostName) => {
    val stickiness = new Stickiness(new Period(period))
    stickiness.stopTime = stopTime.map(s => Util.Str.simpleDateFormat.parse(s))
    stickiness.hostname = hostName

    stickiness
  })

}
