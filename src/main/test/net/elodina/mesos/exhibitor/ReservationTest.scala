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

import org.junit.Assert._
import org.junit.Test

class ReservationTest extends MesosTestCase {
  @Test
  def getPreferredSharedPort() {
    assertEquals(4000, Reservation.getPreferredSharedPort(4000, None))
    assertEquals(4000, Reservation.getPreferredSharedPort(4000, Some("4001")))
    assertEquals(4001, Reservation.getPreferredSharedPort(-1, Some("4001")))
    assertEquals(-1, Reservation.getPreferredSharedPort(-1, None))
  }

  @Test
  def reservePort() {
    assertEquals(Some(4000), Reservation.reservePort(List(Util.Range(4000, 7000)), -1, Nil))
    assertEquals(Some(4001), Reservation.reservePort(List(Util.Range(4000, 7000)), -1, List(4000)))

    assertEquals(Some(6322), Reservation.reservePort(List(Util.Range(4000, 7000)), 6322, Nil))
    assertEquals(Some(6322), Reservation.reservePort(List(Util.Range(4000, 7000)), 6322, List(4000, 4001)))
    assertEquals(None, Reservation.reservePort(List(Util.Range(4000, 7000)), 6322, List(6322)))
    assertEquals(None, Reservation.reservePort(List(Util.Range(4000, 7000)), 8000, Nil))

    assertEquals(None, Reservation.reservePort(List(Util.Range(4000, 4001)), -1, List(4000, 4001)))
  }

  @Test
  def reserve() {
    val server = Exhibitor("0")
    server.config.sharedConfigOverride += ConfigNames.CLIENT_PORT -> "31000"
    server.config.sharedConfigOverride += ConfigNames.CONNECT_PORT -> "31001"
    server.config.sharedConfigOverride += ConfigNames.ELECTION_PORT -> "31002"
    server.config.ports = List(Util.Range(31000, 32000))

    assertEquals(Left("cpus 0.1 < 0.2"), Reservation.reserve(offer(cpus = 0.1, mem = 1024), Ensemble("test"), server))
    assertEquals(Left("mem 64.0 < 256.0"), Reservation.reserve(offer(cpus = 0.5, mem = 64), Ensemble("test"), server))
    assertEquals(Left("No suitable client port"), Reservation.reserve(offer(cpus = 0.5, mem = 1024, ports = "31001..32000"), Ensemble("test"), server))
    assertEquals(Left("No suitable connect port"), Reservation.reserve(offer(cpus = 0.5, mem = 1024, ports = "31000,31002..32000"), Ensemble("test"), server))
    assertEquals(Left("No suitable election port"), Reservation.reserve(offer(cpus = 0.5, mem = 1024, ports = "31000..31001,31003..32000"), Ensemble("test"), server))
    assertEquals(Left("No suitable UI port"), Reservation.reserve(offer(cpus = 0.5, mem = 1024, ports = "4000..7000,31000..31002"), Ensemble("test"), server))
    assertEquals(Right(Reservation(0.2, 256, 31003, 31001, 31000, 31002)), Reservation.reserve(offer(cpus = 0.5, mem = 1024, ports = "31000..32000"), Ensemble("test"), server))
  }
}
