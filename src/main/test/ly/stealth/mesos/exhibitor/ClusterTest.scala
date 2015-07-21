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

package ly.stealth.mesos.exhibitor

import org.junit.Assert._
import org.junit.Test
import play.api.libs.json.{Writes, Reads}

import scala.util.{Failure, Try}

class ClusterTest extends MesosTestCase {
  @Test
  def expandIds() {
    val cluster = Cluster()

    (0 until 5).foreach(i => cluster.addServer(ExhibitorServer("" + i)))

    Try(cluster.expandIds("")) match {
      case Failure(t) if t.isInstanceOf[IllegalArgumentException] =>
      case other => fail(other.toString)
    }

    assertEquals(List("0"), cluster.expandIds("0"))
    assertEquals(List("0", "2", "4"), cluster.expandIds("0,2,4"))

    assertEquals(List("1", "2", "3"), cluster.expandIds("1..3"))
    assertEquals(List("0", "1", "3", "4"), cluster.expandIds("0..1,3..4"))

    assertEquals(List("0", "1", "2", "3", "4"), cluster.expandIds("*"))

    // duplicates
    assertEquals(List("0", "1", "2", "3", "4"), cluster.expandIds("0..3,2..4"))

    // sorting
    assertEquals(List("2", "3", "4"), cluster.expandIds("4,3,2"))
  }

  @Test
  def loadSave() {
    val cluster = Cluster()
    cluster.frameworkId = Some("some id")
    cluster.save()

    val loaded = Cluster()
    loaded.load()

    assertEquals(cluster.frameworkId, loaded.frameworkId)
  }
}
