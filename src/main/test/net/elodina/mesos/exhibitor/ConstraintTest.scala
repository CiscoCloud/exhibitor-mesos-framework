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

import scala.util.{Failure, Try}

class ConstraintTest {
  @Test
  def parse() {
    val like = Constraint("like:1").asInstanceOf[Constraint.Like]
    assertEquals("1", like.regex)
    assertFalse(like.negated)

    val unlike = Constraint("unlike:1").asInstanceOf[Constraint.Like]
    assertEquals("1", unlike.regex)
    assertTrue(unlike.negated)

    val unique = Constraint("unique")
    assertTrue(unique.isInstanceOf[Constraint.Unique])

    val cluster = Constraint("cluster").asInstanceOf[Constraint.Cluster]
    assertEquals(None, cluster.value)

    val cluster123 = Constraint("cluster:123").asInstanceOf[Constraint.Cluster]
    assertEquals(Some("123"), cluster123.value)

    val groupBy = Constraint("groupBy").asInstanceOf[Constraint.GroupBy]
    assertEquals(1, groupBy.groups)

    val groupBy3 = Constraint("groupBy:3").asInstanceOf[Constraint.GroupBy]
    assertEquals(3, groupBy3.groups)

    Try(Constraint("unsupported")) match {
      case Failure(t) if t.isInstanceOf[IllegalArgumentException] => assertTrue("" + t, t.getMessage.contains("Unsupported condition"))
      case other => fail(other.toString)
    }

    val constraints = Constraint.parse("hostname=unique,hostname=like:slave.*")
    assertEquals(1, constraints.size)
    val hostnameConstraintsOpt = constraints.get("hostname")
    assertNotEquals(None, hostnameConstraintsOpt)
    val hostnameConstraints = hostnameConstraintsOpt.get
    assertEquals(2, hostnameConstraints.size)
  }

  @Test
  def matches() {
    assertTrue(Constraint("like:abc").matches("abc"))
    assertFalse(Constraint("like:abc").matches("abc1"))

    assertTrue(Constraint("like:a.*").matches("abc"))
    assertFalse(Constraint("like:a.*").matches("bc"))

    assertTrue(Constraint("unique").matches("a"))
    assertFalse(Constraint("unique").matches("a", List("a")))

    assertTrue(Constraint("cluster").matches("a"))
    assertFalse(Constraint("cluster").matches("b", List("a")))

    assertTrue(Constraint("groupBy").matches("a", List("a")))
    assertFalse(Constraint("groupBy").matches("a", List("b")))
  }

  @Test
  def string() {
    assertEquals("like:abc", "" + Constraint("like:abc"))
    assertEquals("unlike:abc", "" + Constraint("unlike:abc"))
    assertEquals("unique", "" + Constraint("unique"))
    assertEquals("cluster", "" + Constraint("cluster"))
    assertEquals("cluster:123", "" + Constraint("cluster:123"))
    assertEquals("groupBy", "" + Constraint("groupBy"))
    assertEquals("groupBy:3", "" + Constraint("groupBy:3"))
  }

  @Test
  def matchesLike() {
    val like = Constraint("like:1.*2")
    assertTrue(like.matches("12"))
    assertTrue(like.matches("1a2"))
    assertTrue(like.matches("1ab2"))

    assertFalse(like.matches("a1a2"))
    assertFalse(like.matches("1a2a"))

    val unlike = Constraint("unlike:1")
    assertFalse(unlike.matches("1"))
    assertTrue(unlike.matches("2"))
  }

  @Test
  def matchesUnique() {
    val unique = Constraint("unique")
    assertTrue(unique.matches("1"))
    assertTrue(unique.matches("2", List("1")))
    assertTrue(unique.matches("3", List("1", "2")))

    assertFalse(unique.matches("1", List("1", "2")))
    assertFalse(unique.matches("2", List("1", "2")))
  }

  @Test
  def matchesCluster() {
    val cluster = Constraint("cluster")
    assertTrue(cluster.matches("1"))
    assertTrue(cluster.matches("2"))

    assertTrue(cluster.matches("1", List("1")))
    assertTrue(cluster.matches("1", List("1", "1")))
    assertFalse(cluster.matches("2", List("1")))

    val cluster3 = Constraint("cluster:3")
    assertTrue(cluster3.matches("3"))
    assertFalse(cluster3.matches("2"))

    assertTrue(cluster3.matches("3", List("3")))
    assertTrue(cluster3.matches("3", List("3", "3")))
    assertFalse(cluster3.matches("2", List("3")))
  }

  @Test
  def matchesGroupBy() {
    val groupBy = Constraint("groupBy")
    assertTrue(groupBy.matches("1"))
    assertTrue(groupBy.matches("1", List("1")))
    assertTrue(groupBy.matches("1", List("1", "1")))
    assertFalse(groupBy.matches("1", List("2")))

    val groupBy2 = Constraint("groupBy:2")
    assertTrue(groupBy2.matches("1"))
    assertFalse(groupBy2.matches("1", List("1")))
    assertFalse(groupBy2.matches("1", List("1", "1")))
    assertTrue(groupBy2.matches("2", List("1")))

    assertTrue(groupBy2.matches("1", List("1", "2")))
    assertTrue(groupBy2.matches("2", List("1", "2")))

    assertFalse(groupBy2.matches("1", List("1", "1", "2")))
    assertTrue(groupBy2.matches("2", List("1", "1", "2")))
  }
}
