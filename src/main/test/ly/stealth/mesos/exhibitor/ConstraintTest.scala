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

    Try(Constraint("unsupported")) match {
      case Failure(t) if t.isInstanceOf[IllegalArgumentException] => assertTrue("" + t, t.getMessage.contains("Unsupported condition"))
      case other => fail(other.toString)
    }
  }

  @Test
  def matches() {
    assertTrue(Constraint("like:abc").matches("abc"))
    assertFalse(Constraint("like:abc").matches("abc1"))

    assertTrue(Constraint("like:a.*").matches("abc"))
    assertFalse(Constraint("like:a.*").matches("bc"))

    assertTrue(Constraint("unique").matches("a"))
    assertFalse(Constraint("unique").matches("a", List("a")))
  }

  @Test
  def string() {
    assertEquals("like:abc", "" + Constraint("like:abc"))
    assertEquals("unlike:abc", "" + Constraint("unlike:abc"))
    assertEquals("unique", "" + Constraint("unique"))
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
}
