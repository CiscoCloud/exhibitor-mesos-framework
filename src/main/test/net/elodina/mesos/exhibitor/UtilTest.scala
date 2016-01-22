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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util

import net.elodina.mesos.exhibitor.Util.Range
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable
import scala.util.{Failure, Try}

class UtilTest {
  @Test
  def parseMap() {
    var map = Util.parseMap("a=1,b=2")
    assertEquals(2, map.size)
    assertEquals("1", map.getOrElse("a", ""))
    assertEquals("2", map.getOrElse("b", ""))

    // missing pair
    Try(Util.parseMap("a=1,,b=2")) match {
      case Failure(t) if t.isInstanceOf[IllegalArgumentException] =>
      case other => fail(other.toString)
    }

    // null value
    map = Util.parseMap("a=1,b,c=3")
    assertEquals(3, map.size)
    assertNull(map.getOrElse("b", ""))

    Try(Util.parseMap("a=1,b,c=3", nullValues = false)) match {
      case Failure(t) if t.isInstanceOf[IllegalArgumentException] =>
      case other => fail(other.toString)
    }

    // escaping
    map = Util.parseMap("a=\\,,b=\\=,c=\\\\")
    assertEquals(3, map.size)
    assertEquals(",", map.getOrElse("a", ""))
    assertEquals("=", map.getOrElse("b", ""))
    assertEquals("\\", map.getOrElse("c", ""))

    // open escaping
    Try(Util.parseMap("a=\\")) match {
      case Failure(t) if t.isInstanceOf[IllegalArgumentException] =>
      case other => fail(other.toString)
    }

    // null
    assertTrue(Util.parseMap(null).isEmpty)
  }

  @Test
  def formatMap() {
    val map = new mutable.LinkedHashMap[String, String]()
    map.put("a", "1")
    map.put("b", "2")
    assertEquals("a=1,b=2", Util.formatMap(map))

    // null value
    map.put("b", null)
    assertEquals("a=1,b", Util.formatMap(map))

    // escaping
    map.put("a", ",")
    map.put("b", "=")
    map.put("c", "\\")
    assertEquals("a=\\,,b=\\=,c=\\\\", Util.formatMap(map))
  }

  @Test
  def copyAndClose() {
    val data = new Array[Byte](16 * 1024)
    for (i <- 0 until data.length) data(i) = i.toByte

    var inClosed = false
    var outClosed = false

    val in = new ByteArrayInputStream(data) {
      override def close() {
        super.close()
        inClosed = true
      }
    }
    val out = new ByteArrayOutputStream() {
      override def close() {
        super.close()
        outClosed = true
      }
    }

    Util.copyAndClose(in, out)
    assertTrue(util.Arrays.equals(data, out.toByteArray))
    assertTrue(inClosed)
    assertTrue(outClosed)
  }

  // Range
  @Test
  def initRange() {
    Range("30")
    Range("30..31")
    Range(30)
    Range(30, 31)

    // empty
    Try(Range("")) match {
      case Failure(t) if t.isInstanceOf[IllegalArgumentException] =>
      case other => fail(other.toString)
    }

    // non int
    Try(Range("abc")) match {
      case Failure(t) if t.isInstanceOf[IllegalArgumentException] =>
      case other => fail(other.toString)
    }

    // non int first
    Try(Range("abc..30")) match {
      case Failure(t) if t.isInstanceOf[IllegalArgumentException] =>
      case other => fail(other.toString)
    }

    // non int second
    Try(Range("30..abc")) match {
      case Failure(t) if t.isInstanceOf[IllegalArgumentException] =>
      case other => fail(other.toString)
    }

    // inverted range
    Try(Range("10..0")) match {
      case Failure(t) if t.isInstanceOf[IllegalArgumentException] =>
      case other => fail(other.toString)
    }
  }

  @Test
  def startEndRange() {
    assertEquals(0, Range("0").start)
    assertEquals(0, Range("0..10").start)
    assertEquals(10, Range("0..10").end)
  }

  @Test
  def overlap() {
    // no overlap
    assertEquals(None, Range(0, 10).overlap(Range(20, 30)))
    assertEquals(None, Range(20, 30).overlap(Range(0, 10)))
    assertEquals(None, Range(0).overlap(Range(1)))

    // partial
    assertEquals(Some(Range(5, 10)), Range(0, 10).overlap(Range(5, 15)))
    assertEquals(Some(Range(5, 10)), Range(5, 15).overlap(Range(0, 10)))

    // includes
    assertEquals(Some(Range(2, 3)), Range(0, 10).overlap(Range(2, 3)))
    assertEquals(Some(Range(2, 3)), Range(2, 3).overlap(Range(0, 10)))
    assertEquals(Some(Range(5)), Range(0, 10).overlap(Range(5)))

    // last point
    assertEquals(Some(Range(0)), Range(0, 10).overlap(Range(0)))
    assertEquals(Some(Range(10)), Range(0, 10).overlap(Range(10)))
    assertEquals(Some(Range(0)), Range(0).overlap(Range(0)))
  }

  @Test
  def rangeValues() {
    assertEquals(List(3), Range(3).values)
    assertEquals(List(0, 1), Range(0, 1).values)
    assertEquals(List(0, 1, 2, 3, 4), Range(0, 4).values)
  }

  @Test
  def toStringRange() {
    assertEquals("0", "" + Range("0"))
    assertEquals("0..10", "" + Range("0..10"))
    assertEquals("0", "" + Range("0..0"))
  }
}
