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

import java.io.{IOException, InputStream, OutputStream}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.mesos.Protos
import org.apache.mesos.Protos._

import scala.collection.JavaConversions._
import scala.collection.mutable

object Util {
  def parseList(s: String, entrySep: Char = ',', valueSep: Char = '=', nullValues: Boolean = true): List[(String, String)] = {
    def splitEscaped(s: String, sep: Char, unescape: Boolean = false): Array[String] = {
      val parts = new util.ArrayList[String]()

      var escaped = false
      var part = ""
      for (c <- s.toCharArray) {
        if (c == '\\' && !escaped) escaped = true
        else if (c == sep && !escaped) {
          parts.add(part)
          part = ""
        } else {
          if (escaped && !unescape) part += "\\"
          part += c
          escaped = false
        }
      }

      if (escaped) throw new IllegalArgumentException("open escaping")
      if (part != "") parts.add(part)

      parts.toArray(Array[String]())
    }

    val result = new mutable.ListBuffer[(String, String)]()
    if (s == null) return result.toList

    for (entry <- splitEscaped(s, entrySep)) {
      if (entry.trim.isEmpty) throw new IllegalArgumentException(s)

      val pair = splitEscaped(entry, valueSep, unescape = true)
      val key: String = pair(0).trim
      val value: String = if (pair.length > 1) pair(1).trim else null

      if (value == null && !nullValues) throw new IllegalArgumentException(s)
      result += key -> value
    }

    result.toList
  }

  def parseMap(s: String, entrySep: Char = ',', valueSep: Char = '=', nullValues: Boolean = true): Map[String, String] = parseList(s, entrySep, valueSep, nullValues).toMap

  def formatList(list: List[(String, _ <: Any)], entrySep: Char = ',', valueSep: Char = '='): String = {
    def escape(s: String): String = {
      var result = ""

      for (c <- s.toCharArray) {
        if (c == entrySep || c == valueSep || c == '\\') result += "\\"
        result += c
      }

      result
    }

    var s = ""
    list.foreach { tuple =>
      if (!s.isEmpty) s += entrySep
      s += escape(tuple._1)
      if (tuple._2 != null) s += valueSep + escape("" + tuple._2)
    }

    s
  }

  def formatMap(map: collection.Map[String, _ <: Any], entrySep: Char = ',', valueSep: Char = '='): String = formatList(map.toList, entrySep, valueSep)

  def formatConstraints(constraints: scala.collection.Map[String, List[Constraint]]): String = formatList(constraints.toList.flatMap { case (name, values) =>
    values.map(name -> _)
  })

  case class Range(start: Int, end: Int) {
    def overlap(r: Range): Range = {
      var x: Range = this
      var y: Range = r
      if (x.start > y.start) {
        val t = x
        x = y
        y = t
      }
      assert(x.start <= y.start)

      if (y.start > x.end) return null
      assert(y.start <= x.end)

      val start = y.start
      val end = Math.min(x.end, y.end)
      Range(start, end)
    }

    def values: List[Int] = (start to end).toList

    override def toString: String = if (start == end) s"$start" else s"$start..$end"
  }

  object Range {
    def apply(s: String): Range = parse(s)

    def apply(start: Int): Range = Range(start, start)

    private def parse(range: String): Range = {
      val idx = range.indexOf("..")

      if (idx == -1) {
        val value = range.toInt
        Range(value, value)
      } else {
        val start = range.substring(0, idx).toInt
        val end = range.substring(idx + 2).toInt
        if (start > end) throw new IllegalArgumentException("start > end")
        Range(start, end)
      }
    }

    def parseRanges(ranges: String): Array[Range] = ranges.split(",").map(parse)
  }

  object Str {
    def dateTime(date: Date): String = {
      new SimpleDateFormat("yyyy-MM-dd hh:mm:ssX").format(date)
    }

    def framework(framework: FrameworkInfo): String = {
      var s = ""

      s += id(framework.getId.getValue)
      s += " name: " + framework.getName
      s += " host: " + framework.getHostname
      s += " failover_timeout: " + framework.getFailoverTimeout

      s
    }

    def master(master: MasterInfo): String = {
      var s = ""

      s += id(master.getId)
      s += " pid:" + master.getPid
      s += " host:" + master.getHostname

      s
    }

    def slave(slave: SlaveInfo): String = {
      var s = ""

      s += id(slave.getId.getValue)
      s += " host:" + slave.getHostname
      s += " port:" + slave.getPort
      s += " " + resources(slave.getResourcesList)

      s
    }

    def offer(offer: Offer): String = {
      var s = ""

      s += offer.getHostname + id(offer.getId.getValue)
      s += " " + resources(offer.getResourcesList)
      s += " " + attributes(offer.getAttributesList)

      s
    }

    def offers(offers: Iterable[Offer]): String = {
      var s = ""

      for (offer <- offers)
        s += (if (s.isEmpty) "" else "\n") + Str.offer(offer)

      s
    }

    def task(task: TaskInfo): String = {
      var s = ""

      s += task.getTaskId.getValue
      s += " slave:" + id(task.getSlaveId.getValue)

      s += " " + resources(task.getResourcesList)
      s += " data:" + new String(task.getData.toByteArray)

      s
    }

    def resources(resources: util.List[Protos.Resource]): String = {
      var s = ""

      val order: util.List[String] = "cpus mem disk ports".split(" ").toList
      for (resource <- resources.sortBy(r => order.indexOf(r.getName))) {
        if (!s.isEmpty) s += " "
        s += resource.getName + ":"

        if (resource.hasScalar)
          s += "%.2f".format(resource.getScalar.getValue)

        if (resource.hasRanges)
          for (range <- resource.getRanges.getRangeList)
            s += "[" + range.getBegin + ".." + range.getEnd + "]"
      }

      s
    }

    def attributes(attributes: util.List[Protos.Attribute]): String = {
      var s = ""

      for (attr <- attributes) {
        if (!s.isEmpty) s += ";"
        s += attr.getName + ":"

        if (attr.hasText) s += attr.getText.getValue
        if (attr.hasScalar) s += "%.2f".format(attr.getScalar.getValue)
      }

      s
    }

    def taskStatus(status: TaskStatus): String = {
      var s = ""
      s += status.getTaskId.getValue
      s += " " + status.getState.name()

      s += " slave:" + id(status.getSlaveId.getValue)

      if (status.getState != TaskState.TASK_RUNNING)
        s += " reason:" + status.getReason.name()

      if (status.getMessage != null && status.getMessage != "")
        s += " message:" + status.getMessage

      s
    }

    def id(id: String): String = "#" + suffix(id, 5)

    def suffix(s: String, maxLen: Int): String = {
      if (s.length <= maxLen) return s
      s.substring(s.length - maxLen)
    }
  }

  def copyAndClose(in: InputStream, out: OutputStream): Unit = {
    val buffer = new Array[Byte](128 * 1024)
    var actuallyRead = 0

    try {
      while (actuallyRead != -1) {
        actuallyRead = in.read(buffer)
        if (actuallyRead != -1) out.write(buffer, 0, actuallyRead)
      }
    } finally {
      try {
        in.close()
      }
      catch {
        case ignore: IOException =>
      }

      try {
        out.close()
      }
      catch {
        case ignore: IOException =>
      }
    }
  }

  def getScalarResources(offer: Offer, name: String): Double = {
    offer.getResourcesList.foldLeft(0.0) { (all, current) =>
      if (current.getName == name) all + current.getScalar.getValue
      else all
    }
  }

  def getRangeResources(offer: Offer, name: String): List[Protos.Value.Range] = {
    offer.getResourcesList.foldLeft[List[Protos.Value.Range]](List()) { case (all, current) =>
      if (current.getName == name) all ++ current.getRanges.getRangeList
      else all
    }
  }

}
