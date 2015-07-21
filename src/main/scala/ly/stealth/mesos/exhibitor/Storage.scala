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

import java.io.{File, FileWriter}

import play.api.libs.json.{Json, Reads, Writes}

import scala.io.Source

trait Storage[T] {
  def save(value: T)(implicit writes: Writes[T])

  def load(implicit reads: Reads[T]): Option[T]
}

case class FileStorage[T](file: String) extends Storage[T] {
  override def save(value: T)(implicit writes: Writes[T]) {
    val writer = new FileWriter(file)
    try {
      writer.write(Json.stringify(Json.toJson(value)))
    } finally {
      writer.close()
    }
  }

  override def load(implicit reads: Reads[T]): Option[T] = {
    if (!new File(file).exists()) None
    else Json.parse(Source.fromFile(file).mkString).asOpt[T]
  }
}