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

import java.net.URI

import scala.concurrent.duration._
import scala.language.postfixOps

object Config {
  var debug: Boolean = false

  var master: String = null
  var user: String = ""
  var principal: Option[String] = None
  var secret: Option[String] = None

  var api: String = null

  var storage: String = "file:exhibitor-mesos.json"

  var frameworkName: String = "exhibitor"
  var frameworkTimeout: Duration = 30 days
  var frameworkRole: String = "*"

  var ensembleModifyRetries: Int = 15
  var ensembleModifyBackoff: Long = 4000

  def httpServerPort: Int = {
    val port = new URI(api).getPort
    if (port == -1) 80 else port
  }
}

object ConfigNames {
  final val API_ENV = "EM_API"
  final val API = "api"
  final val MASTER = "master"
  final val USER = "user"
  final val PRINCIPAL = "principal"
  final val SECRET = "secret"
  final val FRAMEWORK_NAME = "framework-name"
  final val FRAMEWORK_TIMEOUT = "framework-timeout"
  final val FRAMEWORK_ROLE = "framework-role"
  final val STORAGE = "storage"
  final val DEBUG = "debug"
  final val ENSEMBLE_MODIFY_RETRIES = "ensemble-modify-retries"
  final val ENSEMBLE_MODIFY_BACKOFF = "ensemble-modify-backoff"

  final val ID = "id"
  final val CPU = "cpu"
  final val MEM = "mem"
  final val CONSTRAINTS = "constraints"
  final val PORT = "port"
  final val TIMEOUT = "timeout"
  final val SHARED_CONFIG_CHANGE_BACKOFF = "configchangebackoff"
  final val STICKINESS_PERIOD = "stickiness-period"

  final val SHARED_CONFIG_TYPE = "configtype"
  final val SHARED_CONFIG_CHECK_MS = "configcheckms"
  final val DEFAULT_SHARED_CONFIG = "defaultconfig"
  final val HEADING_TEXT = "headingtext"
  final val HOSTNAME = "hostname"
  final val JQUERY_STYLE = "jquerystyle"
  final val LOGLINES = "loglines"
  final val NODE_MODIFICATION = "nodemodification"
  final val PREFS_PATH = "prefspath"
  final val SERVO = "servo"

  final val S3_CREDENTIALS = "s3credentials"
  final val S3_REGION = "s3region"
  final val S3_CONFIG = "s3config"
  final val S3_CONFIG_PREFIX = "s3configprefix"

  final val ZK_CONFIG_CONNECT = "zkconfigconnect"
  final val ZK_CONFIG_EXHIBITOR_PATH = "zkconfigexhibitorpath"
  final val ZK_CONFIG_EXHIBITOR_PORT = "zkconfigexhibitorport"
  final val ZK_CONFIG_POLL_MS = "zkconfigpollms"
  final val ZK_CONFIG_RETRY = "zkconfigretry"
  final val ZK_CONFIG_ZPATH = "zkconfigzpath"

  final val FILE_SYSTEM_BACKUP = "filesystembackup"
  final val S3_BACKUP = "s3backup"

  final val ACL_ID = "aclid"
  final val ACL_PERMISSIONS = "aclperms"
  final val ACL_SCHEME = "aclscheme"

  final val LOG_INDEX_DIRECTORY = "log-index-directory"
  final val ZOOKEEPER_INSTALL_DIRECTORY = "zookeeper-install-directory"
  final val ZOOKEEPER_DATA_DIRECTORY = "zookeeper-data-directory"
  final val ZOOKEEPER_LOG_DIRECTORY = "zookeeper-log-directory"
  final val BACKUP_EXTRA = "backup-extra"
  final val ZOO_CFG_EXTRA = "zoo-cfg-extra"
  final val JAVA_ENVIRONMENT = "java-environment"
  final val LOG4J_PROPERTIES = "log4j-properties"
  final val CLIENT_PORT = "client-port"
  final val CONNECT_PORT = "connect-port"
  final val ELECTION_PORT = "election-port"
  final val CHECK_MS = "check-ms"
  final val CLEANUP_PERIOD_MS = "cleanup-period-ms"
  final val CLEANUP_MAX_FILES = "cleanup-max-files"
  final val BACKUP_MAX_STORE_MS = "backup-max-store-ms"
  final val BACKUP_PERIOD_MS = "backup-period-ms"
}