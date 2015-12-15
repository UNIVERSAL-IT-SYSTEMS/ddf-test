/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.ddf.ddfTest

import io.ddf.{DDFManager, DDF}
//import io.ddf.aws.ExtJDBCDataSourceDescriptor
import io.ddf.datasource.{JDBCDataSourceDescriptor, JDBCDataSourceCredentials, DataSourceURI}
import io.ddf.misc.Config
import io.ddf.util.ConfigHandler
import com.google.common.base.Strings

import org.scalatest.{FeatureSpec}

object BaseSpecAWS extends FeatureSpec {

  val configHandler: ConfigHandler = new ConfigHandler("ddf-conf", "ddf_spec.ini")

  val engineName: String = configHandler.getValue("global", "engine")

  def loadMtCarsDDF(): DDF = {
    val ddfName: String = "mtcars"
    loadCSVIfNotExists(ddfName, s"/$ddfName")
  }

  def manager: DDFManager = {
    if (engineName == "aws" || engineName == "jdbc" || engineName == "postgres")
      DDFManager.get(DDFManager.EngineType.fromString(engineName), EngineDescriptor(engineName))
    else
      DDFManager.get(DDFManager.EngineType.fromString(engineName))
  }

  def EngineDescriptor(engine: String) = {
    val USER = "jdbcUser"
    val PASSWORD = "jdbcPassword"
    val URL = "jdbcUrl"
    val user = Config.getValue(engine, USER)
    val password = Config.getValue(engine, PASSWORD)
    val jdbcUrl = Config.getValue(engine, URL)
    val dataSourceURI = new DataSourceURI(jdbcUrl)
    val credentials = new JDBCDataSourceCredentials(user, password)
    val engineDescriptor = new JDBCDataSourceDescriptor(dataSourceURI, credentials, null)
    /*if (engine == "aws")
      new ExtJDBCDataSourceDescriptor(engineDescriptor.getDataSourceUri, engineDescriptor.getCredentials, new java.util.HashMap())
    else*/
      engineDescriptor
  }

  private def loadCSVIfNotExists(ddfName: String,
                                 fileName: String): DDF = {
    try {
      manager.sql2ddf(s"SELECT * FROM $ddfName", engineName)
    } catch {
      case e: Exception =>
        if (engineName != "flink")
          manager.sql(getValue("drop-" + ddfName), engineName)

        manager.sql(getValue("create-" + ddfName), engineName)

        val filePath = getClass.getResource(fileName).getPath

        manager.sql(getValue("load-" + ddfName).replace("$filePath", s"$filePath"), engineName)

        manager.sql2ddf(s"SELECT * FROM $ddfName", engineName)
    }
  }

  private def getValue(key: String) = {
    val value = configHandler.getValue(engineName, key)
    if (Strings.isNullOrEmpty(value)) configHandler.getValue("global", key)
    else value
  }

  def loadAirlineDDF(): DDF = {
    val ddfName = "airline"
    loadCSVIfNotExists(ddfName, s"/$ddfName.csv")
  }

  def loadAirlineDDFWithoutDefault(): DDF = {
    val ddfName = "airlineWithoutDefault"
    loadCSVIfNotExists(ddfName, "/airline.csv")
  }

  def loadAirlineNADDF(): DDF = {
    val ddfName = "airlineWithNA"
    loadCSVIfNotExists(ddfName, s"/$ddfName.csv")
  }


  def loadYearNamesDDF(): DDF = {
    val ddfName = "year_names"
    loadCSVIfNotExists(ddfName, s"/$ddfName.csv")
  }


}

