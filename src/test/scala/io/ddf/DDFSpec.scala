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
package io.ddf

import io.ddf.datasource.{JDBCDataSourceDescriptor, JDBCDataSourceCredentials, DataSourceURI}
import io.ddf.misc.Config
import io.ddf.util.ConfigHandler
import org.scalatest.BeforeAndAfterAll

class DDFSpec extends BaseSpec with StatisticsSpec with BinningSpec with AggregationSpec with
JoinSpec with MissingDataSpec with PersistenceSpec with SchemaSpec with SqlSpec
with TransformationSpec with ViewSpec with BeforeAndAfterAll {

  //override val engineName = scala.util.Properties.envOrElse("DDF_ENGINE", "")
  override val engineName = new ConfigHandler("ddf-conf","ddf_spec.ini").getValue("global","engine")

  override val configHandler = new ConfigHandler("ddf-conf","ddf_spec.ini")

  override def beforeAll() = {
//
  }

  override def afterAll(): Unit = {
    //manager.sql2ddf("DELETE * from ", engineName)
  }

  object EngineDescriptor {
    def apply(engine: String) = {
      val USER = "jdbcUser"
      val PASSWORD = "jdbcPassword"
      val URL = "jdbcUrl"
      val user = Config.getValue(engine, USER)
      val password = Config.getValue(engine, PASSWORD)
      val jdbcUrl = Config.getValue(engine, URL)
      val dataSourceURI = new DataSourceURI(jdbcUrl)
      val credentials = new JDBCDataSourceCredentials(user, password)
      new JDBCDataSourceDescriptor(dataSourceURI, credentials, null)
    }
  }

  override val manager = {
    if (engineName == "aws" || engineName == "jdbc" || engineName == "postgres")
      DDFManager.get(DDFManager.EngineType.fromString(engineName), EngineDescriptor(engineName))
    else
      DDFManager.get(DDFManager.EngineType.fromString(engineName))
  }

  def runMultiple(names: String) = {
    names.split(",").foreach(name => this.execute(name))
  }

  feature("copy" ) {
    //This feature is unsupported for ddf-on-jdbc

    scenario("factor columns should be copied") {
      val ddf1 = loadMtCarsDDF()
      Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
        col => ddf1.getSchemaHandler.setAsFactor(col)
      }

      val ddf2 = ddf1.copy()
      Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
        col =>
          assert(ddf2.getSchemaHandler.getColumn(col).getOptionalFactor != null)
      }
    }
    scenario("all rows are copied") {
      val ddf1 = loadMtCarsDDF()
      Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
        col => ddf1.getSchemaHandler.setAsFactor(col)
      }

      val ddf2 = ddf1.copy()
      assert(ddf1.getNumRows == ddf2.getNumRows)
    }

    scenario("all columns are copied") {
      val ddf1 = loadMtCarsDDF()
      Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
        col => ddf1.getSchemaHandler.setAsFactor(col)
      }

      val ddf2 = ddf1.copy()
      assert(ddf1.getNumColumns == ddf2.getNumColumns)
    }
    scenario("name is not copied") {
      val ddf1 = loadMtCarsDDF()
      Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
        col => ddf1.getSchemaHandler.setAsFactor(col)
      }

      val ddf2 = ddf1.copy()
      assert(ddf1.getName != ddf2.getName)
    }
  }
}