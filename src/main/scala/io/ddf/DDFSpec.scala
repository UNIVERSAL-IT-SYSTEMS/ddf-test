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

class DDFSpec(engine: String) extends BaseSpec with StatisticsSpec with BinningSpec with AggregationSpec with
JoinSpec with MissingDataSpec with PersistenceSpec with SchemaSpec with SqlSpec
with TransformationSpec with ViewSpec {

  override val engineName = engine

  object EngineDescriptor {
    def apply(engine: String) = {
      val user = Config.getValue(engine, "jdbcUser")
      val password = Config.getValue(engine, "jdbcPassword")
      val jdbcUrl = Config.getValue(engine, "jdbcUrl")
      val dataSourceURI = new DataSourceURI(jdbcUrl)
      val credentials = new JDBCDataSourceCredentials(user, password)
      new JDBCDataSourceDescriptor(dataSourceURI, credentials, null)
    }
  }

  override val manager = DDFManager.get(DDFManager.EngineType.fromString(engineName), EngineDescriptor("postgres"))

  def runMultiple(names: String) = {
    names.split(",").foreach(name => this.execute(name))
  }

  /*
  feature("as") {
    scenario("asd") {
      val ddf = loadMtCarsDDF()

      val schemaHandler = ddf.getSchemaHandler
      println(ddf.getNumColumns+"sdf"+ddf.getNumRows+"ll"+ddf.getColumnNames)
      Array(7, 8, 9, 10).foreach {
        idx => schemaHandler.setAsFactor(idx)
      }
      schemaHandler.computeFactorLevelsAndLevelCounts()
      val cols = Array(7, 8, 9, 10).map {
        idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
      }
      println("", cols.mkString(","))
      assert(cols(0).getOptionalFactor.getLevelCounts.get("1") === 14)
      assert(cols(0).getOptionalFactor.getLevelCounts.get("0") === 18)
      assert(cols(1).getOptionalFactor.getLevelCounts.get("1") === 13)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("4") === 12)

      assert(cols(2).getOptionalFactor.getLevelCounts.get("3") === 15)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("5") === 5)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("1") === 7)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("2") === 10)
    }
  }*/


  /*feature("copy") {
    val ddf1 = loadMtCarsDDF()
    Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
          println(ddf1.getSchemaHandler+"ddd")
      col => ddf1.getSchemaHandler.setAsFactor(col)
    }

    val ddf2 = ddf1.copy()
    scenario("factor columns should be copied") {

      Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
        col =>
          assert(ddf2.getSchemaHandler.getColumn(col).getOptionalFactor != null)
      }
    }
    scenario("all rows are copied") {
      assert(ddf1.getNumRows == ddf2.getNumRows)
    }

    scenario("all columns are copied") {
      assert(ddf1.getNumColumns == ddf2.getNumColumns)
    }
    scenario("name is not copied") {
      assert(ddf1.getName != ddf2.getName)
    }
  }*/
}