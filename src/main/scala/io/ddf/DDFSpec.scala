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

class DDFSpec(engineName: String) extends BaseSpec with StatisticsSpec with BinningSpec {

  override val manager = DDFManager.get(DDFManager.EngineType.fromString(engineName))

  def runMultiple(names: String) = {
    names.split(",").foreach(name => this.execute(name))
  }

  private def loadCSVIfNotExists(ddfName: String,
                                 fileName: String,
                                 columns: Seq[String],
                                 delimiter: Char = ',',
                                 isNullSetToDefault: Boolean = true): DDF = {
    try {
      manager.getDDFByName(ddfName)
    } catch {
      case e: Exception =>
//        manager.sql(s"create table $ddfName (${columns.mkString(",")})", engineName)
        manager.sql(s"create table $ddfName (${columns.mkString(",")}) row format delimited fields terminated by ' '", engineName)
        val filePath = getClass.getResource(fileName).getPath
        /*val additionalOptions = if (!isNullSetToDefault) {
          "WITH NULL '' NO DEFAULTS"
        } else {
          s"DELIMITED BY '$delimiter'"
        }*/
//        manager.sql(s"load '$filePath' $additionalOptions INTO $ddfName", engineName)
        manager.sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE $ddfName", engineName)
        manager.getDDFByName(ddfName)
    }
  }

  def loadMtCarsDDF(): DDF = {
    val ddfName: String = "mtcars"
    loadCSVIfNotExists(ddfName, s"/$ddfName",
      Seq("mpg double", "cyl int", "disp double", "hp int", "drat double", "wt double",
        "qsec double", "vs int", "am int", "gear int", "carb int"),
      delimiter = ' ')
  }

  feature("copy") {
    val ddf1 = loadMtCarsDDF()
    Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
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
  }
}