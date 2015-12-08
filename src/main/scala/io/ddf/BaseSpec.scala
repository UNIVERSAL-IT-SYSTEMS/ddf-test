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

import org.scalatest.FeatureSpec

trait BaseSpec extends FeatureSpec {
  val manager: DDFManager
  val engineName: String


  def loadMtCarsDDF(): DDF = {
    val ddfName: String = "mtcars"
    loadCSVIfNotExists(ddfName, s"/$ddfName",
      Seq(s"mpg ${floatingType(engineName.toLowerCase)}", "cyl int", s"disp ${floatingType(engineName.toLowerCase)}",
        "hp int", s"drat ${floatingType(engineName.toLowerCase)}", s"wt ${floatingType(engineName.toLowerCase)}",
        s"qsec ${floatingType(engineName.toLowerCase)}", "vs int", "am int", "gear int", "carb int"),
      delimiter = ' ')
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
        manager.sql(s"drop table if exists $ddfName cascade", engineName)
        manager.sql(s"create table $ddfName (${columns.mkString(",")})", engineName)
        // manager.sql(s"create table $ddfName (${columns.mkString(",")}) row format delimited fields terminated by ' " +s"'", engineName)

        val filePath = getClass.getResource(fileName).getPath
        val additionalOptions = if (!isNullSetToDefault) {
          "WITH NULL '' NO DEFAULTS"
        } else {
          s"DELIMITED BY '$delimiter'"
        }
        manager.sql(s"load '$filePath' $additionalOptions INTO $ddfName", engineName)
        // manager.sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE $ddfName", engineName)
        manager.getDDFByName(ddfName)
    }
  }

  val varcharType = Map("aws" -> "varchar", "jdbc" -> "varchar", "postgres" -> "varchar", "spark" -> "string", "flink" -> "string")
  val floatingType = Map("aws" -> "decimal", "jdbc" -> "double", "postgres" -> "decimal", "spark" -> "double", "flink" -> "double")

  private def airlineColumns = Seq("Year int", "Month int", "DayofMonth int", "DayofWeek int", "DepTime int",
    "CRSDepTime int", "ArrTime int", "CRSArrTime int", s"UniqueCarrier ${varcharType(engineName.toLowerCase)}",
    "FlightNum int", s"TailNum ${varcharType(engineName.toLowerCase)}", "ActualElapsedTime int", "CRSElapsedTime int",
    "AirTime int", "ArrDelay int", "DepDelay int", s"Origin ${varcharType(engineName.toLowerCase)}",
    s"Dest ${varcharType(engineName.toLowerCase)}", "Distance int", "TaxiIn int", "TaxiOut int", "Cancelled int",
    s"CancellationCode ${varcharType(engineName.toLowerCase)}", s"Diverted ${varcharType(engineName.toLowerCase)}",
    "CarrierDelay int", "WeatherDelay int", "NASDelay int", "SecurityDelay int", "LateAircraftDelay int")

  def loadIrisTest(): DDF = {
    val train = manager.getDDFByName("iris")
    //train.sql2ddf("SELECT petal, septal FROM iris WHERE flower = 1.0000")
    train.VIEWS.project("petal", "septal")
  }

  def loadAirlineDDF(): DDF = {
    val ddfName = "airline"
    loadCSVIfNotExists(ddfName, s"/$ddfName.csv", airlineColumns)
  }

  def loadAirlineDDFWithoutDefault(): DDF = {
    val ddfName = "airlineWithoutDefault"
    loadCSVIfNotExists(ddfName, "/airline.csv", airlineColumns, isNullSetToDefault = false)
  }

  def loadAirlineNADDF(): DDF = {
    val ddfName = "airlineWithNA"
    loadCSVIfNotExists(ddfName, s"/$ddfName.csv", airlineColumns, isNullSetToDefault = false)
  }


  def loadYearNamesDDF(): DDF = {
    val ddfName = "year_names"
    loadCSVIfNotExists(ddfName, s"/$ddfName.csv", Seq("Year_num int", s"Name ${varcharType(engineName.toLowerCase)}"))
  }

  def loadIrisTrain(): DDF = {
    loadCSVIfNotExists("iris", "/fisheriris.csv", Seq(s"flower ${floatingType(engineName.toLowerCase)}", "petal " +
      s"${floatingType(engineName.toLowerCase)}", s"septal ${floatingType(engineName.toLowerCase)}"))
  }

  def loadRegressionTrain(): DDF = {
    val ddfName: String = "regression_data"
    loadCSVIfNotExists(ddfName, "/regressionData.csv", Seq(s"col1 ${floatingType(engineName.toLowerCase)}", "col2 " +
      s"${floatingType(engineName.toLowerCase)}"))
  }

  def loadRegressionTest(): DDF = {
    val train = manager.getDDFByName("regression_data")
    train.VIEWS.project("col2")
  }

  def loadRatingsTrain(): DDF = {
    loadCSVIfNotExists("user_ratings", "/ratings.csv", Seq("user_id int", "item_id int",
      s"rating ${floatingType(engineName.toLowerCase)}"))
  }

  def loadRatingsTest(): DDF = {
    val train = manager.getDDFByName("user_ratings")
    train.VIEWS.project("user_id", "item_id")
  }


}
