package com.github.danp11.pipeline

import org.apache.spark.sql.DataFrame
import com.github.danp11.pipeline.helper.TableHelper
import com.github.danp11.pipeline.spark.SparkSessionTestWrapper
import com.github.mrpowers.spark.daria.utils.NioUtils
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import io.delta.tables.DeltaTable
import org.scalatest.FunSpec

abstract class PipelineTestBase extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  private val sinkPath = "./tmp/delta_lake_pipeline/"

  val entityPath: String

  val sut: Pipeline

  def verifyValidDF(validDF: DataFrame)
  def verifyInvalidDF(invalidDF: DataFrame)

  it("clean up") {
    val file = new java.io.File(sinkPath)
    if (file.exists) {
      NioUtils.removeUnder(file)
      NioUtils.removeAll(file)
    }
    spark.sql(s"DROP TABLE IF EXISTS ${sut.tableName}")
  }

  it("create folders") {
    createFolder(sinkPath)
    createFolder(sinkPath + entityPath + "invalid_record")
    createFolder(sinkPath + entityPath + sut.tableName)
  }

  it("create invalid records table") {
    createInvalidRecordTable
  }

  it("start processing") {
    sut.apply()
  }

  it("verify valid dataframe") {
   verifyValidDF(DeltaTable.forName(spark, sut.tableName).toDF)
  }

  it("verify invalid dataframe") {
    verifyInvalidDF(DeltaTable.forName(spark, TableHelper.invalidRecordsTableName).toDF)
  }

  private def createFolder(path: String) {
    new java.io.File(path).mkdir()
  }

  private def createInvalidRecordTable = {
    val invalidRecordsTable = TableHelper.invalidRecordsTableName
    val invalidRecordsLocation = "'" + sinkPath + entityPath +"invalid_record" + "'"

    spark.sql(s"DROP TABLE IF EXISTS $invalidRecordsTable")

    spark.sql(
      s"""CREATE TABLE $invalidRecordsTable (origin_value STRING NOT NULL)
         |  USING DELTA
         |  LOCATION $invalidRecordsLocation""".stripMargin)
  }

}
