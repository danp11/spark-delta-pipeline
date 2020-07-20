package com.github.danp11.pipeline

import com.github.danp11.pipeline.conf.SourceSinkConfig
import org.apache.spark.sql.DataFrame
import com.github.danp11.pipeline.helper.TableHelper
import com.github.mrpowers.spark.daria.utils.NioUtils
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import io.delta.tables.DeltaTable
import org.scalatest.FunSpec

abstract class PipelineTestBase extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  val sut: Pipeline

  def verifyValidDF(validDF: DataFrame)
  def verifyInvalidDF(invalidDF: DataFrame)

  it("clean up") {
    val file = new java.io.File(SourceSinkConfig.sinkRootPath)
    if (file.exists) {
      NioUtils.removeUnder(file)
      NioUtils.removeAll(file)
    }
   // spark.sql(s"DROP TABLE IF EXISTS ${sut.tableName}")
  }

  it("create sink folders") {
    createFolder(SourceSinkConfig.invalidRecordSinkPath)
    createFolder(SourceSinkConfig.sinkRootPath + sut.tableName)
  }

  it("start processing") {
    sut.apply()
  }

  it("verify valid dataframe") {
   verifyValidDF(DeltaTable.forName(spark, sut.tableName).toDF)
  }

  it("verify invalid dataframe") {
    verifyInvalidDF(DeltaTable.forName(spark, InvalidRecord.tableName).toDF)
  }

  private def createFolder(path: String) {
    new java.io.File(path).mkdir()
  }

}
