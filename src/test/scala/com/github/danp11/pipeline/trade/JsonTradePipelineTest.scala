package com.github.danp11.pipeline.trade

import com.github.danp11.pipeline.PipelineTestBase
import org.apache.spark.sql.DataFrame


class JsonTradePipelineTest extends PipelineTestBase {

  override val sut = new JsonTradePipeline(spark)

  override def verifyValidDF(validDF: DataFrame): Unit = {
    validDF.show(10)
  }

  override def verifyInvalidDF(invalidDF: DataFrame): Unit = {
    invalidDF.show(10)
  }
}
