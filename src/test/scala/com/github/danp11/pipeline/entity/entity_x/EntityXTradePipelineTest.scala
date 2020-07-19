package com.github.danp11.pipeline.entity.entity_x

import com.github.danp11.pipeline.PipelineTestBase
import org.apache.spark.sql.DataFrame


class EntityXTradePipelineTest extends PipelineTestBase {

  override val entityPath = "entity_x/"

  override val sut = new EntityXTradePipeline(spark)

  override def verifyValidDF(validDF: DataFrame): Unit = {
    validDF.show(10)
  }

  override def verifyInvalidDF(invalidDF: DataFrame): Unit = {
    invalidDF.show(10)
  }
}
