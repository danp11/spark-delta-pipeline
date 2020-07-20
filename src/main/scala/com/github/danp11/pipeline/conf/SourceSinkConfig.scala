package com.github.danp11.pipeline.conf

object SourceSinkConfig extends MyAppConfig {

  val sourceRootPath: String = load().getString("sourceRootPath")
  val sinkRootPath: String = load().getString("sinkRootPath")

  val tradeSourcePath: String = sourceRootPath + "trade/"
  val tradeSinkPath: String = sinkRootPath + "trade/"

  val invalidRecordSinkPath: String = sinkRootPath + "invalidRecordSinkPath/"


}
