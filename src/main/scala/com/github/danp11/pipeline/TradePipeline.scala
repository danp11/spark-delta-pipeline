package com.github.danp11.pipeline

import com.github.danp11.pipeline.helper.{ColumnHelper, TableHelper}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

abstract class TradePipeline extends Pipeline {

  override val tableName: String = "trade"

  val tableSchema: StructType

  val additionalDetailsExpr: Column

  override val partitionColumns = List("ob_id")

  override val validConditionExpr: Column = TableHelper.createValidConditionExpr(tableSchema)

  override val uniqueConditions = "x.trade_id = y.trade_id";

  override val requiredSchema: StructType = StructType(
    Seq(
      StructField("trade_id", StringType, false),
      StructField("ts", LongType, false),
      StructField("ob_id", StringType, false),
      StructField("ask_member", StringType, false),
      StructField("bid_member", StringType, false),
      StructField("volume", ColumnHelper.decimalType, false),
      StructField("price", ColumnHelper.decimalType, false)
    ))
}
