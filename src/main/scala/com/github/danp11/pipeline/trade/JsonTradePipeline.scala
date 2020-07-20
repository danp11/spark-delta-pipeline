package com.github.danp11.pipeline.trade

import com.github.danp11.pipeline.conf.SourceSinkConfig
import com.github.danp11.pipeline.helper.{ColumnHelper, TableHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class JsonTradePipeline(spark: SparkSession) extends TradePipeline {

  override val additionalDetailsExpr: Column = concat_ws(":", lit("Input File Name"), input_file_name())

  override lazy val tableSchema = new StructType(parsingSchema.fields ++ withAdditionalColumnsSchema.fields)

  private lazy val parsingSchema: StructType = StructType(
    Seq(
      StructField("trade_id", StringType, false),
      StructField("ts", LongType, false),
      StructField("ob_id", StringType, false),
      StructField("ask_member", StringType, false),
      StructField("bid_member", StringType, false),
      StructField("volume", ColumnHelper.decimalType, false),
      StructField("price", ColumnHelper.decimalType, false)
    ))

  private lazy val withAdditionalColumnsSchema: StructType = StructType(
    Seq(
      StructField("turnover", ColumnHelper.decimalType, false),
    ))

  override def parseRawData(rawData: String): Column = {
    from_json(col(rawData), parsingSchema)
  }

  override def withAdditionalColumns(source: DataFrame): DataFrame = {
    source.transform(ColumnHelper.withTurnover())
  }

  override def streamingDF(): DataFrame = {
    spark.readStream
      .format("text")
      .option("maxFilesPerTrigger", 1)
      .option("checkpointLocation", "./tmp/delta_lake_pipeline/entity_x/checkpoint/trade")
      .load(SourceSinkConfig.tradeSourcePath)
  }

  override def init(): Unit = {
    TableHelper.createTable(spark, tableName, tableSchema, partitionColumns, SourceSinkConfig.tradeSinkPath)
  }
}
