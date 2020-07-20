package com.github.danp11.pipeline

import com.github.danp11.pipeline.conf.SourceSinkConfig
import com.github.danp11.pipeline.helper.TableHelper
import com.github.mrpowers.spark.daria.sql.DariaValidator
import com.github.mrpowers.spark.daria.sql.DataFrameExt.DataFrameMethods
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row}

trait Pipeline extends SparkSessionWrapper {

  private val parsingValidCol = col("parse_details.status") === "OK"
  private val parsingInvalidCol = col("parse_details.status") === "NOT_VALID"

  val tableSchema: StructType
  val requiredSchema : StructType

  val tableName: String
  val partitionColumns: List[String]
  val uniqueConditions: String

  val validConditionExpr: Column
  val additionalDetailsExpr: Column

  def streamingDF(): DataFrame

  def prepare(source: DataFrame): DataFrame = source

  def parseRawData(rawData: String): Column

  def withAdditionalColumns(source: DataFrame): DataFrame = source

  def createInvalidRecordTable() = {
    TableHelper.createTable(spark, InvalidRecord.tableName, InvalidRecord.schema, InvalidRecord.partitionColumns, SourceSinkConfig.invalidRecordSinkPath)
  }

  def init():Unit

  def validateDF():Unit = {
    val actualDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], tableSchema)
    DariaValidator.validateSchema(actualDF, requiredSchema)
  }

  final def apply() {
    createInvalidRecordTable()
    init()
    validateDF()

    streamingDF()
      .writeStream
      .outputMode(OutputMode.Append())
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        runPipeline(batchDF, batchId)
      }.start()
      .awaitTermination(60000) //DPDPDP ingen timeout
  }

  private def runPipeline(source: DataFrame, batchId: Long): Unit = {
    source.persist()

    source
      .transform(prepare)
      .select(struct("*") as 'origin)
      .transform(parse)
      .transform(withAdditionalColumns)
      .transform(withParseDetails)
      .transform(insertIntoDelta(batchId))

    compressFiles()

    source.unpersist()
  }

  private def parse(source: DataFrame): DataFrame = {
    source
      .withColumn(
        "extractedFields",
        parseRawData("origin.value")
      )
      .select(
        "extractedFields.*",
        "origin"
      )
  }

  private def withParseDetails(source: DataFrame): DataFrame = {
    source.withColumn(
      "parse_details",
      struct(
        when(validConditionExpr, lit("OK")).otherwise(lit("NOT_VALID")) as "status",
        additionalDetailsExpr as "info",
        current_timestamp() as "at"
      )
    )
  }

  private def insertIntoDelta(batchId: Long)(source: DataFrame): DataFrame = {
    source.withColumn("batch_id", typedLit(batchId))
    //Use the batch_id in tables to guarantee exactly-once

    insert(source, parsingValidCol, uniqueConditions, tableName)
    insert(source, parsingInvalidCol, InvalidRecord.uniqueConditions, InvalidRecord.tableName)

    source
  }


  private def insert(source: DataFrame, columnPredicate: Column, uniqueConditions: String, tableName: String) {
    val flattened = source.where(columnPredicate).flattenSchema("_").as("y")

    DeltaTable.forName(tableName).as("x")
      .merge(flattened, uniqueConditions)
      .whenNotMatched().insertAll()
      .execute()
  }

  private def compressFiles() {
    //Decompress in interval's here ?
    // val partitions: List[String]
    /* DeltaLogHelpers.partitionedLake1GbChunks

     spark.read
       .format("delta")
       .load(path)
       .where(partition)
       .repartition(numFilesPerPartition)
       .write
       .option("dataChange", "false")
       .format("delta")
       .mode("overwrite")
       .option("replaceWhere", partition)
       .save(path)*/
  }
}
