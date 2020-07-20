package com.github.danp11.pipeline

import org.apache.spark.sql.types._

object InvalidRecord {

  val tableName: String = "invalid_record"

  val partitionColumns = List("ob_id") //DPDPDP fix

  val uniqueConditions = "x.origin_value = y.origin_value"

  val schema: StructType = StructType(
    Seq(
      StructField("ob_id", StringType, true),
      StructField("origin_value", StringType, false)
    ))
}
