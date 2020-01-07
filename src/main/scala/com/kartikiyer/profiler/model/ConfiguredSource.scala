package com.kartikiyer.profiler.model

import org.apache.spark.sql.SparkSession

class ConfiguredSource(val sparkSession: SparkSession, val tableName: String)