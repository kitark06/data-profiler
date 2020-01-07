package com.kartikiyer.profiler.source

import com.kartikiyer.profiler.model.ConfiguredSource
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object InputSources {

  val defaultTableName: String = "AutoProfilerStagingTable"


  /*def hive(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .config("hive.metastore.uris", "thrift://localhost:9083") // replace with your hivemetastore service's thrift url
      .enableHiveSupport() // don't forget to enable hive support
      .getOrCreate()
  }*/

  def distFileSystem(appName: String, filePath: String, delimiter: String): ConfiguredSource = {
    val sparkSession: sql.SparkSession = SparkSession.builder().master("local[*]").appName(appName).getOrCreate()
    val df = sparkSession.read.option("header", "true").option("delimiter", delimiter).csv(filePath).toDF()
    df.createTempView(defaultTableName)
    new ConfiguredSource(sparkSession, defaultTableName)
  }

  def localFileSystem(appName: String, filePath: String, delimiter: String, hadoopBinariesPath: String, sparkMasterURL: String): ConfiguredSource = {
    System.setProperty("hadoop.home.dir", hadoopBinariesPath)
    val sparkSession: sql.SparkSession = SparkSession.builder().master(sparkMasterURL).appName(appName).getOrCreate()
    val df = sparkSession.read.option("header", "true").option("delimiter", delimiter).csv(filePath).toDF()
    df.createTempView(defaultTableName)
    new ConfiguredSource(sparkSession, defaultTableName)
  }
}
