package com.kartikiyer.profiler

import com.kartikiyer.profiler.core.{CoreFunctions, DataProfilePlan}
import com.kartikiyer.profiler.sink.DelimitedSink
import com.kartikiyer.profiler.source.InputSources
import com.typesafe.config.ConfigFactory

object Initiator {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load().getConfig("com.kartikiyer.profiler")

    val localFileSystem = config.getConfig("localFileSystem")
    val appName = localFileSystem.getString("app-name")

    val hadoopBinariesPath = localFileSystem.getString("hadoop-binaries-path")
    val sparkMasterUrl = localFileSystem.getString("spark-master-url")

    val inputFilePath = localFileSystem.getString("input-file-path")
    val outputFilePath = localFileSystem.getString("output-file-path")
    val inputFileDelim = localFileSystem.getString("input-file-delim")
    val outputRowDelim = localFileSystem.getString("output-row-delim")
    val outputColDelim = localFileSystem.getString("output-col-delim")

    val configuredSource = InputSources.localFileSystem(appName, inputFilePath, inputFileDelim, hadoopBinariesPath, sparkMasterUrl)
    val configuredSink = new DelimitedSink(outputRowDelim, outputColDelim, outputFilePath)

    new DataProfilePlan with CoreFunctions {}
      .totalCount
      .nullCount()
      .blankStringCount()
      .min()
      .max()
      .profileData(configuredSource)
      .execute(configuredSink)
  }
}