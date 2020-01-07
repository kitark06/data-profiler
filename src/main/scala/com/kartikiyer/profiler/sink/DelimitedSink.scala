package com.kartikiyer.profiler.sink

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.kartikiyer.profiler.model.ProfiledData

import scala.collection.mutable

class DelimitedSink(val rowDelim: String, val colDelim: String, val localOutputPath: String) extends OutputSink {

  override def generateOutput(profiledData: ProfiledData): Unit = {
    val output = new mutable.StringBuilder()
    // leads to tabelname being printed even for file input source
    // output.append("Table Name  :: " + profiledData.tableName).append(rowDelim)

    profiledData.getResult.foreach(metric => {
      if (metric._2.isEmpty == false) {
        output.append(metric._1).append(rowDelim)
        metric._2.foreach(value => output.append(value._1).append(colDelim).append(value._2).append(colDelim))
        output.append(rowDelim)
      }
      Unit
    })

    Files.write(Paths.get(localOutputPath), output.toString.getBytes(StandardCharsets.UTF_8))

    if (profiledData.getSqlExceptions.isEmpty == false) logStoredExceptions(profiledData)
  }

  override def logStoredExceptions(profiledData: ProfiledData): Unit = {
    val output = new mutable.StringBuilder()

    profiledData.getSqlExceptions.foreach { exception =>
      output
        .append("Query Causing Exception :: ").append(exception._1).append(rowDelim)
        .append(exception._2.toString).append(rowDelim)
        .append(rowDelim).append("*******").append(rowDelim)
    }

    Files.write(Paths.get(localOutputPath + "_errors"), output.toString.getBytes(StandardCharsets.UTF_8))
  }
}