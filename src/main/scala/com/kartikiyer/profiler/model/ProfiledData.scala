package com.kartikiyer.profiler.model

import scala.collection.mutable
import scala.collection.mutable._

class ProfiledData(val tableName: String) {

  private val result: mutable.HashMap[String, ListBuffer[(String, String)]] = mutable.HashMap()
  private val sqlExceptions: ListBuffer[(String, Exception)] = mutable.ListBuffer()

  def getResult: mutable.HashMap[String, ListBuffer[(String, String)]] = result

  def getSqlExceptions: ListBuffer[(String, Exception)] = sqlExceptions

  def addMetric(metricLabel: String): Unit = result += metricLabel -> new ListBuffer()

  def updateMetric(metricLabel: String, columnName: String, value: String): Unit = result(metricLabel) += ((columnName, value))

  def storeException(query: String, exception: Exception): Unit = sqlExceptions += ((query, exception))

  override def toString: String = {
    val output = new mutable.StringBuilder()
    val colDelim: String = " , "
    val rowDelim: String = "\n"

    output.append("Table Name  :: " + tableName)
    output.append(rowDelim)

    result.foreach(metric => {
      output.append(metric._1).append(rowDelim)
      metric._2.foreach(value => output.append(value._1).append(colDelim).append(value._2).append(colDelim))
      output.append(rowDelim)
      Unit
    })
    output.toString()
  }

}