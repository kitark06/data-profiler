package com.kartikiyer.profiler.core

import com.kartikiyer.profiler.model.{ConfiguredSource, ProfiledData}
import com.kartikiyer.profiler.sink.OutputSink
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

abstract class DataProfilePlan() {

  private val plan: ListBuffer[(String, String, Array[String])] = ListBuffer()
  var output: ProfiledData = _
  var spark: SparkSession = _

  def addQuery(queryLabel: String, queryString: String, columns: Array[String]): Unit = plan += ((queryLabel, queryString, columns))

  def execute(sink: OutputSink): Unit = sink.generateOutput(this.output)

  def profileData(configuredSource: ConfiguredSource): DataProfilePlan = {
    spark = configuredSource.sparkSession
    val tableName: String = configuredSource.tableName
    output = new ProfiledData(tableName)

    val metaData = spark.sql("describe table " + tableName).collect()
    val allColumns: Array[String] = metaData.map(row => row(0).toString)

    plan.foreach(p => {
      val label = p._1
      val query = p._2
      val userSuppliedColumns = p._3
      val columns = if (userSuppliedColumns == null) allColumns else userSuppliedColumns

      output.addMetric(label)

      try {
        if (query.contains("@COLUMN")) { //queries which are fired for each column eg min
          columns.foreach(column => {
            val formattedQuery: String = query.toUpperCase.replace("@TABLE", tableName).replace("@COLUMN", column)
            val result = spark.sql(formattedQuery).collect()(0).get(0).toString
            output.updateMetric(label, column, result)
          })
        }
        else { // table wide query like count(*) , which will be fired only once
          val formattedQuery: String = query.toUpperCase.replace("@TABLE", tableName)
          val result = spark.sql(formattedQuery).collect()(0).get(0).toString
          output.updateMetric(label, tableName, result)
        }
      }
      catch {
        case sqlException: org.apache.spark.sql.AnalysisException => output.storeException(query, sqlException)
      }
    })

    this
  }
}