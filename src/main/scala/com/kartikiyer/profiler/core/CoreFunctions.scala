package com.kartikiyer.profiler.core

trait CoreFunctions extends DataProfilePlan {

  def totalCount: CoreFunctions = {
    addQuery("Count", "Select count(*) from @TABLE", null)
    this
  }

  def nullCount(columns: Array[String] = null): CoreFunctions = {
    addQuery("NullCount", "Select count(*) from @TABLE where @COLUMN is null", columns)
    this
  }

  def blankStringCount(columns: Array[String] = null): CoreFunctions = {
    addQuery("BlankStringCount", "Select count(*) from @TABLE here trim(@COLUMN) =''", columns)
    this
  }

  def min(columns: Array[String] = null): CoreFunctions = {
    addQuery("Min", f"Select min(@COLUMN) fom @TABLE", columns)
    this
  }

  def max(columns: Array[String] = null): CoreFunctions = {
    addQuery("Max", f"Select max(@COLUMN) from @TABLE", columns)
    this
  }
}
