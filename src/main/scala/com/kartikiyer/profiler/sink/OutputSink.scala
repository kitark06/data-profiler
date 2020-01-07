package com.kartikiyer.profiler.sink

import com.kartikiyer.profiler.model.ProfiledData

trait OutputSink {
  def generateOutput(profiledData: ProfiledData)

  def logStoredExceptions(profiledData: ProfiledData)
}
