package com.ulb.code.wit.util

import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import scala.io.Source.fromURL

object SparkAppStats {

  /**
   * (partial) representation of a Spark Stage object
   */
  case class SparkStage(stageId: Int, name: String, submissionTime: String, completionTime: String, details: String, shuffleWriteBytes: Long, shuffleReadBytes: Long, memoryBytesSpilled: Long, diskBytesSpilled: Long, accumulatorUpdates: List[Details])
  case class Details(name: String, id: Int, value: String)
  case class SparkJobs(jobId: Int, name: String, submissionTime: String, completionTime: String,stageIds:List[Long])
  case class taskDetail(taskId: Int, taskLocality:String,executorId: String, taskMetrics: taskMetrics, accumulatorUpdates: List[Details])
  case class taskMetrics(shuffleReadMetrics: shuffleReadMetrics,shuffleWriteMetrics:shuffleWriteMetrics,executorDeserializeTime:Int,executorRunTime:Int,jvmGcTime:Int,diskBytesSpilled:Long,outputMetrics:outputMetrics,inputMetrics:inputMetrics)
  case class shuffleReadMetrics(remoteBytesRead: Long,fetchWaitTime:Long, remoteBlocksFetched: Long,recordsRead:Long,localBytesRead:Long,localBlocksFetched:Long)
  case class shuffleWriteMetrics(bytesWritten:Long,writeTime:Long,recordsWritten:Long)
  case class inputMetrics(bytesRead:Long,recordsRead:Long)
  case class outputMetrics(bytesWritten:Long,recordsWritten:Long)
  implicit val formats = DefaultFormats
  var url = "http://localhost:4040/api/v1/applications/local-1485446946983/stages"

  def main(args: Array[String]) {
    val json = fromURL(url).mkString
    val stages: List[SparkStage] = parse(json).extract[List[SparkStage]].filter { _.name.equals("mapPartitions at GraphImpl.scala:207") }
    println("stages count: " + stages.size)
    println("shuffleWriteBytes: " + stages.map(_.shuffleWriteBytes).sum / (1024 * 1024))
    println("shuffleReadBytes: " + stages.map(_.shuffleReadBytes).sum / (1024 * 1024))
    println("memoryBytesSpilled: " + stages.map(_.memoryBytesSpilled).sum)
    println("diskBytesSpilled: " + stages.map(_.details))

    val detail = stages.map(_.accumulatorUpdates
      .filter { _.name.equals("internal.metrics.shuffle.read.remoteBytesRead") }.
      map(_.value.toLong).sum).sum
    println(detail)
  }

}