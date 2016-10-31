package com.ulb.code.wit.util

import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import scala.io.Source.fromURL

object SparkAppStats {

  /**
   * (partial) representation of a Spark Stage object 
   */
  case class SparkStage(name: String, shuffleWriteBytes: Long,shuffleReadBytes: Long, memoryBytesSpilled: Long, diskBytesSpilled: Long)
  
  implicit val formats = DefaultFormats
  val url = "http://localhost:4040/api/v1/applications/local-1477578853608/stages"

  def main (args: Array[String]) {
    val json = fromURL(url).mkString
    val stages: List[SparkStage] = parse(json).extract[List[SparkStage]]
    println("stages count: " + stages.size)
    println("shuffleWriteBytes: " + stages.map(_.shuffleWriteBytes).sum)
    println("shuffleReadBytes: " + stages.map(_.shuffleReadBytes).sum)
    println("memoryBytesSpilled: " + stages.map(_.memoryBytesSpilled).sum)
    println("diskBytesSpilled: " + stages.map(_.diskBytesSpilled).sum)
  }

}