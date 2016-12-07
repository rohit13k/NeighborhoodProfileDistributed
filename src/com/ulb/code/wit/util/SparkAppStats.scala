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
  var url = "http://localhost:4042/api/v1/applications/app-20161102152721-0000/stages"

  def main (args: Array[String]) {
    val json = fromURL(url).mkString
    val stages: List[SparkStage] = parse(json).extract[List[SparkStage]].filter { _.name.equals("mapPartitions at GraphImpl.scala:207") }
    println("stages count: " + stages.size)
    println("shuffleWriteBytes: " + stages.map(_.shuffleWriteBytes).sum/(1024*1024))
    println("shuffleReadBytes: " + stages.map(_.shuffleReadBytes).sum/(1024*1024))
    println("memoryBytesSpilled: " + stages.map(_.memoryBytesSpilled).sum)
    println("diskBytesSpilled: " + stages.map(_.diskBytesSpilled).sum)
  }

}