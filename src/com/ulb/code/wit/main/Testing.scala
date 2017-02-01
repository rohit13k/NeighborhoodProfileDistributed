package com.ulb.code.wit.main

import com.ulb.code.wit.util.NodeApprox
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd._
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Date
/**
 * @author Rohit
 */
object Testing {
  def main(args: Array[String]) {
    val gd=GenerateData
    println(gd.stringToDate("2017-01-26T18:36:37.171GMT").getTime)
    
    //    testSpark
  }
  def slidingHyperAnf(vertexList: Array[(Long, NodeApprox)], edgeList: Array[(Long, Long, Long)], distance: Int, numberOfBucket: Int): Array[(Long, NodeApprox)] = {
    var i = 0
    var j = 0
    //initialize the 0th distance summary with the node itself
    vertexList.map { x =>
      {
        val log = new SlidingHLL(numberOfBucket)
        log.add(x._1)
        x._2.getDistanceWiseSummaries.add(log)
      }
    }
    for (i <- 1 to distance - 1) {
      vertexList.map { x =>
        {
          x._2.getDistanceWiseSummaries.add(new SlidingHLL(numberOfBucket))
          x._2.getDistanceWiseSummaries.get(i).union(x._2.getDistanceWiseSummaries.get(i - 1))
        }
      }

      for (j <- 0 to edgeList.length - 1) {

      }
    }

    return vertexList
  }
  def testSpark {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Test")
    conf.set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops")

    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sb = StringBuilder.newBuilder
    for (line <- Source.fromFile(".\\data\\facebook_reduced.csv").getLines()) {
      sb.append(line)
    }
    val broadcastVar = sc.broadcast(sb.toString())
    var rd = sc.textFile(".\\data\\facebook.csv", 1)

    rd = rd.map(x => {

      if (broadcastVar.value.length() == x.length()) {
        x
      } else {
        x + "~"
      }
    }).setName("rd")
    //    rd.cache()
    println(rd.count())
    val nrd = rd.map(x => {
      var temp = x.split(",")
      (temp(0), temp(1))
    }).setName("nrd")
    val trd = rd.map(x => (x, 1)).setName("trd")

    val temp = trd.join(nrd).setName("temp")
    temp.count()
    println("done")
    nrd.partitionBy(new HashPartitioner(2))
    multi("")_
  }
  def multi(x: String)(a: String)(b: Int): Int = {
    0
  }

}