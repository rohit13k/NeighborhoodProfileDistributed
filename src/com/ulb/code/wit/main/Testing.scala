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
import com.ulb.code.wit.util.NodeExact
import com.ulb.code.wit.util.NewNodeExact
import java.io.ObjectOutputStream
import java.io.FileOutputStream
/**
 * @author Rohit
 */
object Testing {
  def main(args: Array[String]) {
    val defaultNode = (new NewNodeExact(-1, new Array[scala.collection.immutable.HashMap[Long, Long]](3)), java.lang.Boolean.FALSE)
    val temp: collection.immutable.HashMap[Long, Long] = collection.immutable.HashMap(1l -> 1l)
    defaultNode._1.summary.update(0, temp)
    defaultNode._1.summary.update(1, temp)
    defaultNode._1.summary.update(2, temp)

    for (i <- 0 to 200) {
      defaultNode._1.summary.update(2, defaultNode._1.summary(2).+(i + 0l -> (i + 100l)))
    }
        for (i <- 0 to 100) {
          defaultNode._1.summary.update(1, defaultNode._1.summary(1).+(i + 0l -> (i + 100l)))
        }
    //    for (i <- 0 to 109) {
    //      defaultNode._1.summary.update(0, defaultNode._1.summary(0).+(i + 0l -> (i + 100l)))
    //    }

    val oos = new ObjectOutputStream(new FileOutputStream("D:\\dataset\\defaultnode.obj"))
    oos.writeObject(defaultNode)
    oos.close
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