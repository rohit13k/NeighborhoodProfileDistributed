package com.ulb.code.wit.main

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import java.util.Date
import org.slf4j.LoggerFactory
/**
 * @author Rohit
 */
class PageRank {

}
object Rank {
  var minPartitions = 6
  private val logger = LoggerFactory getLogger classOf[PageRank]
  def main(args: Array[String]) {
    var starttime = new Date().getTime
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Pagerank")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val f = args(0)
    if (args.length > 1)
      minPartitions = Integer.parseInt(args(1))
    starttime = new Date().getTime
    val path = f + ".txt"
    val seeds = f + "_pagerank_reversed_50.txt"
    val time = f + "_pageRankTime.txt"
    val bwresult = new BufferedWriter(new FileWriter(new File(time)))
    val relationships: RDD[Edge[Boolean]] =
      sc.parallelize(Array(Edge(1L, 2L, true), Edge(1L, 4L, true),
        Edge(2L, 4L, true), Edge(3L, 1L, true),
        Edge(3L, 4L, true)))
    // Create an RDD for edges
    val input = sc.textFile(path, minPartitions).map(x => x.split(",")).map(x => Edge(x(1).toLong, x(0).toLong, x(2).toLong))
    val defaultVertex = (0L, 0L)

    // Create the graph
    val graph = Graph.fromEdges(input, defaultVertex, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK);
    graph.cache()
    graph.vertices.count()
    val node = graph.pageRank(0.0001).vertices
    val result = node.distinct().takeOrdered(50)(Ordering[Double].reverse.on(x => x._2))
    // println(result.mkString("\n"))

    val fil = new File(seeds)

    val bw = new BufferedWriter(new FileWriter(fil))
    for (line <- result) {

      bw.write(line._1 + "\n")

    }
    bw.close
    bwresult.write("done " + f + ": " + (new Date().getTime - starttime) + "\n")
    logger.info("done " + f + ": " + (new Date().getTime - starttime))

    bwresult.close()
  }
//  def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
//      val (oldPR, lastDelta) = attr
//      val newPR = oldPR + (1.0 - resetProb) * msgSum
//      (newPR, newPR - oldPR)
//    }
//  def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
//      if (edge.srcAttr._2 > tol) {
//        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
//      } else {
//        Iterator.empty
//      }
//    }
//
//    def messageCombiner(a: Double, b: Double): Double = a + b
}