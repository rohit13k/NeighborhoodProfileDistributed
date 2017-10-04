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
import com.ulb.code.wit.util.Wait
/**
 * @author Rohit
 */
class PageRank(val vertexProgramCounter: MyAccumulator, val sendMsgProgramCounter: MyAccumulator, val mergeMsgProgramCounter: MyAccumulator, val vertexProgDelay: Int, val sendProgDelay: Int, val mergeProgDelay: Int) extends Serializable {
  val tol = 0.0001
  val resetProb: Double = 0.15
  def pageRank(graph: Graph[(Long, String), Long], PR_Itteration: Int): Array[(VertexId, Double)] = {
    val stime = new Date().getTime
    val pagerankGraph: Graph[(Double, Double, String), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (vid, vdata, deg) => (deg.getOrElse(0), vdata._2)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr._1)
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (id, attr) =>
        if (id == -1) (resetProb, Double.NegativeInfinity, attr._2) else (0.0, 0.0, attr._2)
      }
      .cache()
    val initialMessage = resetProb / (1.0 - resetProb)

    pagerankGraph.vertices.count()
    pagerankGraph.triplets.count()
    val ptime = new Date().getTime
    val node = PregelMon(pagerankGraph, initialMessage,
      PR_Itteration, EdgeDirection.Out)(
        vertexProgram,
        sendMessage,
        messageCombiner)(vertexProgramCounter, sendMsgProgramCounter, mergeMsgProgramCounter)
    val result = node.mapVertices { (id, attr) => (id, attr._1) }.vertices.collect().map(x => (x._1, x._2._2))

    println("PR time: " + (new Date().getTime - stime))
    println("PR Pregel time:" + (new Date().getTime - ptime))
    result
  }
  def vertexProgram(id: VertexId, attr: (Double, Double, String), msgSum: Double): (Double, Double, String) = {
    if (vertexProgDelay != -1) {
      if (vertexProgDelay == 0) {
        vertexProgramCounter.add(1)
      } else {
        vertexProgramCounter.add(vertexProgDelay)
        //        Thread.sleep(vertexProgDelay)
        Wait.waitfor(vertexProgDelay)
      }
    }
    val (oldPR, lastDelta, data) = attr
    val newPR = oldPR + (1.0 - resetProb) * msgSum
    (newPR, newPR - oldPR, attr._3)
  }
  def sendMessage(edge: EdgeTriplet[(Double, Double, String), Double]) = {
    if (sendProgDelay != -1) {
      if (sendProgDelay == 0) {
        sendMsgProgramCounter.add(1)
      } else {
        sendMsgProgramCounter.add(sendProgDelay)
        //        Thread.sleep(sendProgDelay)
        Wait.waitfor(sendProgDelay)
      }
    }
    if (edge.srcAttr._2 > tol) {
      Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
    } else {
      Iterator.empty
    }
  }

  def messageCombiner(a: Double, b: Double): Double = {
    if (mergeProgDelay != -1) {
      if (mergeProgDelay == 0) {
        mergeMsgProgramCounter.add(1)
      } else {
        mergeMsgProgramCounter.add(mergeProgDelay)
        //        Thread.sleep(mergeProgDelay)
        Wait.waitfor(mergeProgDelay)
      }
    }
    a + b
  }

}
object Rank {
  var minPartitions = 6
  private val logger = LoggerFactory getLogger classOf[PageRank]
  val tol = 0.0001
  val resetProb: Double = 0.15
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

    // println(result.mkString("\n"))
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr)
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (id, attr) =>
        if (id == -1) (resetProb, Double.NegativeInfinity) else (0.0, 0.0)
      }
      .cache()
    val initialMessage = resetProb / (1.0 - resetProb)
    val node = Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vertexProgram, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1).vertices

    //      val node = graph.pageRank(tol).vertices
    val result = node.distinct().takeOrdered(50)(Ordering[Double].reverse.on(x => x._2))
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
  def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
    val (oldPR, lastDelta) = attr
    val newPR = oldPR + (1.0 - resetProb) * msgSum
    (newPR, newPR - oldPR)
  }
  def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
    if (edge.srcAttr._2 > tol) {
      Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
    } else {
      Iterator.empty
    }
  }

  def messageCombiner(a: Double, b: Double): Double = a + b
}