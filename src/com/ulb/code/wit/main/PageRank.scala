package com.ulb.code.wit.main

/**
 * @author Rohit
 */
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import com.ulb.code.wit.util._
import org.apache.log4j.Logger
import org.apache.log4j.Level
object PageRank {
  val initialMsg = 9999
  def main(args: Array[String]) {
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Neighborhood Profile").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val vertices: RDD[(VertexId, (Int, Int))] =
      sc.parallelize(Array((1L, (7, -1)), (2L, (3, -1)),
        (3L, (2, -1)), (4L, (6, -1))))

    // Create an RDD for edges
    val relationships: RDD[Edge[Boolean]] =
      sc.parallelize(Array(Edge(1L, 2L, true), Edge(1L, 4L, true),
        Edge(2L, 4L, true), Edge(3L, 1L, true),
        Edge(3L, 4L, true)))

    // Create the graph
    val graph = Graph(vertices, relationships)

    // Check the graph
    graph.vertices.collect.foreach(println)

    val minGraph = graph.pregel(initialMsg,
      Int.MaxValue,
      EdgeDirection.Out)(
        vprog,
        sendMsg,
        mergeMsg)
    minGraph.vertices.collect.foreach {
      case (vertexId, (value, original_value)) => println(value)
    }
  }

  def vprog(vertexId: VertexId, value: (Int, Int), message: Int): (Int, Int) = {
    if (message == initialMsg)
      value
    else
      (message min value._1, value._1)
  }

  def mergeMsg(msg1: Int, msg2: Int): Int = msg1 min msg2
  def sendMsg(triplet: EdgeTriplet[(Int, Int), Boolean]): Iterator[(VertexId, Int)] = {
    val sourceVertex = triplet.srcAttr

    if (sourceVertex._1 == sourceVertex._2)
      Iterator.empty
    else
      Iterator((triplet.dstId, sourceVertex._1))
  }
}