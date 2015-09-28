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
object NewTestGraph {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Neighborhood Profile").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val num_buckets = 256
    val distance = 3
    var initialVertexArray = Array((1L, (1L, Array(Array(Array((Long, Long)))))), (2L, (2L, Array(Array(Array((Long, Long)))))))
    var initialEdgeArray = Array(Edge(1L, 2L, 1L), Edge(2L, 1L, 1L))

    var users = sc.parallelize(initialVertexArray)

    val relationships: RDD[Edge[Long]] = sc.parallelize(initialEdgeArray)
    val inputgraph = Graph(users, relationships)

    val initialmsg = new PropogationObjectApprox(1L, 2L, new SlidingHLL(num_buckets), 1L, 0)
//    val outputgraph = Pregel(inputgraph, (0, Array(initialmsg)), distance)(vertexProgram, sendMessage, messageCombiner)
    //inputgraph.pregel((0, Array(initialmsg)), distance, EdgeDirection.Both)(vprog, sendMessage, messageCombiner)
//    println(outputgraph.vertices.collect.foreach {
//      case (vertexId, (value, original_value)) => println(value)
//    })
    //    var newVertexArray = Array((1L, (1L, new NodeApprox(distance, num_buckets))), (3L, (3L, new NodeApprox(distance, num_buckets))))
    //    var newEdgeArray = Array(Edge(1L, 3L, 2))
    //
    //    var newusers: RDD[(VertexId, (Long, NodeApprox))] = sc.parallelize(newVertexArray)
    //    var newrelationships: RDD[Edge[Int]] = sc.parallelize(initialEdgeArray)
    //    newusers = newusers.leftOuterJoin(users).filter(x => {
    //      if (x._2._2 == None)
    //        true
    //      else
    //        false
    //
    //    }).map(x => (x._1, (x._1, x._2._1._2)))
    //    users = users.union(newusers)
    //    println("new: " + users.collect().length)
    //    println("new: " + newusers.collect().length)

  }

  def vertexProgram(id: VertexId, value: (Long, Array[Array[Array[(Long, Long)]]]), msgSum: (Int, Array[PropogationObjectApprox])): (Long, Array[Array[Array[(Long, Long)]]]) = {

    value
  }
  def messageCombiner(msg1: (Int, Array[PropogationObjectApprox]), msg2: (Int, Array[PropogationObjectApprox])): (Int, Array[PropogationObjectApprox]) = (msg1._1, msg1._2 ++ msg2._2)

  def sendMessage(triplet: EdgeTriplet[(Long, NodeApprox), Long]): Iterator[(VertexId, (Int, Array[PropogationObjectApprox]))] = {

    val sourceVertex = triplet.srcAttr
    var propogationObj = new PropogationObjectApprox(triplet.dstId, triplet.srcId,
      sourceVertex._2.getDistanceWiseSummaries.get(sourceVertex._2.getCurrentSuperStep), triplet.attr, sourceVertex._2.getCurrentSuperStep + 1)

    if (!sourceVertex._2.isIschanged)
      Iterator.empty
    else
      Iterator((triplet.dstId, (sourceVertex._2.getCurrentSuperStep + 1, Array(propogationObj))))
  }

 
}