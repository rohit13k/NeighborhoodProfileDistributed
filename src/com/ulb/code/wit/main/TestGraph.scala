package com.ulb.code.wit.main

/**
 * @author Rohit
 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import com.ulb.code.wit.util._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import scala.util.control.Breaks.break
import scala.collection.mutable.HashMap
object TestGraph {

  val num_buckets = 256
  val distance = 3
  val batch = 1000
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("NeighborhoodProfile").setMaster("local[*]")
    val sc = new SparkContext(conf)

    var line = ""
    var node1 = 0L
    var node2 = 0L
    var time = 0L
    var isFirst = true
    var initialVertexArray: Array[(Long, (Long, NodeApprox))] = null
    var initialEdgeArray: Array[Edge[Long]] = null
    var users: RDD[(VertexId, (Long, NodeApprox))] = null
    var relationships: RDD[Edge[Long]] = null
    var inputgraph: Graph[(Long, NodeApprox), Long] = null

    var msg1: PropogationObjectApprox = null
    var msg2: PropogationObjectApprox = null
    var outputgraph: Graph[(Long, NodeApprox), Long] = null
    var count = 0
    var edges = collection.mutable.Map[(Long, Long), Long]()
    var nodes = collection.mutable.Set[Long]()
    for (line <- Source.fromFile("./data/fb1.csv").getLines()) {
      val tmp = line.split(",")

      count = count + 1
      edges.put((tmp(0).toLong, tmp(1).toLong), tmp(2).toLong)
      nodes.add(tmp(0).toLong)
      nodes.add(tmp(1).toLong)
      if (count == batch) {

        if (isFirst) {

          for (node1 <- nodes.iterator) {
            initialVertexArray ++ Array((node1, (node1, new NodeApprox(distance, num_buckets))))
          }
          for (((node1, node2), time) <- edges.seq.iterator) {
            initialEdgeArray = Array(Edge(node1, node2, time), Edge(node2, node1, time))
          }
          println("initialvertex : " + initialVertexArray.length + " : " + nodes.size)
          println("initialvertex : " + initialEdgeArray.length)
          users = sc.parallelize(initialVertexArray)
          relationships = sc.parallelize(initialEdgeArray)
          inputgraph = Graph(users, relationships)
          isFirst = false

        } else {
          val newVertexArray = Array((node1, (node1, new NodeApprox(distance, num_buckets))), (node2, (node2, new NodeApprox(distance, num_buckets))))
          val newEdgeArray = Array(Edge(node1, node2, time), Edge(node2, node1, time))

          var newusers: RDD[(VertexId, (Long, NodeApprox))] = sc.parallelize(newVertexArray)
          val oldusers = inputgraph.vertices
          //creating new user rdd by removing existing users in graph from the list of new users
          newusers = newusers.leftOuterJoin(oldusers).filter(x => {
            if (x._2._2 == None)
              true
            else
              false

          }).map(x => (x._1, (x._1, x._2._1._2)))
          users = oldusers.union(newusers)

          //creating new relationship rdd by removing existing relationships from graph if the edge is existing 
          val newrelationships: RDD[Edge[Long]] = sc.parallelize(newEdgeArray)
          val oldrelationships = inputgraph.edges.filter { x =>
            {
              if (((x.srcId == node1) & (x.dstId == node2)) || ((x.srcId == node2) & (x.dstId == node1))) {
                false
              } else
                true
            }
          }

          relationships = oldrelationships.union(newrelationships)
          inputgraph = Graph(users, relationships)

        }

        msg1 = new PropogationObjectApprox(node1, node2, new SlidingHLL(num_buckets), node1, 0)
        msg2 = new PropogationObjectApprox(node2, node1, new SlidingHLL(num_buckets), node2, 0)
        inputgraph = Pregel(inputgraph, (0, Array(msg1, msg2)), distance)(vertexProgram, sendMessage, messageCombiner)
        nodes = nodes.empty
      }

      inputgraph.vertices.collect.foreach {
        case (vertexId, (value, original_value)) => println("node summary for " + value + " : " + original_value.getNodeSummary.estimate())
      }
    }
    //    initialVertexArray = Array((1L, (1L, new NodeApprox(distance, num_buckets))), (2L, (2L, new NodeApprox(distance, num_buckets))))
    //    initialEdgeArray = Array(Edge(1L, 2L, 1L), Edge(2L, 1L, 1L))
    //
    //    users = sc.parallelize(initialVertexArray)
    //
    //    relationships = sc.parallelize(initialEdgeArray)
    //    inputgraph = Graph(users, relationships)
    //
    //    var initialmsg1 = new PropogationObjectApprox(1L, 2L, new SlidingHLL(num_buckets), 1L, 0)
    //    var initialmsg2 = new PropogationObjectApprox(2L, 1L, new SlidingHLL(num_buckets), 2L, 0)
    //    outputgraph = Pregel(inputgraph, (0, Array(initialmsg1, initialmsg2)), distance)(vertexProgram, sendMessage, messageCombiner)
    //    //inputgraph.pregel((0, Array(initialmsg)), distance, EdgeDirection.Both)(vprog, sendMessage, messageCombiner)
    //    outputgraph.vertices.collect.foreach {
    //      case (vertexId, (value, original_value)) => println("node summary for " + value + " : " + original_value.getNodeSummary.estimate())
    //    }
    //    var newVertexArray = Array((1L, (1L, new NodeApprox(distance, num_buckets))), (4L, (4L, new NodeApprox(distance, num_buckets))))
    //    var newEdgeArray = Array(Edge(1L, 4L, 2L), Edge(4L, 1L, 2L))

    //    var newusers: RDD[(VertexId, (Long, NodeApprox))] = sc.parallelize(newVertexArray)
    //    var newrelationships: RDD[Edge[Long]] = sc.parallelize(newEdgeArray)
    //    newusers = newusers.leftOuterJoin(users).filter(x => {
    //      if (x._2._2 == None)
    //        true
    //      else
    //        false
    //
    //    }).map(x => (x._1, (x._1, x._2._1._2)))
    //    users = outputgraph.vertices.union(newusers)
    //    relationships = outputgraph.edges.union(newrelationships)
    //    inputgraph = Graph(users, relationships)
    //
    //    initialmsg1 = new PropogationObjectApprox(1L, 4L, new SlidingHLL(num_buckets), 1L, 0)
    //    initialmsg2 = new PropogationObjectApprox(4L, 1L, new SlidingHLL(num_buckets), 4L, 0)
    //    outputgraph = Pregel(inputgraph, (0, Array(initialmsg1, initialmsg2)), distance)(vertexProgram, sendMessage, messageCombiner)
    //inputgraph.pregel((0, Array(initialmsg)), distance, EdgeDirection.Both)(vprog, sendMessage, messageCombiner)

    //    println("new: " + users.collect().length)
    //    println("new: " + newusers.collect().length)

  }

  def vertexProgram(id: VertexId, value: (Long, NodeApprox), msgSum: (Int, Array[PropogationObjectApprox])): (Long, NodeApprox) = {

    //if inital msg
    if (msgSum._1 == 0) {
      //reset the variables to clear last round data 
      value._2.setCurrentSuperStep(msgSum._1)
      value._2.setIschanged(false)
      for (propObj <- msgSum._2) {
        //check if the current node and the target node is same 
        if (propObj.getTargetNode == value._1) {
          value._2.getDistanceWiseSummaries.get(0).add(propObj.getSourceNode, propObj.getTimestamp)

          value._2.setIschanged(true)
        }
      }
    } else {
      //not initial msg

      var targetSketch = value._2.getDistanceWiseSummaries.get(msgSum._1)
      for (propObj <- msgSum._2) {
        var sourceSketch = propObj.getSourceElement
        var changed = false
        var timeschanged = 0
        var sourceBuckets = sourceSketch.getBuckets()
        var bucketNo = 0
        for (bucketNo <- 0 to sourceBuckets.size() - 1) {
          var el = sourceBuckets.get(bucketNo)
          if (el != null) {
            var i = 0

            for (i <- 0 to el.size() - 1) {
              var element = el.getElement(i)
              if (propObj.getTimestamp < element.getTimestamp) {
                if (targetSketch.merge(bucketNo, element.getValue, propObj.getTimestamp)) {
                  timeschanged = timeschanged + 1
                }

              } else {
                if (targetSketch.merge(bucketNo, element)) {
                  timeschanged = timeschanged + 1
                }
              }
            }
          }
        }
        if (timeschanged > 0) {
          value._2.setIschanged(true)
        }

      }

      value._2.setCurrentSuperStep(msgSum._1)

    }
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