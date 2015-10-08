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
import java.io.{ File, FileWriter, BufferedWriter }
import java.util.Date
object TestGraphExact {

  val distance = 4
  val batch = 5
  var graph: Graph[NodeExact, Long] = null
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("NeighborhoodProfile").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputfile = "simple.csv"
    val outputFile = "simple_out.csv"
    val folder = ".//data//"
    var line = ""
    var output = new StringBuilder
    var node1 = 0L
    var node2 = 0L
    var time = 0L
    var isFirst = true
    var inputVertexArray: Array[(Long, NodeExact)] = null
    var inputEdgeArray: Array[Edge[Long]] = null
    var users: RDD[(VertexId, NodeExact)] = null
    var relationships: RDD[Edge[Long]] = null

    var count = 0
    var edges = collection.mutable.Map[(Long, Long), Long]()
    var nodes = collection.mutable.Set[Long]()
    var msgs: Array[(Long, Long, Long)] = null
    val startime = new Date().getTime
    for (line <- Source.fromFile(folder + inputfile).getLines()) {
      val tmp = line.split(",")

      count = count + 1
      edges.put((tmp(0).toLong, tmp(1).toLong), tmp(2).toLong)
      nodes.add(tmp(0).toLong)
      nodes.add(tmp(1).toLong)
      if (count == batch) {
        count = 0
        //creating vertext RDD from input 
        for (node1 <- nodes.iterator) {
          if (inputVertexArray == null) {
            inputVertexArray = Array((node1, new NodeExact(node1, new Array[collection.mutable.Map[Long, Long]](3))))
          } else {
            inputVertexArray = inputVertexArray ++ Array((node1, new NodeExact(node1, new Array[collection.mutable.Map[Long, Long]](3))))
          }

        }
        //creating vertex RDD from input
        for (((node1, node2), time) <- edges.seq.iterator) {
          if (inputEdgeArray == null) {
            inputEdgeArray = Array(Edge(node1, node2, time), Edge(node2, node1, time))
          } else
            inputEdgeArray = inputEdgeArray ++ Array(Edge(node1, node2, time), Edge(node2, node1, time))
          if (msgs == null)
            msgs = Array((node1, node2, time), (node2, node1, time))
          else
            msgs = msgs ++ Array((node1, node2, time), (node2, node1, time))
        }
        if (isFirst) {

          println("initialvertex : " + inputVertexArray.length + " : " + nodes.size)
          println("initialvertex : " + inputEdgeArray.length)
          users = sc.parallelize(inputVertexArray)
          relationships = sc.parallelize(inputEdgeArray)
          graph = Graph(users, relationships)
          isFirst = false

        } else {

          var newusers: RDD[(VertexId, NodeExact)] = sc.parallelize(inputVertexArray)
          val oldusers = graph.vertices
          //creating new user rdd by removing existing users in graph from the list of new users
          newusers = newusers.leftOuterJoin(oldusers).filter(x => {
            if (x._2._2 == None)
              true
            else
              false

          }).map(x => (x._1, new NodeExact(x._1, x._2._1.summary)))
          users = oldusers.union(newusers)

          //creating new relationship rdd by removing existing relationships from graph if the edge is existing 
          val newrelationships: RDD[Edge[Long]] = sc.parallelize(inputEdgeArray)
          val oldrelationships = graph.edges.filter { x =>
            {
              if (((x.srcId == node1) & (x.dstId == node2)) || ((x.srcId == node2) & (x.dstId == node1))) {
                false
              } else
                true
            }
          }

          relationships = oldrelationships.union(newrelationships)
          graph = Graph(users, relationships)

        } //end of else

        graph = graph.pregel((0, msgs), distance, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
        var ver = graph.vertices.collect
        nodes = nodes.empty
      } //end of if

    } // end of for loop
    if (count != 0) {

      count = 0
      //creating vertext RDD from input 
      for (node1 <- nodes.iterator) {
        if (inputVertexArray == null) {
          inputVertexArray = Array((node1, new NodeExact(node1, new Array[collection.mutable.Map[Long, Long]](3))))
        } else {
          inputVertexArray = inputVertexArray ++ Array((node1, new NodeExact(node1, new Array[collection.mutable.Map[Long, Long]](3))))
        }

      }
      //creating vertex RDD from input
      for (((node1, node2), time) <- edges.seq.iterator) {
        if (inputEdgeArray == null) {
          inputEdgeArray = Array(Edge(node1, node2, time), Edge(node2, node1, time))
        } else
          inputEdgeArray = inputEdgeArray ++ Array(Edge(node1, node2, time), Edge(node2, node1, time))
        if (msgs == null)
          msgs = Array((node1, node2, time), (node2, node1, time))
        else
          msgs = msgs ++ Array((node1, node2, time), (node2, node1, time))
      }
      if (isFirst) {

        println("initialvertex : " + inputVertexArray.length + " : " + nodes.size)
        println("initialedge: " + inputEdgeArray.length)
        users = sc.parallelize(inputVertexArray)
        relationships = sc.parallelize(inputEdgeArray)
        graph = Graph(users, relationships)
        isFirst = false

      } else {

        var newusers: RDD[(VertexId, NodeExact)] = sc.parallelize(inputVertexArray)
        val oldusers = graph.vertices
        //creating new user rdd by removing existing users in graph from the list of new users
        newusers = newusers.leftOuterJoin(oldusers).filter(x => {
          if (x._2._2 == None)
            true
          else
            false

        }).map(x => (x._1, new NodeExact(x._1, x._2._1.summary)))
        users = oldusers.union(newusers)

        //creating new relationship rdd by removing existing relationships from graph if the edge is existing 
        val newrelationships: RDD[Edge[Long]] = sc.parallelize(inputEdgeArray)
        val oldrelationships = graph.edges.filter { x =>
          {
            if (((x.srcId == node1) & (x.dstId == node2)) || ((x.srcId == node2) & (x.dstId == node1))) {
              false
            } else
              true
          }
        }

        relationships = oldrelationships.union(newrelationships)
        graph = Graph(users, relationships)

      } //end of else

      graph = graph.pregel((0, msgs), distance, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)

      nodes = nodes.empty

    }
    println("Time : " + (new Date().getTime - startime))
    /*
     * Print the output
     * 
     */
    var ver = graph.vertices.collect

    graph.vertices.collect.foreach {
      case (vertexId, node) => {
        //        println("node summary for " + value + " : " + original_value.getNodeSummary.estimate())

        output.append(node.node + "," + node.getsummary(0L) + "\n")
      }
    }
    val f = new File(folder + outputFile)
    val bw = new BufferedWriter(new FileWriter(f))
    bw.write(output.toString())
    bw.close()

  }

  def vertexProgram(id: VertexId, value: NodeExact, msgSum: (Int, Array[(Long, Long, Long)])): NodeExact = {

    value.ischanged = false
    if (msgSum._2.exists(_._1 == value.node)) {

      msgSum._2.foreach({
        x =>
          if (x._1 == value.node) {
            if (value.summary(msgSum._1) != null) {
              if (value.summary(msgSum._1).contains(x._2)) {
                if (value.summary(msgSum._1).getOrElse((x._2), 0l) < x._3) {
                  value.summary(msgSum._1).update(x._2, x._3)
                  value.ischanged = true
                }
              } else {
                value.summary(msgSum._1) += (x._2 -> x._3)
                value.ischanged = true
              }
            } else {

              value.summary.update(msgSum._1, collection.mutable.Map((x._2 -> x._3)))
              value.ischanged = true
            }
          }
      })

    }

    value.currentsuperstep = msgSum._1
    value
  }
  def messageCombiner(msg1: (Int, Array[(Long, Long, Long)]), msg2: (Int, Array[(Long, Long, Long)])): (Int, Array[(Long, Long, Long)]) = (msg1._1, msg1._2 ++ msg2._2)

  def sendMessage(triplet: EdgeTriplet[NodeExact, Long]): Iterator[(VertexId, (Int, Array[(Long, Long, Long)]))] = {

    if (triplet.srcAttr.ischanged) {
      var msg: Array[(Long, Long, Long)] = null
      triplet.srcAttr.summary(triplet.srcAttr.currentsuperstep).seq.foreach({ x =>
        if (triplet.dstId != x._1) {
          if (msg == null) {
            msg = Array((triplet.dstId, x._1, Math.min(x._2, triplet.attr)))
          } else {
            msg ++ Array((triplet.dstId, x._1, Math.min(x._2, triplet.attr)))
          }
        }
      })
      Iterator((triplet.dstId, (triplet.srcAttr.currentsuperstep + 1, msg)))
    } else
      Iterator.empty

  }

}