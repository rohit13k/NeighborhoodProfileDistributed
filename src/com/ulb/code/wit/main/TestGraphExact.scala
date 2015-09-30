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

  val distance = 3
  val batch = 1000
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("NeighborhoodProfile").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputfile = "facebook_reduced.csv"
    val outputFile = "facebook_reduced.csv"
    val folder = ".//data//"
    var line = ""
    var output = new StringBuilder
    var node1 = 0L
    var node2 = 0L
    var time = 0L
    var isFirst = true
    var inputVertexArray: Array[(Long, (Long, Array[collection.mutable.Map[Long, Long]]))] = null
    var inputEdgeArray: Array[Edge[Long]] = null
    var users: RDD[(VertexId, (Long, Array[collection.mutable.Map[Long, Long]]))] = null
    var relationships: RDD[Edge[Long]] = null
    var graph: Graph[(Long, Array[collection.mutable.Map[Long, Long]]), Long] = null

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
            inputVertexArray = Array((node1, (node1, new Array[collection.mutable.Map[Long, Long]](3))))
          } else {
            inputVertexArray = inputVertexArray ++ Array((node1, (node1, new Array[collection.mutable.Map[Long, Long]](3))))
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

          var newusers: RDD[(VertexId, (Long, Array[collection.mutable.Map[Long, Long]]))] = sc.parallelize(inputVertexArray)
          val oldusers = graph.vertices
          //creating new user rdd by removing existing users in graph from the list of new users
          newusers = newusers.leftOuterJoin(oldusers).filter(x => {
            if (x._2._2 == None)
              true
            else
              false

          }).map(x => (x._1, (x._1, x._2._1._2)))
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
      } //end of if

    } // end of for loop
    println("Time : " + (new Date().getTime - startime))
    /*
     * Print the output
     * 
     */
    graph.vertices.collect.foreach {
      case (vertexId, (value, original_value)) => {
        //        println("node summary for " + value + " : " + original_value.getNodeSummary.estimate())
        var total = collection.mutable.Set[Long]()
        original_value.foreach {
          value => total = total ++ value.keySet
        }
        output.append(value + "," + total.size + "\n")
      }
    }
    val f = new File(folder + outputFile)
    val bw = new BufferedWriter(new FileWriter(f))
    bw.write(output.toString())
    bw.close()

  }

  def vertexProgram(id: VertexId, value: (Long, Array[collection.mutable.Map[Long, Long]]), msgSum: (Int, Array[(Long, Long, Long)])): (Long, Array[collection.mutable.Map[Long, Long]]) = {

    //if inital msg
    if (msgSum._1 == 0) {
      //reset the variables to clear last round data 

    } else {
      //not initial msg

    }
    value
  }
  def messageCombiner(msg1: (Int, Array[(Long, Long, Long)]), msg2: (Int, Array[(Long, Long, Long)])): (Int, Array[(Long, Long, Long)]) = (msg1._1, msg1._2 ++ msg2._2)

  def sendMessage(triplet: EdgeTriplet[(Long, Array[collection.mutable.Map[Long, Long]]), Long]): Iterator[(VertexId, (Int, Array[(Long, Long, Long)]))] = {

    Iterator.empty

  }

}