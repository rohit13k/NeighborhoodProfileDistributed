package com.ulb.code.wit.main

/**
 * @author Rohit
 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import com.ulb.code.wit.util._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import scala.util.control.Breaks.break
import scala.collection.mutable.HashMap
import java.io.{ File, FileWriter, BufferedWriter }
import java.util.Date
import scala.reflect.ClassTag
import org.apache.spark.storage.StorageLevel
import java.util.Properties
import java.io.FileInputStream
import scala.util.{ Failure, Try }
import it.unimi.dsi.fastutil.longs.Long2LongArrayMap
object TestGraphExact {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //    val sc = new SparkContext(new SparkConf().setAppName("NeighborhoodProfile").setMaster("local[*]"))

    val configFile = args(0).toString
    if (configFile.startsWith("--config=")) {
      val filePath = configFile.split("[=]")(1)
      Try(ConfigUtil.load(filePath)) match {
        case Failure(e) =>
          println(s"\n\nERROR: Could not load config file $filePath. Please specify a valid config file.\n\n $e")
          System.exit(0)
        case _ =>
      }
    } else {
      println("\n\nERROR: Invalid arguments!\n\nSyntax: com.sbs.PersonIdTool --config=<path-to-application.conf>\n\n")
      System.exit(0)
    }
    val mode = ConfigUtil.get[String]("mode", "cluster")
    val conf = new SparkConf().setAppName("NeighbourHoodProfile")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[NodeExact]))
    conf.set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops")
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.driver.memory", "2g")
    if (mode.equals("local")) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    val inputfile = ConfigUtil.get[String]("inputfile", "simple.csv")
    val outputFile = ConfigUtil.get[String]("outputFile", "simple_out.csv")
    val folder = ConfigUtil.get[String]("folder", ".//data//")

    val distance = ConfigUtil.get[Int]("distance", 3)
    val batch = ConfigUtil.get[Int]("batch", 1000)
    val itteration = distance - 1
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
    var graph: Graph[NodeExact, Long] = null
    var total = 0
    var count = 0
    var edges = collection.mutable.Map[(Long, Long), Long]()
    var nodes = collection.mutable.Set[Long]()
    var msgs: Array[(Long, Long, Long)] = null
    val startime = new Date().getTime
    for (line <- Source.fromFile(folder + inputfile).getLines()) {
      val tmp = line.split(",")
      total = total + 1
      count = count + 1
      edges.put((tmp(0).toLong, tmp(1).toLong), tmp(2).toLong)
      nodes.add(tmp(0).toLong)
      nodes.add(tmp(1).toLong)
      if (count == batch) {
        count = 0

        //creating vertext RDD from input 
        for (node1 <- nodes.iterator) {
          if (inputVertexArray == null) {
            inputVertexArray = Array((node1, new NodeExact(node1, new Array[Long2LongArrayMap](3))))
          } else {
            inputVertexArray = inputVertexArray ++ Array((node1, new NodeExact(node1, new Array[Long2LongArrayMap](3))))
          }

        }
        //creating Edge RDD from input
        for (((node1, node2), time) <- edges.seq.iterator) {
          if (inputEdgeArray == null) {
            inputEdgeArray = Array(Edge(node1, node2, time), Edge(node2, node1, time))
            //            inputEdgeArray = Array(Edge(node1, node2, time))
          } else
            //            inputEdgeArray = inputEdgeArray ++ Array(Edge(node1, node2, time))
            inputEdgeArray = inputEdgeArray ++ Array(Edge(node1, node2, time), Edge(node2, node1, time))
          if (msgs == null)
            msgs = Array((node1, node2, time), (node2, node1, time))
          else
            msgs = msgs ++ Array((node1, node2, time), (node2, node1, time))
        }
        if (isFirst) {

          //println("initialvertex : " + inputVertexArray.length + " : " + nodes.size)
          //          println("initialvertex : " + inputEdgeArray.length)
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

        graph = PregelCorrected(graph, (0, msgs), itteration, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)

        nodes = nodes.empty
        edges = edges.empty
        inputEdgeArray = null
        inputVertexArray = null
        println("Done: " + total + " at : " + new Date())
      } //end of if

    } // end of for loop
    if (count != 0) {

      count = 0
      //creating vertext RDD from input 
      for (node1 <- nodes.iterator) {
        if (inputVertexArray == null) {
          inputVertexArray = Array((node1, new NodeExact(node1, new Array[Long2LongArrayMap](3))))
        } else {
          inputVertexArray = inputVertexArray ++ Array((node1, new NodeExact(node1, new Array[Long2LongArrayMap](3))))
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

        //        println("initialvertex : " + inputVertexArray.length + " : " + nodes.size)
        //        println("initialedge: " + inputEdgeArray.length)
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

      graph = PregelCorrected(graph, (0, msgs), itteration, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)

      nodes = nodes.empty
      edges = edges.empty
      inputEdgeArray = null
      inputVertexArray = null

      users.unpersist(blocking = false)
      relationships.unpersist(blocking = false)
      println("Done: " + total + " at : " + new Date())

    }
    println("Time : " + (new Date().getTime - startime))
    /*
     * Print the output
     * 
     */

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
              if (value.summary(msgSum._1).containsKey(x._2)) {
                if (value.summary(msgSum._1).get(x._2) < x._3) {
                  //need to check if present in lower level

                  value.summary(msgSum._1).put(x._2, x._3)
                  value.ischanged = true
                  //need to remove from upper if present at later horizon

                }
              } else {
                value.summary(msgSum._1).put(x._2, x._3)
                value.ischanged = true
              }
            } else {
              val temp = new Long2LongArrayMap()
              temp.put(x._2, x._3)
              value.summary.update(msgSum._1, temp)
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

      val sum = triplet.srcAttr.summary(triplet.srcAttr.currentsuperstep)
      val tempItertator = sum.keySet().iterator()
      while (tempItertator.hasNext()) {
        val value = tempItertator.nextLong()
        val time = sum.get(value)
        if (triplet.dstId != value) {
          if (msg == null) {
            msg = Array((triplet.dstId, value, Math.min(time, triplet.attr)))
          } else {
            msg = msg ++ Array((triplet.dstId, value, Math.min(time, triplet.attr)))
          }
        }
      }

      if (msg == null) {
        Iterator.empty
      } else
        Iterator((triplet.dstId, (triplet.srcAttr.currentsuperstep + 1, msg)))
    } else
      Iterator.empty

  }

}