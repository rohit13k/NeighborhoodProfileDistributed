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
import java.lang.Boolean
object TestGraphExact {

  val logger = Logger.getLogger(getClass().getName());
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //    val sc = new SparkContext(new SparkConf().setAppName("NeighborhoodProfile").setMaster("local[*]"))
    val prop = new Properties()
    try {
      val configFile = args(0).toString
      prop.load(new FileInputStream(configFile))

      logger.info("Test Started")
      val mode = prop.getProperty("mode", "cluster")
      val conf = new SparkConf().setAppName("NeighbourHoodProfileExact")
//      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      conf.registerKryoClasses(Array(classOf[NodeExact]))
//      conf.set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops")

      if (mode.equals("local")) {
        conf.setMaster("local[*]")
      }

      val sc = new SparkContext(conf)

      val inputfile = prop.getProperty("inputfile", "facebook_reduced.csv")
      val outputFile = prop.getProperty("outputFile", "facebook_reduced_estimate.csv")
      val folder = prop.getProperty("folder", "./data/")
      val tmpfolder = prop.getProperty("tmpfolder", "./data/")
      val timeFile = prop.getProperty("timefile", "time.csv")
      val distance = Integer.parseInt(prop.getProperty("distance", "3"))
      val batch = Integer.parseInt(prop.getProperty("batch", "1000"))
      sc.setCheckpointDir(tmpfolder)
      val numPartitions = Integer.parseInt(prop.getProperty("partition", "6"))
      val writeResult = Boolean.parseBoolean((prop.getProperty("writeResult", "false")))

      val ftime = new File(folder + timeFile)
      val bwtime = new BufferedWriter(new FileWriter(ftime))
      try {
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
        var startime = new Date().getTime
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
                inputVertexArray = Array((node1, new NodeExact(node1, new Array[Long2LongArrayMap](distance))))
              } else {
                inputVertexArray = inputVertexArray ++ Array((node1, new NodeExact(node1, new Array[Long2LongArrayMap](distance))))
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
              users = sc.parallelize(inputVertexArray, numPartitions)
              relationships = sc.parallelize(inputEdgeArray, numPartitions)
              users.cache()
              relationships.cache()
              graph = Graph(users, relationships)
              graph.cache()
              graph.vertices.count()
              users.unpersist(false)
              relationships.unpersist(false)
              isFirst = false

            } else {

              var newusers: RDD[(VertexId, NodeExact)] = sc.parallelize(inputVertexArray, numPartitions)
              val oldusers = graph.vertices
              //creating new user rdd by removing existing users in graph from the list of new users
              newusers = newusers.leftOuterJoin(oldusers).filter(x => {
                if (x._2._2 == None)
                  true
                else
                  false

              }).map(x => (x._1, new NodeExact(x._1, x._2._1.summary)))
              users = oldusers.union(newusers).coalesce(numPartitions, false)

              //creating new relationship rdd by removing existing relationships from graph if the edge is existing 
              val newrelationships: RDD[Edge[Long]] = sc.parallelize(inputEdgeArray, numPartitions)

              relationships = graph.edges.union(newrelationships).coalesce(numPartitions, false)
              relationships.cache()
              users.cache()
              graph = Graph(users, relationships)
              graph.cache()
              users.unpersist(false)
              relationships.unpersist(false)

            } //end of else
            val newgraph = graph
            graph = Pregel(graph, (0, msgs), itteration, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
            graph.cache()
            newgraph.unpersist(false)
            //                graph = PregelCorrected(graph, (0, msgs), itteration, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
            //             graph.pickRandomVertex();
            //        graph.inDegrees.count();
            nodes = nodes.empty
            edges = edges.empty
            inputEdgeArray = null
            inputVertexArray = null
            println("Done: " + total + " at : " + new Date())
            bwtime.write(total + "," + (new Date().getTime - startime) + "\n")
            bwtime.flush()
            startime = new Date().getTime
          } //end of if

        } // end of for loop
        if (count != 0) {

          count = 0
          //creating vertext RDD from input 
          for (node1 <- nodes.iterator) {
            if (inputVertexArray == null) {
              inputVertexArray = Array((node1, new NodeExact(node1, new Array[Long2LongArrayMap](distance))))
            } else {
              inputVertexArray = inputVertexArray ++ Array((node1, new NodeExact(node1, new Array[Long2LongArrayMap](distance))))
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
            users = sc.parallelize(inputVertexArray, numPartitions)
            relationships = sc.parallelize(inputEdgeArray, numPartitions)
            users.cache()
            relationships.cache()
            graph.cache()
            graph.vertices.count()
            isFirst = false
            users.unpersist(false)
            relationships.unpersist(false)
          } else {

            var newusers: RDD[(VertexId, NodeExact)] = sc.parallelize(inputVertexArray, numPartitions)
            val oldusers = graph.vertices
            //creating new user rdd by removing existing users in graph from the list of new users
            newusers = newusers.leftOuterJoin(oldusers).filter(x => {
              if (x._2._2 == None)
                true
              else
                false

            }).map(x => (x._1, new NodeExact(x._1, x._2._1.summary)))
            users = oldusers.union(newusers).coalesce(numPartitions, false)

            //creating new relationship rdd by removing existing relationships from graph if the edge is existing 
            val newrelationships: RDD[Edge[Long]] = sc.parallelize(inputEdgeArray, numPartitions)

            relationships = graph.edges.union(newrelationships).coalesce(numPartitions, false)
            graph = Graph(users, relationships, null, StorageLevel.MEMORY_AND_DISK_SER, StorageLevel.MEMORY_AND_DISK_SER)
            users.cache()
            relationships.cache()
            graph.cache()
            graph.vertices.count()
            users.unpersist(false)
            relationships.unpersist(false)
          } //end of else

          val newgraph = graph
          graph = PregelCorrected(graph, (0, msgs), itteration, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
          graph.cache()
          nodes = nodes.empty
          edges = edges.empty
          inputEdgeArray = null
          inputVertexArray = null

          users.unpersist(blocking = false)
          relationships.unpersist(blocking = false)
          println("Done: " + total + " at : " + new Date())
          bwtime.write(total + "," + (new Date().getTime - startime) + "\n")
          bwtime.flush()

        }
        println("Time : " + (new Date().getTime - startime))
        /*
     * Print the output
     * 
     */
        if (writeResult) {
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
      } finally {
        bwtime.flush()
        bwtime.close()
      }
    } catch {
      case e: Exception =>
        logger.error(e)
    }
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