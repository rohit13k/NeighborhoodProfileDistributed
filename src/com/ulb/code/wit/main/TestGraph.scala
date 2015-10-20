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

import scala.util.{ Failure, Try }
object TestGraph {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
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
    conf.registerKryoClasses(Array(classOf[NodeApprox], classOf[SlidingHLL], classOf[Element], classOf[PropogationObjectApprox], classOf[BucketAndHash]))
    conf.set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops")
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.driver.memory", "2g")
    if (mode.equals("local")) {
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)
    val inputfile = ConfigUtil.get[String]("inputfile", "facebook_reduced.csv")
    val outputFile = ConfigUtil.get[String]("outputFile", "facebook_reduced_estimate.csv")
    val folder = ConfigUtil.get[String]("folder", ".//data//")
    val num_buckets = ConfigUtil.get[Int]("number_of_bucket", 256)
    val distance = ConfigUtil.get[Int]("distance", 3)
    val batch = ConfigUtil.get[Int]("batch", 1000)

    val itteration = distance - 1
    var line = ""
    var output = new StringBuilder
    var node1 = 0L
    var node2 = 0L
    var time = 0L
    var isFirst = true
    var inputVertexArray: Array[(Long, (Long, NodeApprox))] = null
    var inputEdgeArray: Array[Edge[Long]] = null
    var users: RDD[(VertexId, (Long, NodeApprox))] = null
    var relationships: RDD[Edge[Long]] = null
    var graph: Graph[(Long, NodeApprox), Long] = null
    var total = 0
    var count = 0
    var edges = collection.mutable.Map[(Long, Long), Long]()
    var nodes = collection.mutable.Set[Long]()
    var msgs: Array[PropogationObjectApprox] = null
    val startime = new Date().getTime
    for (line <- Source.fromFile(folder + inputfile).getLines()) {
      val tmp = line.split(",")

      count = count + 1
      total = total + 1
      edges.put((tmp(0).toLong, tmp(1).toLong), tmp(2).toLong)
      nodes.add(tmp(0).toLong)
      nodes.add(tmp(1).toLong)
      if (count == batch) {

        count = 0
        //creating vertext RDD from input 
        for (node1 <- nodes.iterator) {
          if (inputVertexArray == null) {
            inputVertexArray = Array((node1, (node1, new NodeApprox(distance, num_buckets))))
          } else {
            inputVertexArray = inputVertexArray ++ Array((node1, (node1, new NodeApprox(distance, num_buckets))))
          }

        }
        //creating vertex RDD from input
        for (((node1, node2), time) <- edges.seq.iterator) {
          if (inputEdgeArray == null) {
            inputEdgeArray = Array(Edge(node1, node2, time), Edge(node2, node1, time))
          } else
            inputEdgeArray = inputEdgeArray ++ Array(Edge(node1, node2, time), Edge(node2, node1, time))
          if (msgs == null)
            msgs = Array((new PropogationObjectApprox(node1, node2, null, node1, 0)), (new PropogationObjectApprox(node2, node1, null, node2, 0)))
          else
            msgs = msgs ++ Array((new PropogationObjectApprox(node1, node2, null, node1, 0)), (new PropogationObjectApprox(node2, node1, null, node2, 0)))
        }
        if (isFirst) {

          //          println("initialvertex : " + inputVertexArray.length + " : " + nodes.size)
          //          println("initialedge : " + inputEdgeArray.length)
          users = sc.parallelize(inputVertexArray)
          relationships = sc.parallelize(inputEdgeArray)
          graph = Graph(users, relationships)

          isFirst = false

        } else {

          var newusers: RDD[(VertexId, (Long, NodeApprox))] = sc.parallelize(inputVertexArray)
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

        //        graph = graph.pregel((0, msgs), distance, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
        graph = PregelCorrected(graph, (0, msgs), itteration, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)

        nodes = nodes.empty
        edges = edges.empty
        inputEdgeArray = null
        inputVertexArray = null
        users.unpersist(blocking = false)
        relationships.unpersist(blocking = false)
        println("Done: " + total + " at : " + new Date())
      } //end of if

    } // end of for loop
    if (count != 0) {

      count = 0
      //creating vertext RDD from input 
      for (node1 <- nodes.iterator) {
        if (inputVertexArray == null) {
          inputVertexArray = Array((node1, (node1, new NodeApprox(distance, num_buckets))))
        } else {
          inputVertexArray = inputVertexArray ++ Array((node1, (node1, new NodeApprox(distance, num_buckets))))
        }

      }
      //creating vertex RDD from input
      for (((node1, node2), time) <- edges.seq.iterator) {
        if (inputEdgeArray == null) {
          inputEdgeArray = Array(Edge(node1, node2, time), Edge(node2, node1, time))
        } else
          inputEdgeArray = inputEdgeArray ++ Array(Edge(node1, node2, time), Edge(node2, node1, time))
        if (msgs == null)
          msgs = Array((new PropogationObjectApprox(node1, node2, null, node1, 0)), (new PropogationObjectApprox(node2, node1, null, node2, 0)))
        else
          msgs = msgs ++ Array((new PropogationObjectApprox(node1, node2, null, node1, 0)), (new PropogationObjectApprox(node2, node1, null, node2, 0)))
      }
      if (isFirst) {

        //        println("initialvertex : " + inputVertexArray.length + " : " + nodes.size)
        //        println("initialedge : " + inputEdgeArray.length)
        users = sc.parallelize(inputVertexArray)
        relationships = sc.parallelize(inputEdgeArray)
        graph = Graph(users, relationships)
        isFirst = false

      } else {

        var newusers: RDD[(VertexId, (Long, NodeApprox))] = sc.parallelize(inputVertexArray)
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

      //        graph = graph.pregel((0, msgs), distance, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
      graph = PregelCorrected(graph, (0, msgs), itteration, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)

      nodes = nodes.empty
      edges = edges.empty
      inputEdgeArray = null
      inputVertexArray = null
      println("Done: " + total + " at : " + new Date())

    }
    println("Completed in time : " + (new Date().getTime - startime))
    /*
     * Print the output
     * 
     */
    graph.vertices.collect.foreach {
      case (vertexId, (value, original_value)) => {
        //        println("node summary for " + value + " : " + original_value.getNodeSummary.estimate())
        output.append(value + "," + original_value.getNodeSummary.estimate() + "\n")
      }
    }
    val f = new File(folder + outputFile)
    val bw = new BufferedWriter(new FileWriter(f))
    bw.write(output.toString())
    bw.close()

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