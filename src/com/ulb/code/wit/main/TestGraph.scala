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
import java.util.Properties
import java.io.FileInputStream
import java.lang.Boolean
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuilder
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.storage.{ StorageStatus, StorageStatusListener }
import org.apache.spark.ui.{ SparkUI, SparkUITab }
import org.apache.spark.ui.jobs.ExecutorTable
object TestGraph {
  // println(getClass().getName())
  //  val logger = Logger.getLogger(getClass().getName());
  def main(args: Array[String]) {
    val prop = new Properties()
    try {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      val configFile = args(0).toString
      prop.load(new FileInputStream(configFile))

      //      logger.info("Test Started")
      println("Test Started" + new Date())

      val mode = prop.getProperty("mode", "cluster")
      val conf = new SparkConf().setAppName("NeighbourHoodProfileApprox")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.registerKryoClasses(Array(classOf[NodeApprox], classOf[SlidingHLL], classOf[Element], classOf[PropogationObjectApprox], classOf[BucketAndHash]))
      conf.set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops")

      if (mode.equals("local")) {
        conf.setMaster("local[2]")
      }

      val sc = new SparkContext(conf)
      val numPartitions = Integer.parseInt(prop.getProperty("partition", "6"))
      val inputfile = prop.getProperty("inputfile", "facebook_reduced.csv")
      val outputFile = prop.getProperty("outputFile", "facebook_reduced_estimate.csv")
      val folder = prop.getProperty("folder", "./data/")
      val tmpfolder = prop.getProperty("tmpfolder", "")
      val timeFile = prop.getProperty("timefile", "time.csv")
      val num_buckets = Integer.parseInt(prop.getProperty("number_of_bucket", "256"))
      val distance = Integer.parseInt(prop.getProperty("distance", "3"))
      val storage = Boolean.parseBoolean((prop.getProperty("storage", "false")))
      val batch = Integer.parseInt(prop.getProperty("batch", "1000").trim())
      val writeResult = Boolean.parseBoolean((prop.getProperty("writeResult", "false")))
      val partionStrategy = prop.getProperty("partionStrategy", "")
      val hdrfLambda = Integer.parseInt(prop.getProperty("hdrfLambda", "1"))
      val ftime = new File(folder + timeFile)
      val bwtime = new BufferedWriter(new FileWriter(ftime))
      var mypartitioner = new MyPartitionStrategy()
      val globalstats = new GlobalStats(numPartitions, hdrfLambda)

      if (!tmpfolder.equals(""))
        sc.setCheckpointDir(tmpfolder)
      try {
        val appid = sc.applicationId
        var monitorShuffleData=false
        var masterURL="localhost"
       if(args.length>1){
         monitorShuffleData=true
         masterURL=args(1)
       }
        val url = s"""http://$masterURL:4040/api/v1/applications/$appid/stages"""
        println(url)
        val itteration = distance - 1
        var line = ""
        var output = new StringBuilder
        var node1 = 0L
        var node2 = 0L
        var time = 0L
        var isFirst = true
        //     var inputVertexArray: ArrayBuilder[(Long, (Long, NodeApprox))] = ArrayBuilder.make()
        var inputEdgeArray: ArrayBuilder[Edge[Long]] = ArrayBuilder.make()
        var users: RDD[(VertexId, (Long, NodeApprox))] = null
        var relationships: RDD[Edge[Long]] = null
        var graph: Graph[NodeApprox, Long] = null
        var total = 0
        var count = 0
        var edges = collection.mutable.Map[(Long, Long), Long]()
        //   var nodes = collection.mutable.Set[Long]()
        val nodeneighbours = collection.mutable.Map[Long, collection.mutable.Set[Long]]()
        //  var allnodes = collection.mutable.Map[Long, Int]()
        var nodeactivity = collection.mutable.Map[Long, Int]()
        var msgs: ArrayBuilder[PropogationObjectApprox] = ArrayBuilder.make()
        var startime = new Date().getTime
        var startimeTotal = new Date().getTime
        var partitionlookup = sc.broadcast(globalstats.edgepartitionsummary)
        val defaultNode = new NodeApprox(distance, num_buckets)
        //   var degree = sc.broadcast(allnodes)
        for (line <- Source.fromFile(folder + inputfile).getLines()) {
          val tmp = line.split(",")

          count = count + 1
          total = total + 1
          edges.put((tmp(0).toLong, tmp(1).toLong), tmp(2).toLong)

          nodeactivity.put(tmp(0).toLong, nodeactivity.getOrElse(tmp(0).toLong, 0) + 1)
          nodeactivity.put(tmp(1).toLong, nodeactivity.getOrElse(tmp(1).toLong, 0) + 1)

          if (count == batch) {
            process
          } //end of if

        } // end of for loop
        if (count != 0) {

          process
        }
        def process {

          count = 0

          //creating edge RDD from input
          for (((node1, node2), time) <- edges.seq.iterator) {

            inputEdgeArray.+=(Edge(node1, node2, time), Edge(node2, node1, time))

            msgs.+=((new PropogationObjectApprox(node1, node2, null, time, 0)), (new PropogationObjectApprox(node2, node1, null, time, 0)))

          }

          if (isFirst) {

            //          println("initialvertex : " + inputVertexArray.length + " : " + nodes.size)
            //          println("initialedge : " + inputEdgeArray.length)
            //            users = sc.parallelize(inputVertexArray.result(), numPartitions).partitionBy(new HashPartitioner(numPartitions)).setName("User RDD")

            relationships = sc.parallelize(inputEdgeArray.result(), numPartitions).cache().setName("Relationship RDD")

            if (storage) {
              graph = Graph.fromEdges(relationships, defaultNode, StorageLevel.MEMORY_ONLY_SER, StorageLevel.MEMORY_ONLY_SER)
            } else {
              graph = Graph.fromEdges(relationships, defaultNode)
            }
            graph.vertices.setName("g vertex")
            graph.edges.setName("g edges")

            isFirst = false

          } else {

            val newgraph = graph
            val oldusers = newgraph.vertices

            val newrelationships: RDD[Edge[Long]] = sc.parallelize(inputEdgeArray.result())
            val newedges = sc.broadcast(edges.keySet)
            //creating new relationship rdd by removing existing relationships from graph if the edge is existing 
            val oldedges = newgraph.edges.filter(x => {
              if (newedges.value.contains((x.srcId, x.dstId))) {
                false
              } else {
                true
              }
            })
            println(oldedges.partitioner)

            relationships = oldedges.union(newrelationships).cache().setName("updated edge RDD")
            //      graph.unpersist(false)

            if (storage) {
              graph = Graph(oldusers, relationships, new NodeApprox(distance, num_buckets), StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
            } else {
              graph = Graph(oldusers, relationships, new NodeApprox(distance, num_buckets))
            }
            graph.vertices.setName("gu vertex")
            graph.edges.setName("gu edges")

            newrelationships.unpersist(false);

          } //end of else

          //        graph = graph.pregel((0, msgs), distance, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
          // graph = PregelCorrected(graph, (0, msgs), itteration, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
          val newgraph = graph
          //updating degree for all nodes based on the edge batch
          edges.foreach { x =>
            {
              var temp = nodeneighbours.getOrElse(x._1._1, collection.mutable.Set[Long]())
              temp.add(x._1._2)
              nodeneighbours.put(x._1._1, temp)
              temp = nodeneighbours.getOrElse(x._1._2, collection.mutable.Set[Long]())
              temp.add(x._1._1)
              nodeneighbours.put(x._1._2, temp)
              //              temp = nodeneighbours.getOrElse(x._1._2, collection.mutable.Set[Long]())
              //              temp.add(x._1._1)
              //              nodeneighbours.put(x._1._2, temp)
              if (partionStrategy.equals("HDRF")) {
                globalstats.nodedegree.put(x._1._1, nodeneighbours.getOrElse(x._1._1, collection.mutable.Set[Long]()).size)
                globalstats.nodedegree.put(x._1._2, nodeneighbours.getOrElse(x._1._2, collection.mutable.Set[Long]()).size)
                globalstats.updatePartition(x._1._1, x._1._2, numPartitions)
              }
            }
          }

          //          val neighbourhoodProfile = graph.vertices.map(x => {
          //            (x._1, x._2.getNodeSummary.estimate())
          //          }).collect().map(f => {
          //            f._1 -> f._2
          //          }).toMap
          if (partionStrategy.equals("HDRF")) {
            partitionlookup = sc.broadcast(globalstats.edgepartitionsummary)
            mypartitioner = new MyPartitionStrategy(partitionlookup.value, nodeneighbours.map(f => (f._1, f._2.size)))
          } else if (partionStrategy.equals("DBH")) {

            val degree = sc.broadcast(nodeneighbours.map(f => (f._1, f._2.size)))
            mypartitioner = new MyPartitionStrategy(degree.value)
          } else if (partionStrategy.equals("ABH")) {
            val activity = sc.broadcast(nodeactivity)
            mypartitioner = new MyPartitionStrategy(activity.value)
          } else if (partionStrategy.equals("NPH")) {
            //            val neighbourhoodsize = sc.broadcast(neighbourhoodProfile)
            //            mypartitioner = new MyPartitionStrategy(null, null, neighbourhoodsize.value)
          }
      //    println("graph partioner before: " + graph.edges.partitioner)
          if (!partionStrategy.equals(""))
            graph = graph.partitionBy(mypartitioner.fromString(partionStrategy), numPartitions)
          graph.cache()

          if (!tmpfolder.equals("")) {
            if (!graph.isCheckpointed)
              graph.checkpoint()
          }
          graph.vertices.cache()
          graph.edges.cache()

          graph = Pregel(graph, (0, msgs.result()), itteration, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
       //   println("graph partioner after: " + graph.edges.partitioner)
          //          graph.edges.count()
          //          newgraph.unpersist(false)

          edges = edges.empty
          inputEdgeArray.clear()
          msgs.clear()

          relationships.unpersist(blocking = false)
          //            logger.info("Done: " + total + " at : " + new Date())
          println("Done: " + total + " at : " + new Date())
          bwtime.write(total + "," + (new Date().getTime - startime) + "\n")
          bwtime.flush()
          startime = new Date().getTime

        }
        //        logger.info("Completed in time : " + (new Date().getTime - startime))
        println("Completed in time : " + (new Date().getTime - startimeTotal))

        /*
     * Print the output
     * 
     */
        if (writeResult) {
          graph.vertices.collect.foreach {
            case (vertexId, original_value) => {
              //        println("node summary for " + value + " : " + original_value.getNodeSummary.estimate())
              output.append(vertexId + "," + original_value.getNodeSummary.estimate() + "\n")
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
      case e: Exception => {
        //        logger.error(e)
        e.printStackTrace()
      }

    }

  }

  def vertexProgram(id: VertexId, value: NodeApprox, msgSum: (Int, Array[PropogationObjectApprox])): NodeApprox = {
    val nodeid = id
    //if inital msg
    if (msgSum._1 == 0) {
      //reset the variables to clear last round data 
      value.setCurrentSuperStep(msgSum._1)
      value.setIschanged(false)
      for (propObj <- msgSum._2) {
        //check if the current node and the target node is same 
        if (propObj.getTargetNode == nodeid) {
          value.getDistanceWiseSummaries.get(0).add(propObj.getSourceNode, propObj.getTimestamp)

          value.setIschanged(true)
        }
      }
    } else {
      //not initial msg

      var targetSketch = value.getDistanceWiseSummaries.get(msgSum._1)
      for (propObj <- msgSum._2) {
        if (propObj.getTargetNode == nodeid) {

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
            value.setIschanged(true)
          }
        }
      }

      value.setCurrentSuperStep(msgSum._1)

    }
    value
  }
  def messageCombiner(msg1: (Int, Array[PropogationObjectApprox]), msg2: (Int, Array[PropogationObjectApprox])): (Int, Array[PropogationObjectApprox]) = (msg1._1, msg1._2 ++ msg2._2)

  def sendMessage(triplet: EdgeTriplet[NodeApprox, Long]): Iterator[(VertexId, (Int, Array[PropogationObjectApprox]))] = {

    val sourceVertex = triplet.srcAttr
    var propogationObj = new PropogationObjectApprox(triplet.dstId, triplet.srcId,
      sourceVertex.getDistanceWiseSummaries.get(sourceVertex.getCurrentSuperStep), triplet.attr, sourceVertex.getCurrentSuperStep + 1)

    if (!sourceVertex.isIschanged)
      Iterator.empty
    else
      Iterator((triplet.dstId, (sourceVertex.getCurrentSuperStep + 1, Array(propogationObj))))
  }

}