package com.ulb.code.wit.main

/**
 * @author Rohit
 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import com.ulb.code.wit.util.NodeExact
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
import scala.collection.mutable.ArrayBuilder
import org.json4s.DefaultFormats
import scala.collection.mutable.HashSet
object TestGraphExact {

  val logger = Logger.getLogger(getClass().getName());
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
      //      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      conf.registerKryoClasses(Array(classOf[NodeApprox], classOf[SlidingHLL], classOf[Element], classOf[PropogationObjectApprox], classOf[BucketAndHash]))
      //      conf.set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops")

      if (mode.equals("local")) {
        conf.setMaster("local[*]")
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
      val batch = Integer.parseInt(prop.getProperty("batch", "1000"))
      val writeResult = Boolean.parseBoolean((prop.getProperty("writeResult", "false")))
      val getReplicationFactor = Boolean.parseBoolean((prop.getProperty("getReplicationFactor", "false")))
      val partionStrategy = prop.getProperty("partionStrategy", "")
      val hdrfLambda = (prop.getProperty("hdrfLambda", "1")).toDouble
      val ftime = new File(folder + timeFile)
      val bwtime = new BufferedWriter(new FileWriter(ftime))
      var mypartitioner = new MyPartitionStrategy()
      val globalstats = new GlobalStats(numPartitions, hdrfLambda)
      var bwshuffle: BufferedWriter = null
      if (!tmpfolder.equals(""))
        sc.setCheckpointDir(tmpfolder)
      try {
        val itteration = distance - 1
        var line = ""
        var output = new StringBuilder
        var node1 = 0L
        var node2 = 0L
        var time = 0L
        var isFirst = true
        var inputVertexArray: ArrayBuilder[(Long, NodeExact)] = ArrayBuilder.make()
        var inputEdgeArray: ArrayBuilder[Edge[Long]] = ArrayBuilder.make()
        var users: RDD[(VertexId, NodeExact)] = null
        var relationships: RDD[Edge[Long]] = null
        var graph: Graph[NodeExact, Long] = null
        var total = 0
        var count = 0
        var edges = collection.mutable.Map[(Long, Long), Long]()
        var nodes = collection.mutable.Set[Long]()
        val nodeneighbours = collection.mutable.Map[Long, collection.mutable.Set[Long]]()
        //  var allnodes = collection.mutable.Map[Long, Int]()
        var nodeactivity = collection.mutable.Map[Long, Int]()
        var msgs: collection.mutable.Set[(Long, Long, Long)] = collection.mutable.Set[(Long, Long, Long)]()
        var startime = new Date().getTime
        var startimeTotal = new Date().getTime
        var partitionlookup = sc.broadcast(globalstats.edgepartitionsummary)
        val appid = sc.applicationId
        var monitorShuffleData = false
        var masterURL = "localhost"
        val replication = sc.longAccumulator("MyreplicationFactor")

        if (args.length > 1) {
          monitorShuffleData = true
          masterURL = args(1)

          val fshuffle = new File(folder + "Memory_" + partionStrategy + "_" + numPartitions + "_" + outputFile)
          bwshuffle = new BufferedWriter(new FileWriter(fshuffle))
        }
        val url = s"""http://$masterURL:4040/api/v1/applications/$appid/stages"""
        println(url)
        implicit val formats = DefaultFormats

        //   var degree = sc.broadcast(allnodes)
        for (line <- Source.fromFile(folder + inputfile).getLines()) {
          val tmp = line.split(",")

          count = count + 1
          total = total + 1
          edges.put((tmp(0).toLong, tmp(1).toLong), tmp(2).toLong)
          nodes.add(tmp(0).toLong)
          nodes.add(tmp(1).toLong)
          nodeactivity.put(tmp(0).toLong, nodeactivity.getOrElse(tmp(0).toLong, 0) + 1)
          nodeactivity.put(tmp(1).toLong, nodeactivity.getOrElse(tmp(1).toLong, 0) + 1)

          if (count == batch) {
            if (edges.size - count != 0) {
              println("distinct edges: " + edges.size)
              println("total edges: " + count)
            }
            process
            count = 0
          } //end of if

        } // end of for loop
        if (count != 0) {
          if (edges.size - count != 0) {
            println("distinct edges: " + edges.size)
            println("total edges: " + count)
          }
          process
        }
        def process {

          //creating vertext RDD from input 
          for (node1 <- nodes.iterator) {
            inputVertexArray.+=((node1, new NodeExact(node1, new Array[Long2LongArrayMap](distance))))

          }
          //creating vertex RDD from input
          for (((node1, node2), time) <- edges.seq.iterator) {

            inputEdgeArray.+=(Edge(node1, node2, time), Edge(node2, node1, time))

            msgs.+=((node1, node2, time), (node2, node1, time))

          }
          val defaultNode = new NodeExact(-1, new Array[Long2LongArrayMap](distance))
          if (isFirst) {

            //          println("initialvertex : " + inputVertexArray.length + " : " + nodes.size)
            //          println("initialedge : " + inputEdgeArray.length)
            users = sc.parallelize(inputVertexArray.result(), numPartitions).partitionBy(new HashPartitioner(numPartitions)).setName("User RDD")

            relationships = sc.parallelize(inputEdgeArray.result(), numPartitions).cache().setName("Relationship RDD")

            if (storage) {

              graph = Graph(users, relationships, defaultNode, StorageLevel.MEMORY_ONLY_SER, StorageLevel.MEMORY_ONLY_SER)
            } else {
              graph = Graph(users, relationships, defaultNode)
            }
            graph.vertices.setName("g vertex")
            graph.edges.setName("g edges")

            isFirst = false

          } else {

            var newusers: RDD[(VertexId, NodeExact)] = sc.parallelize(inputVertexArray.result(), numPartitions).filter(x => {
              if (nodeneighbours.contains(x._1))
                false
              else
                true

            }).partitionBy(new HashPartitioner(numPartitions)).cache()
            newusers.count() //materialize the new users

            val newgraph = graph
            val oldusers = newgraph.vertices
            //creating new user rdd by removing existing users in graph from the list of new users

            val newrelationships: RDD[Edge[Long]] = sc.parallelize(inputEdgeArray.result(), numPartitions)

            users = oldusers.union(newusers).coalesce(numPartitions, false).cache().setName("updated Users RDD")
            //creating new relationship rdd by removing existing relationships from graph if the edge is existing 

            relationships = newgraph.edges.union(newrelationships).coalesce(numPartitions, false).cache().setName("updated edge RDD")
            //      graph.unpersist(false)
            users = users.map(x => {
              x._2.currentsuperstep = 0
              x
            })
            if (storage) {
              graph = Graph(users, relationships, defaultNode, StorageLevel.MEMORY_ONLY_SER, StorageLevel.MEMORY_ONLY_SER)
            } else {
              graph = Graph(users, relationships, defaultNode)
            }

            graph.vertices.setName("gu vertex")
            graph.edges.setName("gu edges")

            newrelationships.unpersist(false);
            newusers.unpersist(false)

            newgraph.unpersist(false)
          } //end of else

          val newgraph = graph
          //          graph.pageRank(100, .01).vertices.take(10)
          //updating degree for all nodes based on the edge batch
          edges.foreach {
            case ((src, dst), t) =>
              {
                var temp = nodeneighbours.getOrElse(src, collection.mutable.Set[Long]())
                temp.add(dst)
                nodeneighbours.put(src, temp)
                temp = nodeneighbours.getOrElse(dst, collection.mutable.Set[Long]())
                temp.add(src)
                nodeneighbours.put(dst, temp)
                if (partionStrategy.equals("HDRF")) {
                  globalstats.nodedegree.put(src, nodeneighbours.getOrElse(src, collection.mutable.Set[Long]()).size)
                  globalstats.nodedegree.put(dst, nodeneighbours.getOrElse(dst, collection.mutable.Set[Long]()).size)
                  globalstats.updatePartitionHDRF(src, dst, numPartitions)
                }
              }
          }

          if (partionStrategy.equals("HDRF")) {
            partitionlookup = sc.broadcast(globalstats.edgepartitionsummary)
            mypartitioner = new MyPartitionStrategy(partitionlookup.value, nodeneighbours.map(f => (f._1, f._2.size)))
          } else if (partionStrategy.equals("DBH")) {

            val degree = sc.broadcast(nodeneighbours.map(f => (f._1, f._2.size)))
            mypartitioner = new MyPartitionStrategy(degree.value)
          } else if (partionStrategy.equals("ABH")) {
            val activity = sc.broadcast(nodeactivity)
            mypartitioner = new MyPartitionStrategy(activity.value)
          } else if (partionStrategy.equals("ReverseABH")) {
            val activity = sc.broadcast(nodeactivity)
            mypartitioner = new MyPartitionStrategy(activity.value)
          } else if (partionStrategy.equals("NPH")) {
            val neighbourhoodProfile = graph.vertices.map(x => {
              (x._2.node, x._2.getsummary(0L).toLong)
            }).collect().map(f => {
              f._1 -> f._2
            }).toMap
            val neighbourhoodsize = sc.broadcast(neighbourhoodProfile)
            mypartitioner = new MyPartitionStrategy(null, null, neighbourhoodsize.value,null,null)
          }

          if (!partionStrategy.equals(""))
            graph = graph.partitionBy(mypartitioner.fromString(partionStrategy), numPartitions)
          graph.cache()

          if (getReplicationFactor) {

            graph.edges.foreachPartition(x => {
              val nodes: HashSet[Long] = HashSet.empty
              for (edge <- x) {

                nodes.add(edge.dstId)
                nodes.add(edge.srcId)
              }
              replication.add(nodes.size)
            })
          }

          if (!tmpfolder.equals("")) {
            if (!graph.isCheckpointed)
              graph.checkpoint()
          }
          graph.vertices.cache()
          graph.edges.cache()
//                              graph = Pregel(graph, (0, msgs.result()), itteration, EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)
          graph = Pregel(graph, (0, msgs.result()), itteration, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
       
          //          graph.edges.count()
          //          newgraph.unpersist(false)

          nodes = nodes.empty
          edges = edges.empty
          inputEdgeArray.clear()
          msgs.clear()
          inputVertexArray.clear()

          users.unpersist(blocking = false)
          relationships.unpersist(blocking = false)
          //            logger.info("Done: " + total + " at : " + new Date())
          val rf: Double = BigDecimal(replication.value.toDouble / nodeactivity.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          println("Done: " + total + " at : " + new Date() + " RF: " + rf + " total sum: " + replication.sum / nodeactivity.size)
          bwtime.write(total + "," + (new Date().getTime - startime) + "," + rf + "\n")
          bwtime.flush()
          replication.reset()
          if (monitorShuffleData) {
            //            val json = fromURL(url).mkString
            //            val stages: List[SparkStage] = parse(json).extract[List[SparkStage]].filter { _.name.equals("mapPartitions at GraphImpl.scala:207") }
            //            //            println("stages count: " + stages.size)
            //            //             println("stages count: " + stages.map(_.shuffleReadBytes).sum / (1024 * 1024))
            //            bwshuffle.write(total + "," + stages.map(_.shuffleWriteBytes).sum / (1024 * 1024) + "," + stages.map(_.shuffleReadBytes).sum / (1024 * 1024) + "\n")
            //            bwshuffle.flush()
          }
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
            case (vertexId, node) => {
              //        println("node summary for " + value + " : " + original_value.getNodeSummary.estimate())
              output.append(vertexId + "," + node.getsummary(0L) + "\n")
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
        if (bwshuffle != null) {
          bwshuffle.flush()
          bwshuffle.close()
        }
      }
    } catch {
      case e: Exception => {
        //        logger.error(e)
        e.printStackTrace()
      }

    }

  }

  def vertexProgram(id: VertexId, value: NodeExact, msgSum: (Int, collection.mutable.Set[(Long, Long, Long)])): NodeExact = {

    value.ischanged = false
    value.currentsuperstep = msgSum._1

    val inspectionset = Set(3460l, 3881l, 3602l, 3761l, 3829l, 3466l, 3915l, 3554l)
    if (inspectionset.contains(id)) {
      //      println(id)
    }
    //if inital msg
    if (msgSum._1 == 0) {
      //reset the variables to clear last round data 

      for (x <- msgSum._2) {
        //check if the current node and the target node is same 
        if (x._1 == value.node) {
          if (value.summary(0) != null) {
            value.summary(0).put(x._2, x._3)

            value.ischanged = true
          } else {
            val temp = new Long2LongArrayMap()
            temp.put(x._2, x._3)
            value.summary.update(0, temp)
            value.ischanged = true
          }
        }
      }
    } else {
      if (msgSum._2.exists(_._1 == value.node)) {

        msgSum._2.foreach({
          x =>
            if (x._1 == value.node) {
              //need to check if present in lower level
              var skipduetolower = false;
              for (i <- 0 to msgSum._1 - 1) {
                if (value.summary(i) == null) {
                  skipduetolower = true
                } else if (value.summary(i).get(x._2) >= x._3) {
                  skipduetolower = true
                }
              }
              if (!skipduetolower) {
                if (value.summary(msgSum._1) != null) {
                  if (value.summary(msgSum._1).containsKey(x._2)) {
                    if (value.summary(msgSum._1).get(x._2) < x._3) {

                      value.summary(msgSum._1).put(x._2, x._3)
                      value.ischanged = true

                      //need to remove from upper if present at later horizon
                      for (i <- msgSum._1 + 1 to value.summary.length - 1) {
                        if (value.summary(i) != null) {
                          if (value.summary(i).get(x._2) <= x._3) {
                            value.summary(i).remove(x._2)
                          }
                        }
                      }
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
            }
        })

      }
    }
    value
  }
  def messageCombiner(msg1: (Int, collection.mutable.Set[(Long, Long, Long)]), msg2: (Int, collection.mutable.Set[(Long, Long, Long)])): (Int, collection.mutable.Set[(Long, Long, Long)]) = {
    val inspectionset = Set(3460l, 3881l, 3602l, 3761l, 3829l, 3466l, 3915l, 3554l)
    if (msg1._1 != msg2._1) {
      for (x <- msg1._2) {
        //check if the current node and the target node is same 
        if (inspectionset.contains(x._1)) {
          //          println(msg1._1)
        }
      }
      for (x <- msg2._2) {
        //check if the current node and the target node is same 
        if (inspectionset.contains(x._1)) {
          //          println(msg1._1)
        }
      }
      //      println(msg1._1)
    }
    (msg1._1, msg1._2.union(msg2._2))

  }

  def sendMessage(triplet: EdgeTriplet[NodeExact, Long]): Iterator[(VertexId, (Int, collection.mutable.Set[(Long, Long, Long)]))] = {
    val inspectionset = Set(3460l, 3881l, 3602l, 3761l, 3829l, 3466l, 3915l, 3554l)
    if (inspectionset.contains(triplet.srcId)) {
//      println(triplet.srcId + "->" + triplet.dstId)
    } else if (inspectionset.contains(triplet.dstId)) {
//      println(triplet.srcId + "->" + triplet.dstId)
    }
    //    if (triplet.srcAttr.ischanged) {
    var msg: collection.mutable.Set[(Long, Long, Long)] = null

    val sum = triplet.srcAttr.summary(triplet.srcAttr.currentsuperstep)
    if (sum != null) {
      val tempItertator = sum.keySet().iterator()
      while (tempItertator.hasNext()) {
        val value = tempItertator.nextLong()
        val time = sum.get(value)
        if (triplet.dstId != value) {
          if (msg == null) {
            msg = collection.mutable.Set((triplet.dstId, value, Math.min(time, triplet.attr)))
          } else {
            msg.add((triplet.dstId, value, Math.min(time, triplet.attr)))
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