package com.ulb.code.wit.main

/**
 * @author Rohit
 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD

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
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import java.lang.Boolean
import scala.collection.mutable.ArrayBuilder
import org.json4s.DefaultFormats
import scala.collection.mutable.HashSet
import com.ulb.code.wit.util.NewNodeExact
import scala.io.Source.fromURL
import com.ulb.code.wit.util.SparkAppStats.SparkStage
object NewTestGraphExact {

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
      val conf = new SparkConf().setAppName("NeighbourHoodProfileExact")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.registerKryoClasses(Array(classOf[NewNodeExact]))
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
      val num_buckets = Integer.parseInt(prop.getProperty("number_of_bucket", "256"))
      val distance = Integer.parseInt(prop.getProperty("distance", "3"))
      val storage = Boolean.parseBoolean((prop.getProperty("storage", "false")))
      val batch = Integer.parseInt(prop.getProperty("batch", "1000"))
      val writeResult = Boolean.parseBoolean((prop.getProperty("writeResult", "false")))
      val getReplicationFactor = Boolean.parseBoolean((prop.getProperty("getReplicationFactor", "false")))
      val partionStrategy = prop.getProperty("partionStrategy", "")
      val hdrfLambda = (prop.getProperty("hdrfLambda", "1")).toDouble
      val ftime = new File(folder + "Time_" + inputfile.replace(".csv","") + "_" + distance + "_" + partionStrategy + "_" + numPartitions + ".csv")
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
        var inputVertexArray: ArrayBuilder[(Long, NewNodeExact)] = ArrayBuilder.make()
        var inputEdgeArray: ArrayBuilder[Edge[Long]] = ArrayBuilder.make()
        var users: RDD[(VertexId, NewNodeExact)] = null
        var relationships: RDD[Edge[Long]] = null
        var graph: Graph[NewNodeExact, Long] = null
        var total = 0
        var count = 0
        var edges = collection.mutable.Map[(Long, Long), Long]()
        var nodes = collection.mutable.Set[Long]()
        var oldnodesAttribute: Map[VertexId, NewNodeExact] = Map[Long, NewNodeExact]()
        val nodeneighbours = collection.mutable.Map[Long, collection.mutable.Set[Long]]()
        //  var allnodes = collection.mutable.Map[Long, Int]()
        var nodeactivity = collection.mutable.Map[Long, Int]()
        var msgs: collection.mutable.Set[(Long, Long, Long, Int)] = collection.mutable.Set[(Long, Long, Long, Int)]()
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

          val fshuffle = new File(folder + "Memory_" + inputfile.replace(".csv","") + "_" + distance + "_" + partionStrategy + "_" + numPartitions + ".csv")
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
          if (tmp(0).toLong != tmp(1).toLong) { //avoid self loop
            edges.put((tmp(0).toLong, tmp(1).toLong), tmp(2).toLong)
            nodes.add(tmp(0).toLong)
            nodes.add(tmp(1).toLong)
            nodeactivity.put(tmp(0).toLong, nodeactivity.getOrElse(tmp(0).toLong, 0) + 1)
            nodeactivity.put(tmp(1).toLong, nodeactivity.getOrElse(tmp(1).toLong, 0) + 1)
          }
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
        def generateInitialMsg(src: Long, dst: Long) {
          //          if (src == 176 && dst == 86) {
          //            println(dst)
          //          }
          if (nodeneighbours.contains(src)) {
            if (oldnodesAttribute.contains(src)) {
              val nodeAttribute = oldnodesAttribute.get(src).get
              var iterator = nodeAttribute.summary(0).iterator
              var item: (Long, Long) = null
              for (i <- 1 to distance - 2) {
                if (nodeAttribute.summary(i) != null) {
                  iterator = nodeAttribute.summary(i).iterator
                  while (iterator.hasNext) {
                    item = iterator.next()
                    if (dst != item._1) //to avoid self addition
                      msgs.+=((dst, item._1, item._2, i + 1))
                  }
                }
              }
            }
          }
        }
        def process {
          //creating vertext RDD from input 
          for (node1 <- nodes.iterator) {
            inputVertexArray.+=((node1, new NewNodeExact(node1, new Array[scala.collection.immutable.HashMap[Long, Long]](distance))))

          }
          if (!isFirst) {
            oldnodesAttribute = graph.vertices.filter(x => {
              if (nodes.contains(x._1)) {
                true
              } else {
                false
              }

            }).collect().toMap
          }
          //creating edge RDD and initial msg from input
          for (((node1, node2), time) <- edges.seq.iterator) {

            inputEdgeArray.+=(Edge(node1, node2, time), Edge(node2, node1, time))

            msgs.+=((node1, node2, time, 0), (node2, node1, time, 0))
            //check if the node is already has a summary in that case generate msg for all distance to be merged
            generateInitialMsg(node1, node2)
            generateInitialMsg(node2, node1)
          }

          val defaultNode = new NewNodeExact(-1, new Array[scala.collection.immutable.HashMap[Long, Long]](distance))
          if (isFirst) {

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

            var newusers: RDD[(VertexId, NewNodeExact)] = sc.parallelize(inputVertexArray.result(), numPartitions).filter(x => {
              if (nodeneighbours.contains(x._1))
                false
              else
                true

            }).partitionBy(new HashPartitioner(numPartitions)).cache()
            newusers.count() //materialize the new users

            val oldusers = graph.vertices
            //creating new user rdd by removing existing users in graph from the list of new users

            val newrelationships: RDD[Edge[Long]] = sc.parallelize(inputEdgeArray.result(), numPartitions)

            users = oldusers.union(newusers).coalesce(numPartitions, false).cache().setName("updated Users RDD")
            //creating new relationship rdd by removing existing relationships from graph if the edge is existing 

            relationships = graph.edges.union(newrelationships).coalesce(numPartitions, false).cache().setName("updated edge RDD")
            //      graph.unpersist(false)

            if (storage) {
              graph = Graph(users, relationships, defaultNode, StorageLevel.MEMORY_ONLY_SER, StorageLevel.MEMORY_ONLY_SER)
            } else {
              graph = Graph(users, relationships, defaultNode)
            }

            graph.vertices.setName("gu vertex")
            graph.edges.setName("gu edges")

            newrelationships.unpersist(false);
            newusers.unpersist(false)

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
            mypartitioner = new MyPartitionStrategy(null, null, neighbourhoodsize.value, null)
          } else if (partionStrategy.equals("UBH")) {
            val nodeupdatecount = graph.vertices.map(x => {
              (x._1, x._2.updateCount)
            }).collect().map(f => {
              f._1 -> f._2
            }).toMap
            val degree = nodeneighbours.map(f => (f._1, f._2.size))
            mypartitioner = new MyPartitionStrategy(null, degree, null, nodeupdatecount)
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

          //                              graph = Pregel(graph, (0, msgs.result()), itteration, EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)
          graph = Pregel(graph, (0, msgs.result()), itteration, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
          graph.vertices.cache()
          graph.edges.cache()
          graph.cache()
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
            val json = fromURL(url).mkString
            val stages: List[SparkStage] = parse(json).extract[List[SparkStage]].filter { _.name.equals("mapPartitions at GraphImpl.scala:207") }
            //            println("stages count: " + stages.size)
            //             println("stages count: " + stages.map(_.shuffleReadBytes).sum / (1024 * 1024))
            bwshuffle.write(total + "," + stages.map(_.shuffleWriteBytes).sum / (1024 * 1024) + "," + stages.map(_.shuffleReadBytes).sum / (1024 * 1024) + "\n")
            bwshuffle.flush()
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
              output.append(vertexId + "," + node.getsummary(0L) + "," + node.updateCount + "," + nodeneighbours.get(vertexId).get.size + "\n")
              if (vertexId == 86l) {
                println
              }
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

  def vertexProgram(id: VertexId, value: NewNodeExact, msgSum: (Int, collection.mutable.Set[(Long, Long, Long, Int)])): NewNodeExact = {

    var changed = false
    var superstep = msgSum._1
    var summary = value.summary
    var updatecnt = value.updateCount
    val filteredmsg = msgSum._2.filter(p => {
      if (p._1 == value.node) {
        true
      } else {
        false
      }
    })
    //    val inspectionset = Set(3460l, 3881l, 3602l, 3761l, 3829l, 3466l, 3915l, 3554l)
//        val inspectionset = Set(86l)
//        if (inspectionset.contains(id)) {
//          println(id + " " + msgSum._1)
//        }
    //if inital msg
    if (msgSum._1 == 0) {
      val newmsg: scala.collection.mutable.Set[(Int, Long, Long)] = scala.collection.mutable.Set[(Int, Long, Long)]()

      for ((src, dst, time, distance) <- filteredmsg) {

        //check if the current node and the target node is same 

        if (src == value.node) {
          //found edge with src and target node for 1st msg 
          if (distance == 0) { //if its distance 0 just update
            if (value.summary(0) != null) {
              summary.update(0, summary(0).+(dst -> time))

              changed = true
            } else {
              val temp: collection.immutable.HashMap[Long, Long] = collection.immutable.HashMap(dst -> time)
              summary.update(0, temp)
              changed = true
            }

          } else {
            newmsg.+=((distance, dst, time))

          }
        }
      }
      val sortedmsg = newmsg.toList.sortBy(f => f._1)
      for ((d, nodetoAdd, timetoAdd) <- sortedmsg) {
        addSummary(d, nodetoAdd, timetoAdd)
      }
    } else {
      if (filteredmsg.size > 0) {

        filteredmsg.foreach({
          x =>
            if (x._1 == value.node) {
              addSummary(msgSum._1, x._2, x._3)
            }
        })

      }
    }

    def addSummary(d: Int, nodetoAdd: Long, timetoAdd: Long) {
      //need to check if present in lower level
      var skipduetolower = false;
      for (i <- 0 to d - 1) {
        if (summary(i) == null) {
          //          skipduetolower = true
        } else if (summary(i).get(nodetoAdd).getOrElse(0l) >= timetoAdd) {
          skipduetolower = true
        }
      }
      if (!skipduetolower) {
        if (summary(d) != null) {
          if (summary(d).contains(nodetoAdd)) {
            if (summary(d).get(nodetoAdd).get < timetoAdd) {

              summary.update(d, summary(d).+(nodetoAdd -> timetoAdd))
              changed = true

              //need to remove from upper if present at later horizon
              for (i <- d + 1 to summary.length - 1) {
                if (summary(i) != null) {
                  if (summary(i).contains(nodetoAdd)) {
                    if (summary(i).get(nodetoAdd).get <= timetoAdd) {
                      summary.update(i, summary(i).-(nodetoAdd))
                    }
                  }
                }
              }
            }
          } else {
            summary.update(d, summary(d).+(nodetoAdd -> timetoAdd))
            changed = true
          }
        } else {

          val temp: collection.immutable.HashMap[Long, Long] = collection.immutable.HashMap(nodetoAdd -> timetoAdd)

          summary.update(d, temp)
          changed = true
        }
      }
    }
    if (changed)
      updatecnt = updatecnt + 1
    new NewNodeExact(value.node, summary, superstep, changed, updatecnt)

  }
  def messageCombiner(msg1: (Int, collection.mutable.Set[(Long, Long, Long, Int)]), msg2: (Int, collection.mutable.Set[(Long, Long, Long, Int)])): (Int, collection.mutable.Set[(Long, Long, Long, Int)]) = {
    //    val inspectionset = Set(3460l, 3881l, 3602l, 3761l, 3829l, 3466l, 3915l, 3554l)
    //    if (msg1._1 != msg2._1) {
    //      for (x <- msg1._2) {
    //        //check if the current node and the target node is same 
    //        if (inspectionset.contains(x._1)) {
    //          //          println(msg1._1)
    //        }
    //      }
    //      for (x <- msg2._2) {
    //        //check if the current node and the target node is same 
    //        if (inspectionset.contains(x._1)) {
    //          //          println(msg1._1)
    //        }
    //      }
    //      //      println(msg1._1)
    //    }
    (msg1._1, msg1._2.union(msg2._2))

  }

  def sendMessage(triplet: EdgeTriplet[NewNodeExact, Long]): Iterator[(VertexId, (Int, collection.mutable.Set[(Long, Long, Long, Int)]))] = {
    //    val inspectionset = Set(3460l, 3881l, 3602l, 3761l, 3829l, 3466l, 3915l, 3554l)
    //    if (inspectionset.contains(triplet.srcId)) {
    //      //      println(triplet.srcId + "->" + triplet.dstId)
    //    } else if (inspectionset.contains(triplet.dstId)) {
    //      //      println(triplet.srcId + "->" + triplet.dstId)
    //    }

    var msg: collection.mutable.Set[(Long, Long, Long, Int)] = null
    if (triplet.srcAttr.ischanged) {
      val sum = triplet.srcAttr.summary(triplet.srcAttr.currentsuperstep)
      if (sum != null) {
        val tempItertator = sum.keysIterator
        while (tempItertator.hasNext) {
          val value = tempItertator.next()
          val time = sum.get(value).get
          if (triplet.dstId != value) {
            if (msg == null) {
              msg = collection.mutable.Set((triplet.dstId, value, Math.min(time, triplet.attr), triplet.srcAttr.currentsuperstep + 1))
            } else {
              msg.add((triplet.dstId, value, Math.min(time, triplet.attr), triplet.srcAttr.currentsuperstep + 1))
            }
          }
        }

        if (msg == null) {
          Iterator.empty
        } else
          Iterator((triplet.dstId, (triplet.srcAttr.currentsuperstep + 1, msg)))
      } else
        Iterator.empty
    } else
      Iterator.empty

  }

}