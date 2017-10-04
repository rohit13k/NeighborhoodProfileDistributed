package com.ulb.code.wit.main
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
import org.apache.spark.util.CollectionAccumulator
import scala.collection.mutable.ListBuffer
import com.ulb.code.wit.util.LoadStatsCalculator
import org.apache.spark.util.LongAccumulator
import scala.collection.immutable.List
import com.ulb.code.wit.util.SparkAppStats.SparkJobs
import org.apache.spark.graphx.MyAccumulator
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerStageCompleted
import com.ulb.code.wit.util.SparkAppStats.taskDetail

object GraphAlgos {
  val tol = 0.01
  val resetProb: Double = 0.15
  val logger = Logger.getLogger(getClass().getName());
  var prTime = 0l
  var totalPrTime = 0l
  def main(args: Array[String]) {
    val prop = new Properties()
    try {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      val configFile = args(0)
      prop.load(new FileInputStream(configFile))

      //      println("spark contest status: " + sc.isStopped)
      val numPartitions = Integer.parseInt(prop.getProperty("partition", "6"))
      var inputfile = prop.getProperty("inputfile", "facebook_reduced.csv")
      var partionStrategy = prop.getProperty("partionStrategy", "")
      val batch = Integer.parseInt(prop.getProperty("batch", "1000"))
      val PR_Itteration = if (prop.getProperty("PR_Itteration", "0").equals("0")) Int.MaxValue else prop.getProperty("PR_Itteration", "0").toInt
      val mergeProgDelay = Integer.parseInt(prop.getProperty("mergeProgDelay", "1"))
      val sendProgDelay = Integer.parseInt(prop.getProperty("sendProgDelay", "1"))
      val vertexProgDelay = Integer.parseInt(prop.getProperty("vertexProgDelay", "1"))
      val landmarkCount = Integer.parseInt(prop.getProperty("landmarkCount", "100"))
      val seedForlandMarkSelection = 484348255l
      var algo = prop.getProperty("algo", "PR")
      var outputFile = algo + "_" + prop.getProperty("outputFile", "facebook_reduced_estimate.csv")

      var jobId = ""
      if (args.length > 2) {
        //override the application properties parameter with command line parameters
        inputfile = args(2)
        partionStrategy = args(3)
        algo = args(5)
        outputFile = algo + "_" + inputfile.replace(".csv", "_estimate") + "_" + PR_Itteration + ".csv"

      }
      val mode = prop.getProperty("mode", "cluster")
      val conf = new SparkConf().setAppName("GraphAlgos-" + algo)
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.registerKryoClasses(Array(classOf[VertexPartitioner], classOf[MyPartitionStrategy]))

      if (mode.equals("local")) {
        conf.setMaster("local[*]")
      }

      val sc = new SparkContext(conf)
      if (args.length > 5) jobId = args(4) else jobId = new Date().getTime + ""
      val folder = prop.getProperty("folder", "./data/")
      val ofolder = folder + jobId + File.separator
      val oFolderPath = new File(ofolder)
      if (!oFolderPath.exists())
        oFolderPath.mkdir()
      val tmpfolder = prop.getProperty("tmpfolder", "")
      val makeObjectHeavy = Boolean.parseBoolean((prop.getProperty("makeObjectHeavy", "false")))

      val vertexPartition = Boolean.parseBoolean((prop.getProperty("useVertexPartitioner", "false")))
      val writeResult = Boolean.parseBoolean((prop.getProperty("writeResult", "false")))
      val fCounter = new File(ofolder + outputFile.replace(".csv", "_" + partionStrategy + "_CounterDetail.csv"))
      val bwCounter = new BufferedWriter(new FileWriter(fCounter))
      val bwCounterData = new StringBuffer
      var mypartitioner = new MyPartitionStrategy()
      var myVertexPartitioner = new VertexPartitioner(numPartitions)

      var bwshuffle: BufferedWriter = null
      if (!tmpfolder.equals(""))
        sc.setCheckpointDir(tmpfolder)
      try {
        println("input File:" + inputfile + " partitioner: " + partionStrategy)

        var line = ""
        var output = new StringBuilder
        var node1 = 0L
        var node2 = 0L
        var time = 0L
        var isFirst = true
        val heavyString = if (makeObjectHeavy) ConnectedComponentBatch.getData(folder) else ""
        var inputEdgeArray: ArrayBuilder[Edge[Long]] = ArrayBuilder.make()
        var inputVertexArray: ArrayBuilder[(Long, (Long, String))] = ArrayBuilder.make()
        var users: RDD[(Long, (Long, String))] = null
        var relationships: RDD[Edge[Long]] = null
        var graph: Graph[(Long, String), Long] = null
        var total = 0
        var count = 0
        var partitionDistribution = collection.mutable.Map[Int, String]()
        var edges = collection.mutable.Map[(Long, Long), Long]()
        //        var distinctedges = collection.mutable.Set[(Long, Long)]()
        var nodes = collection.mutable.Set[Long]()
        var oldnodesAttribute: Map[VertexId, NewNodeExact] = Map[Long, NewNodeExact]()
        val nodeneighbours = collection.mutable.Map[Long, collection.mutable.Set[Long]]()
        val degree = collection.mutable.Map[Long, collection.mutable.Set[Long]]()
        //  var allnodes = collection.mutable.Map[Long, Int]()
        var nodeReplicationCnt = collection.mutable.Map[Long, collection.mutable.Set[Int]]()
        //        var msgs: collection.mutable.Set[(Long, Long, Long, Int)] = collection.mutable.Set[(Long, Long, Long, Int)]()
        //        var msgs: List[(Long, Long, Long, Int)] = List[(Long, Long, Long, Int)]()
        var msgsList: ListBuffer[(Long, Long, Long, Int)] = ListBuffer[(Long, Long, Long, Int)]()
        var startime = new Date().getTime
        var startimeTotal = new Date().getTime

        val appid = sc.applicationId

        val monitorCounterData = true
        var masterURL = "localhost"
        //        var result: Array[(VertexId, Double)] = null

        val vertexProgramCounter = new MyAccumulator

        val mergeMsgProgramCounter = new MyAccumulator
        val sendMsgProgramCounter = new MyAccumulator
        // Then, register it into spark context:
        sc.register(vertexProgramCounter, "vertexProgramCounter")
        sc.register(mergeMsgProgramCounter, "mergeMsgProgramCounter")
        sc.register(sendMsgProgramCounter, "sendMsgProgramCounter")
        val replication = sc.longAccumulator("MyreplicationFactor")
        //        vertexProgramCounter = sc.longAccumulator("vertexProgramTime")

        //        val edgecount: CollectionAccumulator[String] = sc.collectionAccumulator("EdgeCount")
        val url = s"""http://$masterURL:4040/api/v1/applications/$appid/stages"""
        //        println(url)
        implicit val formats = DefaultFormats
        if (args.length > 1) {

          masterURL = args(1)

        }
        if (monitorCounterData) {
          bwCounterData.append("StageId,StageName,vpCounter,sendCounter,mergeCounter,executorRunTime,executorDeserializeTime,jvmGCTime,"
            + "remoteBytesRead,remoteBlocksFetched,fetchWaitTime,localBlocksFetched,localBytesRead,recordsRead,"
            + "bytesWritten,writeTime,recordsWritten,"
            + "inputbytesRead,inputRecordsRead,"
            + "outputBytesWritten,outputRecordsWritten,"
            + "stageTime \n")
          sc.addSparkListener(new SparkListener() {

            override def onStageCompleted(stageComplted: SparkListenerStageCompleted) {
              //              if (stageComplted.stageInfo.stageId > 12) {
              var name = ""
              if (stageComplted.stageInfo.name.equals("mapPartitions at VertexRDDImpl.scala:245")) {
                if (stageComplted.stageInfo.details.contains("org.apache.spark.graphx.impl.GraphImpl.mapVertices")) {
                  name = "mapVertices"
                } else if (stageComplted.stageInfo.details.contains("org.apache.spark.graphx.GraphOps.joinVertices")) {
                  name = "joinVertices"
                } else {
                  name = "shipVertexAttribute"
                }
              } else if (stageComplted.stageInfo.name.equals("reduce at VertexRDDImpl.scala:88")) {
                name = "reduceMsg"
              } else if (stageComplted.stageInfo.name.equals("mapPartitions at VertexRDD.scala:356")) {
                name = "createGraph"
              } else if (stageComplted.stageInfo.name.equals("mapPartitions at VertexRDDImpl.scala:249")) {
                name = "shipVertexIds"
              } else if (stageComplted.stageInfo.name.equals("mapPartitions at GraphImpl.scala:207")) {
                name = "sendMsg"
              } else {
                name = "unKnown"
              }

              if (name.equals("sendMsg")) {
                //GatherSum is finishing

                bwCounterData.append(stageComplted.stageInfo.stageId + ","
                  + name + ","
                  + ","
                  + sendMsgProgramCounter.max + ","
                  + mergeMsgProgramCounter.max + ",")
                mergeMsgProgramCounter.reset()
                writeCommonInfo(stageComplted)

                //                  mergeMsgProgramCounter.reset()
                //                  sendMsgProgramCounter.reset()
              } else if (name.equals("mapVertices") || name.equals("joinVertices")) {
                //Apply is finishing

                bwCounterData.append(stageComplted.stageInfo.stageId + ","
                  + name + ","
                  + vertexProgramCounter.max + ","
                  + ","
                  + ",")
                writeCommonInfo(stageComplted)

                //                  vertexProgramCounter.reset()
              } else if (name.equals("reduceMsg")) {
                //reduce is finishing
                bwCounterData.append(stageComplted.stageInfo.stageId + ","
                  + name + ","
                  + ","
                  + ","
                  + mergeMsgProgramCounter.max + ",")
                writeCommonInfo(stageComplted)

              } else {
                bwCounterData.append(stageComplted.stageInfo.stageId + ","
                  + name + ","
                  + ","
                  + ","
                  + ",")
                writeCommonInfo(stageComplted)

              }

              //              }

            }
            def writeCommonInfo(stageComplted: SparkListenerStageCompleted) {
              val taskURL = url + "/" + stageComplted.stageInfo.stageId + "/" + stageComplted.stageInfo.attemptId + "/taskList?sortBy=-runtime&&length=1"

              val json = fromURL(taskURL).mkString
              //              println(taskURL)
              val taskDetails = parse(json).extract[List[taskDetail]].head
              //              val remoteRead = stages.map(x => (x.stageId, x.name, x.accumulatorUpdates
              //                .filter { _.name.equals("internal.metrics.shuffle.read.remoteBytesRead") }.
              //                map(_.value.toLong).sum))
              //              remoteRead.foreach(x => {
              //                bwshuffle.write(x._1 + "," + x._2 + "," + x._3 + "\n")
              //              })

              bwCounterData.append(taskDetails.taskMetrics.executorRunTime + ","
                + taskDetails.taskMetrics.executorDeserializeTime + ","
                + taskDetails.taskMetrics.jvmGcTime + ","
                + (if (taskDetails.taskMetrics.shuffleReadMetrics != null) taskDetails.taskMetrics.shuffleReadMetrics.remoteBytesRead else "") + ","
                + (if (taskDetails.taskMetrics.shuffleReadMetrics != null) taskDetails.taskMetrics.shuffleReadMetrics.remoteBlocksFetched else "") + ","
                + (if (taskDetails.taskMetrics.shuffleReadMetrics != null) taskDetails.taskMetrics.shuffleReadMetrics.fetchWaitTime else "") + ","
                + (if (taskDetails.taskMetrics.shuffleReadMetrics != null) taskDetails.taskMetrics.shuffleReadMetrics.localBlocksFetched else "") + ","
                + (if (taskDetails.taskMetrics.shuffleReadMetrics != null) taskDetails.taskMetrics.shuffleReadMetrics.localBytesRead else "") + ","
                + (if (taskDetails.taskMetrics.shuffleReadMetrics != null) taskDetails.taskMetrics.shuffleReadMetrics.recordsRead else "") + ","

                + (if (taskDetails.taskMetrics.shuffleWriteMetrics != null) taskDetails.taskMetrics.shuffleWriteMetrics.bytesWritten else "") + ","
                + (if (taskDetails.taskMetrics.shuffleWriteMetrics != null) taskDetails.taskMetrics.shuffleWriteMetrics.writeTime else "") + ","
                + (if (taskDetails.taskMetrics.shuffleWriteMetrics != null) taskDetails.taskMetrics.shuffleWriteMetrics.recordsWritten else "") + ","

                + (if (taskDetails.taskMetrics.inputMetrics != null) taskDetails.taskMetrics.inputMetrics.bytesRead else "") + ","
                + (if (taskDetails.taskMetrics.inputMetrics != null) taskDetails.taskMetrics.inputMetrics.recordsRead else "") + ","

                + (if (taskDetails.taskMetrics.outputMetrics != null) taskDetails.taskMetrics.outputMetrics.bytesWritten else "") + ","
                + (if (taskDetails.taskMetrics.outputMetrics != null) taskDetails.taskMetrics.outputMetrics.recordsWritten else "") + ","

                + (stageComplted.stageInfo.completionTime.get - stageComplted.stageInfo.submissionTime.get) + "\n")
            }
            override def onJobEnd(jobEnd: SparkListenerJobEnd) {
              //              Thread.sleep(10)

              //              vertexProgramCounter.reset()
              //              sendMsgProgramCounter.reset()
              //              mergeMsgProgramCounter.reset()
              //              bwCounterData.append(jobEnd.time)
              bwCounter.write(bwCounterData.toString())
              bwCounterData.delete(0, bwCounterData.length());
              bwCounter.flush()

            }

            override def onApplicationEnd(append: SparkListenerApplicationEnd) {

            }
          });
        }

        var srcV = 0L
        var dstV = 0L
        var vpTime = 0l
        var svpTime = 0l
        var totalvpTime = 0l
        //   var degree = sc.broadcast(allnodes)
        for (line <- Source.fromFile(folder + inputfile).getLines()) {

          val tmp = line.split(",")
          srcV = tmp(0).toLong
          dstV = tmp(1).toLong
          count = count + 1
          total = total + 1
          if (srcV != dstV) { //avoid self loop

            edges.put((srcV, dstV), if (tmp.length > 2) tmp(2).toLong else 0)

            nodes.add(srcV)
            nodes.add(dstV)
            if (partionStrategy.equals("DBH") || vertexPartition) {
              var temp = degree.getOrElse(srcV, collection.mutable.Set[Long]())
              temp.add(dstV)
              degree.put(srcV, temp)

            }
            if (vertexPartition) {
              svpTime = new Date().getTime
              myVertexPartitioner.indegree = degree.map(f => (f._1, f._2.size))
              myVertexPartitioner.partitionVertex(srcV)
              myVertexPartitioner.partitionVertex(dstV)
              vpTime = vpTime + (new Date().getTime - svpTime)
              totalvpTime = totalvpTime + vpTime
            }

          }
          if (count > 0) {
            if (count == batch) {
              //              println("distinct edges: " + edges.size)

              process
              if (vertexPartition) {
                //                println("vertexPartition time : " + vpTime)
                vpTime = 0
              }
              count = 0
            } //end of if
          }
        } // end of for loop
        if (count != 0) {

          process
        }

        def process {

          //creating edge RDD and initial msg from input
          for (((node1, node2), time) <- edges.seq.iterator) {

            inputEdgeArray.+=(Edge(node1, node2, time))

          }
          for (node1 <- nodes.seq.iterator) {

            inputVertexArray.+=((node1, (Long.MaxValue, heavyString)))

          }

          if (vertexPartition)
            users = sc.parallelize(inputVertexArray.result(), numPartitions).partitionBy(myVertexPartitioner.fromString("degree", numPartitions)).setName("User RDD")
          else
            users = sc.parallelize(inputVertexArray.result(), numPartitions).setName("User RDD")

          relationships = sc.parallelize(inputEdgeArray.result(), numPartitions)

          graph = Graph(users, relationships, (Long.MaxValue, heavyString))

          if (partionStrategy.equals("DBH")) {

            val degreeBC = sc.broadcast(degree.map(f => (f._1, f._2.size)))
            mypartitioner = new MyPartitionStrategy(degreeBC.value)

          }

          val oldgraph = graph
          if (!partionStrategy.equals(""))
            graph = graph.partitionBy(mypartitioner.fromString(partionStrategy), numPartitions).groupEdges((a, b) => Math.max(a, b))
          graph.vertices.setName("gu vertex")
          graph.edges.setName("gu edges")
          graph.cache()
          oldgraph.unpersist(blocking = false)
          oldgraph.unpersistVertices(blocking = false)

          graph.vertices.count()
          println("Total Reading and Partitioning time : " + (new Date().getTime - startimeTotal))
          if (algo.equals("PR")) {
            new PageRank(vertexProgramCounter, sendMsgProgramCounter, mergeMsgProgramCounter, vertexProgDelay, sendProgDelay, mergeProgDelay).pageRank(graph, PR_Itteration)
          } else if (algo.equals("CC")) {
            new ConnectedComponent(vertexProgramCounter, sendMsgProgramCounter, mergeMsgProgramCounter, vertexProgDelay, sendProgDelay, mergeProgDelay).connectedComponent(graph, PR_Itteration)
          } else if (algo.equals("SP")) {
            val landmarks = graph.vertices.map(x => x._1).takeSample(false, landmarkCount, seedForlandMarkSelection)
            new ShortestPaths(vertexProgramCounter, sendMsgProgramCounter, mergeMsgProgramCounter, vertexProgDelay, sendProgDelay, mergeProgDelay).shortestPath(graph, landmarks, PR_Itteration)
          } else if (algo.equals("LP")) {
            new LabelPropagation(vertexProgramCounter, sendMsgProgramCounter, mergeMsgProgramCounter, vertexProgDelay, sendProgDelay, mergeProgDelay).run(graph, PR_Itteration)

          }

          println("Total Completed in time : " + (new Date().getTime - startimeTotal))
          //        println("Total Pr time: " + totalPrTime)
          println("Total vertex Partitioning time: " + totalvpTime)
          try {
            val json = fromURL(url).mkString
            val stages: List[SparkStage] = parse(json).extract[List[SparkStage]]
              .filter { x =>
                {
                  if ((x.name.equals("mapPartitions at VertexRDDImpl.scala:245"))
                    || (x.name.equals("reduce at VertexRDDImpl.scala:88"))
                    || (x.name.equals("mapPartitions at GraphImpl.scala:207"))
                    || (x.name.equals("mapPartitions at VertexRDD.scala:356"))
                    || (x.name.equals("mapPartitions at VertexRDDImpl.scala:249")))
                    true
                  else
                    false
                }
              }
            var stageTime = stages.map { x =>
              var name = ""
              if (x.name.equals("mapPartitions at VertexRDDImpl.scala:245")) {
                if (x.details.contains("org.apache.spark.graphx.impl.GraphImpl.mapVertices")) {
                  name = "mapVertices"
                } else if (x.details.contains("org.apache.spark.graphx.GraphOps.joinVertices")) {
                  name = "joinVertices"
                } else {
                  name = "shipVertexAttribute"
                }
              } else if (x.name.equals("reduce at VertexRDDImpl.scala:88")) {
                name = "reduceMsg"
              } else if (x.name.equals("mapPartitions at VertexRDD.scala:356")) {
                name = "createGraph"
              } else if (x.name.equals("mapPartitions at VertexRDDImpl.scala:249")) {
                name = "shipVertexIds"
              } else if (x.name.equals("mapPartitions at GraphImpl.scala:207")) {
                name = "sendMsg"
              } else {
                name = "unKnown"
              }
              (x.stageId, name, GenerateData.stringToDate(x.completionTime).getTime - GenerateData.stringToDate(x.submissionTime).getTime)
            }
            val f = new File(ofolder + outputFile.replace(".csv", "_" + partionStrategy + "_stageDetail.csv"))
            val bw = new BufferedWriter(new FileWriter(f))
            var stagefilter = stageTime.filter(_._2.equals("mapVertices"))
            for (x <- stagefilter) {
              bw.write(x._1 + "," + x._2 + "," + x._3 + "\n")
            }
            bw.write("\n")
            bw.write("\n")
            bw.flush()

            stagefilter = stageTime.filter(_._2.equals("joinVertices"))
            for (x <- stagefilter) {
              bw.write(x._1 + "," + x._2 + "," + x._3 + "\n")
            }
            bw.write("\n")
            bw.write("\n")
            bw.flush()
            stagefilter = stageTime.filter(_._2.equals("unKnown"))
            for (x <- stagefilter) {
              bw.write(x._1 + "," + x._2 + "," + x._3 + "\n")
            }
            bw.write("\n")
            bw.write("\n")
            bw.flush()
            stagefilter = stageTime.filter(_._2.equals("shipVertexAttribute"))
            for (x <- stagefilter) {
              bw.write(x._1 + "," + x._2 + "," + x._3 + "\n")
            }
            bw.write("\n")
            bw.write("\n")
            bw.flush()

            stagefilter = stageTime.filter(_._2.equals("reduceMsg"))
            for (x <- stagefilter) {
              bw.write(x._1 + "," + x._2 + "," + x._3 + "\n")
            }
            bw.write("\n")
            bw.write("\n")
            bw.flush()
            stagefilter = stageTime.filter(_._2.equals("createGraph"))
            for (x <- stagefilter) {
              bw.write(x._1 + "," + x._2 + "," + x._3 + "\n")
            }
            bw.write("\n")
            bw.write("\n")
            bw.flush()
            stagefilter = stageTime.filter(_._2.equals("shipVertexIds"))
            for (x <- stagefilter) {
              bw.write(x._1 + "," + x._2 + "," + x._3 + "\n")
            }
            bw.write("\n")
            bw.write("\n")
            bw.flush()
            stagefilter = stageTime.filter(_._2.equals("sendMsg"))
            for (x <- stagefilter) {
              bw.write(x._1 + "," + x._2 + "," + x._3 + "\n")
            }
            bw.flush()

            bw.close()
          } catch {
            case e: Exception => {
              //        logger.error(e)
              println("error in stage details writing " + e.getMessage)
            }
          }
          //writing job details
          try {
            val jobjson = fromURL(url.replace("stages", "jobs")).mkString
            val jobs: List[SparkJobs] = parse(jobjson).extract[List[SparkJobs]]
            var jobTime = jobs.map { x =>

              (x.jobId, x.name, GenerateData.stringToDate(x.completionTime).getTime - GenerateData.stringToDate(x.submissionTime).getTime, x.completionTime, x.submissionTime, x.stageIds.foldRight(",") {
                (a, b) => a + "," + b
              })

            }
            val fjob = new File(ofolder + outputFile.replace(".csv", "_" + partionStrategy + "_jobDetail.csv"))
            val bwjob = new BufferedWriter(new FileWriter(fjob))
            for (x <- jobTime) {
              bwjob.write(x._1 + "," + x._2 + "," + x._3 + "," + x._4 + "," + x._5 + "," + x._6 + "\n")
            }
            bwjob.close()
          } catch {
            case e: Exception => {
              //        logger.error(e)
              println("error in job details writing " + e.getMessage)
            }
          }

        }
        bwCounter.flush()
        bwCounter.close()
      } catch {
        case e: Exception => {
          //        logger.error(e)
          e.printStackTrace()
        }

      }
    } catch {
      case e: Exception => {
        //        logger.error(e)
        e.printStackTrace()
      }

    }
    println("Finished exiting")
    System.exit(1)
  }

}