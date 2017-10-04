package com.ulb.code.wit.main

import com.ulb.code.wit.util.NodeApprox
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd._
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Date
import com.ulb.code.wit.util.NodeExact
import com.ulb.code.wit.util.NewNodeExact
import java.io.ObjectOutputStream
import java.io.FileOutputStream
import scala.collection.mutable.HashMap
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter
/**
 * @author Rohit
 */
object Testing {
  def main(args: Array[String]) {
    convertFile

  }

  def convertFile() {
    val inputfile = "D:\\dataset\\cit-Patents.txt"
    val outputfile = "D:\\dataset\\cit-Patents.csv"
    val fCounter = new File(outputfile)
    val bwCounter = new BufferedWriter(new FileWriter(fCounter))
    for (line <- Source.fromFile(inputfile).getLines()) {
      val temp = line.replace("\t", ",").replace(" ", ",")
      bwCounter.write(temp + "\n")
    }
    bwCounter.flush()
    bwCounter.close()
  }
  def getobjectSize() {
    val heavyString = ConnectedComponentBatch.getData("D:\\dataset\\")
    val defaultNode = (Long.MaxValue, heavyString)
    //    val temp: collection.immutable.HashMap[Long, Long] = collection.immutable.HashMap(1l -> 1l)
    //    defaultNode._1.summary.update(0, temp)
    //    defaultNode._1.summary.update(1, temp)
    //    defaultNode._1.summary.update(2, temp)
    //
    //    for (i <- 0 to 200) {
    //      defaultNode._1.summary.update(2, defaultNode._1.summary(2).+(i + 0l -> (i + 100l)))
    //    }
    //    for (i <- 0 to 100) {
    //      defaultNode._1.summary.update(1, defaultNode._1.summary(1).+(i + 0l -> (i + 100l)))
    //    }
    //    for (i <- 0 to 109) {
    //      defaultNode._1.summary.update(0, defaultNode._1.summary(0).+(i + 0l -> (i + 100l)))
    //    }

    val oos = new ObjectOutputStream(new FileOutputStream("D:\\dataset\\defaultnode.obj"))
    oos.writeObject(defaultNode)
    oos.close
  }
  def slidingHyperAnf(vertexList: Array[(Long, NodeApprox)], edgeList: Array[(Long, Long, Long)], distance: Int, numberOfBucket: Int): Array[(Long, NodeApprox)] = {
    var i = 0
    var j = 0
    //initialize the 0th distance summary with the node itself
    vertexList.map { x =>
      {
        val log = new SlidingHLL(numberOfBucket)
        log.add(x._1)
        x._2.getDistanceWiseSummaries.add(log)
      }
    }
    for (i <- 1 to distance - 1) {
      vertexList.map { x =>
        {
          x._2.getDistanceWiseSummaries.add(new SlidingHLL(numberOfBucket))
          x._2.getDistanceWiseSummaries.get(i).union(x._2.getDistanceWiseSummaries.get(i - 1))
        }
      }

      for (j <- 0 to edgeList.length - 1) {

      }
    }

    return vertexList
  }
  def testSpark {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Test")
    conf.set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops")

    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sb = StringBuilder.newBuilder
    for (line <- Source.fromFile(".\\data\\facebook_reduced.csv").getLines()) {
      sb.append(line)
    }
    val broadcastVar = sc.broadcast(sb.toString())
    var rd = sc.textFile(".\\data\\facebook.csv", 1)

    rd = rd.map(x => {

      if (broadcastVar.value.length() == x.length()) {
        x
      } else {
        x + "~"
      }
    }).setName("rd")
    //    rd.cache()
    println(rd.count())
    val nrd = rd.map(x => {
      var temp = x.split(",")
      (temp(0), temp(1))
    }).setName("nrd")
    val trd = rd.map(x => (x, 1)).setName("trd")

    val temp = trd.join(nrd).setName("temp")
    temp.count()
    println("done")
    nrd.partitionBy(new HashPartitioner(2))
    multi("")_
  }
  def multi(x: String)(a: String)(b: Int): Int = {
    0
  }
  def resultExtractorForBestJob() {
    val rootfolder = "D:\\experiments\\pageRank_ConnectedComponent\\JournalExperiments\\9754945\\"

    val jobid = "9754945"
    //    val ps = Array("CanonicalRandomVertexCut", "EdgePartition2D", "DBH")
    val ps = Array("CanonicalRandomVertexCut", "EdgePartition2D", "DBH")
    //    val ds = Array("higgs-activity_time", "roadNet-CA")
    val ds = Array("roadNet-CA", "roadNet-PA")
    val maxrun = 5

    val algoList = Array("pr")
    //    val numP = 40
    var rowcount = 0
    var jobdetailMap = collection.mutable.Map[Int, jobInfo]()
    var tempJob: jobInfo = new jobInfo
    var oldJob = new jobInfo
    for (dataset <- ds) {
      for (partioner <- ps) {
        for (algo <- algoList) {
          val outputfile = rootfolder + jobid + "_" + algo + "_" + dataset + "_" + partioner + "_jobInfo.csv"
          jobdetailMap = collection.mutable.Map[Int, jobInfo]()
          for (i <- 1 to maxrun) {

            val datafolder = jobid + "_" + i + "\\"
            var inputfile = rootfolder + datafolder
            if (algo.equals("cc")) {
              inputfile = inputfile + "CC_" + dataset + "_estimate_3_" + partioner + "_jobDetail.csv"
            } else {
              inputfile = inputfile + "PR_" + dataset + "_estimate_300_" + partioner + "_jobDetail.csv"

            }
            rowcount = 0
            for (line <- Source.fromFile(inputfile).getLines()) {

              if (rowcount != 0) {
                tempJob = new jobInfo
                val data = line.split(",")
                if (data.length < 5) {
                  println(inputfile + " : " + line)
                } else {
                  tempJob.jobId = Integer.parseInt(data(0))
                  tempJob.jobName = data(1)
                  tempJob.jobTime = Integer.parseInt(data(2))

                  if (jobdetailMap.contains(tempJob.jobId)) {
                    oldJob = jobdetailMap.get(tempJob.jobId).get
                    if (tempJob.jobTime < oldJob.jobTime) {
                      jobdetailMap.update(tempJob.jobId, tempJob)
                    }
                  } else {
                    jobdetailMap.put(tempJob.jobId, tempJob)
                  }
                }
              }
              rowcount = rowcount + 1
            }

          }

          val fCounter = new File(outputfile)
          val bwCounter = new BufferedWriter(new FileWriter(fCounter))
          bwCounter.write("jobId,JobName,Time\n")
          for (result <- jobdetailMap.values) {
            bwCounter.write(result.jobId + ",")
            bwCounter.write(result.jobName + ",")

            bwCounter.write(result.jobTime + "," + "\n")
          }
          bwCounter.flush()
          bwCounter.close()
        }

      }

    }

  }

  def resultExtracterForBestCounter() {
    val rootfolder = "D:\\experiments\\pageRank_ConnectedComponent\\JournalExperiments\\9796928\\"

    val jobid = "9796928"
    //    val ps = Array("CanonicalRandomVertexCut", "EdgePartition2D", "DBH")
    val ps = Array("CanonicalRandomVertexCut", "EdgePartition2D", "DBH")
    //    val ds = Array("higgs-activity_time", "roadNet-CA","roadNet-PA","twitter_Punjab_10-12","fb_msg")
    val ds = Array("higgs-activity_time","fb_msg","com-dblp.ungraph.txt")
    val maxrun = 5
// val algoList = Array("pr", "cc","sp")
    val algoList = Array("pr")
    //    val numP = 40
    var rowcount = 0
    var stagedetailMap = collection.mutable.Map[Int, stageInfo]()
    var tempStage: stageInfo = new stageInfo
    var oldStage = new stageInfo
    for (dataset <- ds) {
      for (partioner <- ps) {
        for (algo <- algoList) {
          val outputfile = rootfolder + jobid + "_" + algo + "_" + dataset + "_" + partioner + ".csv"
          stagedetailMap = collection.mutable.Map[Int, stageInfo]()
          for (i <- 1 to maxrun) {

            val datafolder = jobid + "_" + i + "\\"
            var inputfile = rootfolder + datafolder
            if (algo.equals("cc")) {
              inputfile = inputfile + "CC_" + dataset + "_estimate_3_" + partioner + "_CounterDetail.csv"
            } else  if (algo.equals("pr")) {
              if(dataset.equals("com-dblp.ungraph.txt")){
              inputfile = inputfile + "PR_" + dataset + "_1000_" + partioner + "_CounterDetail.csv"
                
              }else{
              inputfile = inputfile + "PR_" + dataset + "_estimate_1000_" + partioner + "_CounterDetail.csv"
              }
            }else  {
              inputfile = inputfile + "SP_" + dataset + "_estimate_100_" + partioner + "_CounterDetail.csv"

            }
            rowcount = 0
            for (line <- Source.fromFile(inputfile).getLines()) {

              if (rowcount != 0) {
                tempStage = new stageInfo
                val data = line.split(",")
                if (data.length != 22) {
                  println(inputfile + " : " + line)
                } else {
                  tempStage.stageId = Integer.parseInt(data(0))
                  tempStage.stageName = data(1)
                  tempStage.vpCounter = data(2)
                  tempStage.spCounter = data(3)
                  tempStage.mpCounter = data(4)
                  tempStage.remoteBytesRead = data(8)
                  tempStage.remoteBlocksFetched = data(9)

                  tempStage.fetchWaitTime = data(10)
                  tempStage.localBlocksFetched = data(11)
                  tempStage.localBytesRead = data(12)
                  tempStage.recordsRead = data(13)
                  tempStage.bytesWritten = data(14)
                  tempStage.writeTime = data(15)
                  tempStage.recordsWritten = data(16)
                  tempStage.inputbytesRead = data(17)
                  tempStage.inputRecordsRead = data(18)
                  tempStage.outputBytesWritten = data(19)
                  tempStage.outputRecordsWritten = data(20)
                  tempStage.stageTime = if (data(21).equals("")) 0 else Integer.parseInt(data(21))
                  if (stagedetailMap.contains(tempStage.stageId)) {
                    oldStage = stagedetailMap.get(tempStage.stageId).get
                    if (tempStage.stageTime < oldStage.stageTime) {
                      stagedetailMap.update(tempStage.stageId, tempStage)
                    }
                  } else {
                    stagedetailMap.put(tempStage.stageId, tempStage)
                  }
                }
              }
              rowcount = rowcount + 1
            }

          }

          val fCounter = new File(outputfile)
          val bwCounter = new BufferedWriter(new FileWriter(fCounter))
          bwCounter.write("stageid,stageName,vpCounter,spCounter,mpCounter,remoteBytesRead,remoteBlocksFetched,fetchWaitTime,localBlocksFetched,localBytesRead,recordsRead,bytesWritten,writeTime,recordsWritten,inputbytesRead,inputRecordsRead,outputBytesWritten,outputRecordsWritten,time\n")
          for (result <- stagedetailMap.values) {
            bwCounter.write(result.stageId + ",")
            bwCounter.write(result.stageName + ",")
            bwCounter.write(result.vpCounter + ",")
            bwCounter.write(result.spCounter + ",")
            bwCounter.write(result.mpCounter + ",")
            bwCounter.write(result.remoteBytesRead + ",")
            bwCounter.write(result.remoteBlocksFetched + ",")
            bwCounter.write(result.fetchWaitTime + ",")
            bwCounter.write(result.localBlocksFetched + ",")
            bwCounter.write(result.localBytesRead + ",")
            bwCounter.write(result.recordsRead + ",")
            bwCounter.write(result.bytesWritten + ",")
            bwCounter.write(result.writeTime + ",")
            bwCounter.write(result.recordsWritten + ",")
            bwCounter.write(result.inputbytesRead + ",")
            bwCounter.write(result.inputRecordsRead + ",")
            bwCounter.write(result.outputBytesWritten + ",")
            bwCounter.write(result.outputRecordsWritten + ",")
            bwCounter.write(result.stageTime + "," + "\n")
          }
          bwCounter.flush()
          bwCounter.close()
        }

      }

    }

  }

  def resultMergerForCounter() {
    val rootfolder = "D:\\experiments\\pageRank_ConnectedComponent\\JournalExperiments\\9754945\\"

    val jobid = "9754945"
    //    val ps = Array("CanonicalRandomVertexCut", "EdgePartition2D", "DBH")
    val ps = Array("CanonicalRandomVertexCut", "EdgePartition2D", "DBH")
    //    val ds = Array("higgs-activity_time", "roadNet-CA")
    val ds = Array("roadNet-CA", "roadNet-PA")

    val algoList = Array("pr", "cc")
    //    val numP = 40
    var rowcount = 0
    var stagedetailMap = collection.mutable.Map[Int, stageInfo]()
    var tempStage: stageInfo = new stageInfo
    var oldStage = new stageInfo
    for (dataset <- ds) {
      for (partioner <- ps) {
        for (algo <- algoList) {
          val outputfile = rootfolder + jobid + "_" + algo + "_" + dataset  + ".csv"
          stagedetailMap = collection.mutable.Map[Int, stageInfo]()

          val inputfile = rootfolder + jobid + "_" + algo + "_" + dataset + "_" + partioner + ".csv"
         
          rowcount = 0
          for (line <- Source.fromFile(inputfile).getLines()) {

            if (rowcount != 0) {
              tempStage = new stageInfo
              val data = line.split(",")
              if (data.length != 22) {
                println(inputfile + " : " + line)
              } else {
                tempStage.stageId = Integer.parseInt(data(0))
                tempStage.stageName = data(1)
                tempStage.vpCounter = data(2)
                tempStage.spCounter = data(3)
                tempStage.mpCounter = data(4)
                tempStage.remoteBytesRead = data(5)
                tempStage.remoteBlocksFetched = data(6)

                tempStage.fetchWaitTime = data(7)
                tempStage.localBlocksFetched = data(8)
                tempStage.localBytesRead = data(9)
                tempStage.recordsRead = data(10)
                tempStage.bytesWritten = data(11)
                tempStage.writeTime = data(12)
                tempStage.recordsWritten = data(13)
                tempStage.stageTime = if (data(18).equals("")) 0 else Integer.parseInt(data(18))
                if (stagedetailMap.contains(tempStage.stageId)) {
                  oldStage = stagedetailMap.get(tempStage.stageId).get
                  if (tempStage.stageTime < oldStage.stageTime) {
                    stagedetailMap.update(tempStage.stageId, tempStage)
                  }
                } else {
                  stagedetailMap.put(tempStage.stageId, tempStage)
                }
              }
            }
            rowcount = rowcount + 1
          }

          val fCounter = new File(outputfile)
          val bwCounter = new BufferedWriter(new FileWriter(fCounter))
          bwCounter.write("stageid,stageName,vpCounter,spCounter,mpCounter,remoteBytesRead,remoteBlocksFetched,fetchWaitTime,localBlocksFetched,localBytesRead,recordsRead,bytesWritten,writeTime,recordsWritten,inputbytesRead,inputRecordsRead,outputBytesWritten,outputRecordsWritten,time\n")
          for (result <- stagedetailMap.values) {
            bwCounter.write(result.stageId + ",")
            bwCounter.write(result.stageName + ",")
            bwCounter.write(result.vpCounter + ",")
            bwCounter.write(result.spCounter + ",")
            bwCounter.write(result.mpCounter + ",")
            bwCounter.write(result.remoteBytesRead + ",")
            bwCounter.write(result.remoteBlocksFetched + ",")
            bwCounter.write(result.fetchWaitTime + ",")
            bwCounter.write(result.localBlocksFetched + ",")
            bwCounter.write(result.localBytesRead + ",")
            bwCounter.write(result.recordsRead + ",")
            bwCounter.write(result.bytesWritten + ",")
            bwCounter.write(result.writeTime + ",")
            bwCounter.write(result.recordsWritten + ",")
            bwCounter.write(result.inputbytesRead + ",")
            bwCounter.write(result.inputRecordsRead + ",")
            bwCounter.write(result.outputBytesWritten + ",")
            bwCounter.write(result.outputRecordsWritten + ",")
            bwCounter.write(result.stageTime + "," + "\n")
          }
          bwCounter.flush()
          bwCounter.close()
        }

      }

    }

  }

  /**
   * @author Rohit
   */
  class stageInfo() {
    var stageId = 0
    var stageName = ""
    var vpCounter = ""
    var spCounter = ""
    var mpCounter = ""
    var remoteBytesRead = ""

    var remoteBlocksFetched = ""
    var fetchWaitTime = ""
    var localBlocksFetched = ""
    var localBytesRead = ""
    var recordsRead = ""
    var bytesWritten = ""
    var writeTime = ""
    var recordsWritten = ""
    var inputbytesRead = ""
    var inputRecordsRead = ""
    var outputBytesWritten = ""
    var outputRecordsWritten = ""
    var stageTime = 0
  }
  class jobInfo() {
    var jobId = 0
    var jobName = ""
    var jobTime = 0

  }
def summaryResultParser() {
    val folder = "D:\\experiments\\pageRank_ConnectedComponent\\JournalExperiments\\9796928\\"
    val ifile = "9796928"
    var input = true
    var file = ""
    var partitioner = ""
    var algo = ""
    var time = 0l
    var linenumber = 1
    var oldvalue: (Long, Long) = (1l, 1l)
    var bestPregeltime: Long = 0l
    var bestPartitionTime = 0l
    var bestTime: HashMap[(String, String, String), (Long, Long)] = HashMap.empty
    var newPartitioningTime = 0l
    for (line <- Source.fromFile(folder + ifile + "_time.log").getLines()) {
      val data = line.trim().split(":")
      if (linenumber == 1) {

        file = data(1).trim()
        partitioner = data(2).trim()
      } else if (linenumber == 2) {
        newPartitioningTime = data(1).trim().toLong
      } else if (linenumber == 3) {
        algo = data(0).trim().replace("time", "")
      } else if (linenumber == 4) {

        input = true

        time = data(1).trim().toLong
        oldvalue = bestTime.getOrElse((file, algo, partitioner), (Long.MaxValue, Long.MaxValue))
        bestPregeltime = oldvalue._1
        bestPartitionTime = oldvalue._2
        if (time < bestPregeltime) {
          bestPregeltime = time
        }
        if (newPartitioningTime < bestPartitionTime) {
          bestPartitionTime = newPartitioningTime
        }

        bestTime.update((file, algo, partitioner), (bestPregeltime, bestPartitionTime))
      } else if (linenumber == 6) {
        linenumber = 0
      }

      linenumber = linenumber + 1
    }
    val bw = new BufferedWriter(new FileWriter(folder + ifile + "_best.csv"))
    for (x <- bestTime.iterator) {
      bw.write(x._1._1 + "," + x._1._2 + "," + x._1._3 + "," + x._2._1 + "," + x._2._2 / 5 + "\n")
    }
    bw.flush()
    bw.close()
  }
}