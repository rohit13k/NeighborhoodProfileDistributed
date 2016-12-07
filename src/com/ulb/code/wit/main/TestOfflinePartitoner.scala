package com.ulb.code.wit.main
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
import scala.collection.mutable.ArrayBuilder
import scala.collection.mutable.HashSet
import java.util.Random
object TestOfflinePartitoner {

  var nodesummary = collection.mutable.Map[Long, Int]()
  var nodedegree = collection.mutable.Map[Long, collection.mutable.Set[Long]]()
  var partitions = collection.mutable.Map[Int, collection.mutable.Set[Long]]()
  var edgepartitionsummary = collection.mutable.Map[Int, collection.mutable.Set[(Long, Long)]]()
  var edgepartition = collection.mutable.Map[(Long, Long), Int]()
  val epsilon = 1
  var maxload = 0
  var minload = 0
  var lambda = 1.0
  def main(args: Array[String]) {
    val prop = new Properties()
    try {

      val configFile = args(0).toString
      prop.load(new FileInputStream(configFile))

      //      logger.info("Test Started")
      println("Test Started" + new Date())
      val numParts = Integer.parseInt(prop.getProperty("partition", "6"))
      val partionStrategy = prop.getProperty("partionStrategy", "")
      val inputfile = prop.getProperty("inputfile", "facebook_reduced.csv")
      val output = inputfile.replace(".csv", "") + "_" + partionStrategy + "_rft.csv"
      val folder = prop.getProperty("folder", "./data/")
      lambda = (prop.getProperty("hdrfLambda", "1")).toDouble
      val batch = Integer.parseInt(prop.getProperty("batch", "1000"))

      val hdrfLambda = lambda
      val f = new File(folder + output)
      val bw = new BufferedWriter(new FileWriter(f))
      val globalstats = new GlobalStats(numParts, hdrfLambda)
      var src = 0l
      var dst = 0l
      var count = 0;
      for (line <- Source.fromFile(folder + inputfile).getLines()) {
        val tmp = line.split(",")
        src = tmp(0).toLong
        dst = tmp(1).toLong

        //update activity
        nodesummary.put(src, nodesummary.getOrElse(src, 0) + 1)
        nodesummary.put(dst, nodesummary.getOrElse(dst, 0) + 1)
        //update degree
        var tempdegree = nodedegree.getOrElse(src, collection.mutable.Set[Long]())
        tempdegree.add(dst)
        nodedegree.put(src, tempdegree)
        tempdegree = nodedegree.getOrElse(dst, collection.mutable.Set[Long]())
        tempdegree.add(src)
        nodedegree.put(dst, tempdegree)
        var part = 0
        if (partionStrategy.equals("HDRF")) {
          part = getPartitionHDRF(src, dst, numParts)
        } else if (partionStrategy.equals("DBH")) {
          part = getPartitionDBH(src, dst, numParts)
        } else if (partionStrategy.equals("ABH")) {
          part = getPartitionABH(src, dst, numParts)
        } else if (partionStrategy.equals("ReverseABH")) {
          part = getPartitionReverseABH(src, dst, numParts)
        } else if (partionStrategy.equals("BABH")) {
          part = getPartitionReverseBalancedABH(src, dst, numParts)
        } else if (partionStrategy.equals("CanonicalRandomVertexCut")) {
          part = getPartitionCRVC(src, dst, numParts)
        } else if (partionStrategy.equals("RandomVertexCut")) {
          part = getPartitionRVC(src, dst, numParts)
        } else {
          System.exit(-1)
        }

        //check if the edge is first time or not
        if (edgepartition.contains(src, dst)) {
          val oldpart = edgepartition.get(src, dst).get
          if (oldpart != part) {
            //remove old edge from the partition
            val temppartition = edgepartitionsummary.get(oldpart).get
            temppartition.remove(src, dst)
            edgepartitionsummary.put(oldpart, temppartition)

          }
        }
        //store the partition for the edge
        val temppartition = edgepartitionsummary.getOrElse(part, collection.mutable.Set[(Long, Long)]())
        temppartition.add((src, dst))
        edgepartitionsummary.put(part, temppartition)
        //update node replication in the partition

        count = count + 1
        if (count % batch == 0) {
          //find the node copies on each partition based on the edges on that partition
          partitions = createNodePartitions()
          val rft: Double = BigDecimal(getRFt(partitions, nodesummary)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          val rf: Double = BigDecimal(getRF(partitions)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

          val tp = edgepartitionsummary.mapValues { x => x.size }.values
          var min = Int.MaxValue
          var max = Int.MinValue
          for (x <- tp) {
            if (min > x) {
              min = x
            }
            if (max < x) {
              max = x
            }
          }
          println(count + "," + rft + "," + "," + rf + "," + min + "," + max)
          bw.write(count + "," + rft + "," + "," + rf + "," + min + "," + max + "\n")
          bw.flush()
        }
        if (count == 1000000) {
          bw.close()
          System.exit(0)
        }

      }
      bw.close()
    } catch {
      case e: Exception => {
        //        logger.error(e)
        e.printStackTrace()
      }
    }
  }
  def createNodePartitions(): collection.mutable.Map[Int, collection.mutable.Set[Long]] = {
    var partitions = collection.mutable.Map[Int, collection.mutable.Set[Long]]()
    for ((part, edges) <- edgepartitionsummary.iterator) {
      val tempset: collection.mutable.Set[Long] = collection.mutable.Set.empty[Long]
      for ((src, dst) <- edges) {
        tempset.add(src)
        tempset.add(dst)
      }
      partitions.put(part, tempset)
    }
    partitions
  }

  def getRFt(partitions: collection.mutable.Map[Int, collection.mutable.Set[Long]], nodeSummary: collection.mutable.Map[Long, Int]): Double = {
    var rft = 0.0

    var totalActivity = 0.0
    for ((node, activity) <- nodeSummary.iterator) {
      var rf = 0;
      for ((part, nodereplications) <- partitions) {
        if (nodereplications.contains(node)) {
          rf = rf + 1
        }
      }
      rft = rft + rf * activity
      totalActivity = totalActivity + activity
    }
    return rft / (nodedegree.size * (totalActivity / nodedegree.size))
  }
  def getRF(partitions: collection.mutable.Map[Int, collection.mutable.Set[Long]]): Double = {
    return partitions.mapValues { x => x.size }.valuesIterator.fold(0)(_ + _).toDouble / (nodesummary.size)

  }

  def getPartitionReverseABH(src: Long, dst: Long, numParts: Int): Int = {
    val srcDegree = nodesummary.getOrElse(src, 0)
    val dstDegree = nodesummary.getOrElse(dst, 0)
    if (srcDegree > dstDegree) {
      math.abs(src.hashCode()) % numParts
    } else {
      math.abs(dst.hashCode()) % numParts
    }
  }
  def getPartitionReverseABHBalanced(src: Long, dst: Long, numParts: Int): Int = {
    val srcDegree = nodesummary.getOrElse(src, 0)
    val dstDegree = nodesummary.getOrElse(dst, 0)
    var part = 0;
    if (srcDegree > dstDegree) {
      part = math.abs(src.hashCode()) % numParts
    } else {
      part = math.abs(dst.hashCode()) % numParts
    }
    val load = partitions.get(part).size;
    if(load>5500){
       if (srcDegree < dstDegree) {
      part = math.abs(src.hashCode()) % numParts
    } else {
      part = math.abs(dst.hashCode()) % numParts
    }
    }
    part
  }
  def getPartitionReverseBalancedABH(src: Long, dst: Long, numParts: Int): Int = {

    val srcActivity = nodesummary.getOrElse(src, 1)
    val dstActivity = nodesummary.getOrElse(dst, 1)
    var srcNormalizedActivity: Double = srcActivity / (srcActivity + dstActivity)
    var dstNormaizedActivity: Double = 1 - srcNormalizedActivity

    var costReplication = 0.0
    var costBal = 0
    var candidates = collection.mutable.Set[Int]()
    var MAX_SCORE = 0.0
    for (i <- 0 to numParts - 1) {
      var SCORE_p = 0.0
      if (partitions.get(i).contains(src)) {
        costReplication = 1.0 + (1.0 + srcNormalizedActivity).toDouble
      }
      if (partitions.get(i).contains(dst)) {
        costReplication = 1.0 + (1.0 + dstNormaizedActivity).toDouble
      }
      val load = partitions.get(i).size;
      findLoad()
      costBal = (maxload - load);
      costBal /= (epsilon + maxload - minload);
      if (costBal < 0) { costBal = 0; }

      SCORE_p = costReplication + lambda * costBal
      if (SCORE_p > MAX_SCORE) {
        MAX_SCORE = SCORE_p
        candidates.clear()
        candidates.add(i)
      } else if (SCORE_p == MAX_SCORE) {
        candidates.add(i)
      }
    }
    if (candidates.size == 0) {
      println("ERROR: GreedyObjectiveFunction.performStep -> candidates.isEmpty()");
      println("MAX_SCORE: " + MAX_SCORE);
      System.exit(-1);
    }

    //*** PICK A RANDOM ELEMENT FROM CANDIDATES
    val r = new Random();
    val choice = r.nextInt(candidates.size);
    val part = candidates.toList(choice)
    part

  }
  def getPartitionABH(src: Long, dst: Long, numParts: Int): Int = {
    val srcDegree = nodesummary.getOrElse(src, 0)
    val dstDegree = nodesummary.getOrElse(dst, 0)
    if (srcDegree < dstDegree) {
      math.abs(src.hashCode()) % numParts
    } else {
      math.abs(dst.hashCode()) % numParts
    }
  }
  def getPartitionDBH(src: Long, dst: Long, numParts: Int): Int = {
    val srcDegree = nodedegree.getOrElse(src, Set.empty)
    val dstDegree = nodedegree.getOrElse(dst, Set.empty)
    if (srcDegree.size < dstDegree.size) {
      math.abs(src.hashCode()) % numParts
    } else {
      math.abs(dst.hashCode()) % numParts
    }
  }
  def getPartitionCRVC(src: Long, dst: Long, numParts: Int): Int = {
    if (src < dst) {
      math.abs((src, dst).hashCode()) % numParts
    } else {
      math.abs((dst, src).hashCode()) % numParts
    }
  }
  def getPartitionRVC(src: Long, dst: Long, numParts: Int): Int = {
    math.abs((src, dst).hashCode()) % numParts
  }
  def getPartitionHDRF(src: Long, dst: Long, numParts: Int): Int = {
    val srcDegree = nodedegree.getOrElse(src, Set.empty).size
    val dstDegree = nodedegree.getOrElse(dst, Set.empty).size
    var srcNormalizedDegree: Double = srcDegree / (srcDegree + dstDegree)
    var dstNormaizedDegree: Double = 1 - srcNormalizedDegree

    var costReplication = 0.0
    var costBal = 0
    var candidates = collection.mutable.Set[Int]()
    var MAX_SCORE = 0.0
    for (i <- 0 to numParts - 1) {
      var SCORE_p = 0.0
      if (partitions.get(i).contains(src)) {
        costReplication = 1.0 + (1.0 - srcNormalizedDegree).toDouble
      }
      if (partitions.get(i).contains(dst)) {
        costReplication = 1.0 + (1.0 - dstNormaizedDegree).toDouble
      }
      val load = partitions.get(i).size;
      findLoad()
      costBal = (maxload - load);
      costBal /= (epsilon + maxload - minload);
      if (costBal < 0) { costBal = 0; }

      SCORE_p = costReplication + lambda * costBal
      if (SCORE_p > MAX_SCORE) {
        MAX_SCORE = SCORE_p
        candidates.clear()
        candidates.add(i)
      } else if (SCORE_p == MAX_SCORE) {
        candidates.add(i)
      }
    }
    if (candidates.size == 0) {
      println("ERROR: GreedyObjectiveFunction.performStep -> candidates.isEmpty()");
      println("MAX_SCORE: " + MAX_SCORE);
      System.exit(-1);
    }

    //*** PICK A RANDOM ELEMENT FROM CANDIDATES
    val r = new Random();
    val choice = r.nextInt(candidates.size);
    val part = candidates.toList(choice)
    part

  }
  def findLoad() {

    for (i <- 0 to partitions.size - 1) {
      val temp = partitions.get(i).size
      if (temp > maxload) {
        maxload = temp
      }
      if (temp < minload) {
        minload = temp
      }
    }

  }

}