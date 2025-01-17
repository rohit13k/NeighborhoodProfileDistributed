package com.ulb.code.wit.main

import java.util.Random
import java.io.Serializable

class GlobalStats(val partitioncount: Int, val lambda: Double=1) extends Serializable {
  val epsilon = 1;
  var partitions = collection.mutable.Map[Int, collection.mutable.Set[Long]]()
  var maxload = 0
  var minload = 0
  var nodeactivity: Map[Long, Int] = Map[Long, Int]()
  val edgepartitionsummary = collection.mutable.Map[(Long, Long), Int]()
  val nodedegree = collection.mutable.Map[Long, Int]()
  for (i <- 0 to partitioncount) {
    partitions.put(i, collection.mutable.Set[Long]())
  }

  def findLoad() {

    for (i <- 0 to partitioncount) {
      val temp = partitions.get(i).size
      if (temp > maxload) {
        maxload = temp
      }
      if (temp < minload) {
        minload = temp
      }
    }

  }

  def updatestats(part: Int, edge: (Long, Long)) {
    val temp = partitions.getOrElse(part, collection.mutable.Set[Long]())
    temp.add(edge._1)
    temp.add(edge._2)
    partitions.update(part, temp)
  }

  def updatePartitionHDRF(src: Long, dst: Long, numParts: Int) {
    val srcDegree = nodedegree.getOrElse(src, 0) + 1
    val dstDegree = nodedegree.getOrElse(dst, 0) + 1
    var srcNormalizedDegree: Double = srcDegree / (srcDegree + dstDegree)
    var dstNormaizedDegree: Double = 1 - srcNormalizedDegree

    if (partitioncount != numParts) {
      throw new IllegalArgumentException("Global Stats and numParts does not match: " + partitioncount)
    }
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
      println("ERROR: candidates.isEmpty()");
      println("MAX_SCORE: " + MAX_SCORE);
      System.exit(-1);
    }

    //*** PICK A RANDOM ELEMENT FROM CANDIDATES
    val r = new Random();
    val choice = r.nextInt(candidates.size);
    val part = candidates.toList(choice)
    updatestats(part, (src, dst))
    edgepartitionsummary.put((src, dst), part)

  }
  def updatePartitionHURL(src: Long, dst: Long, numParts: Int) {
    val srcActivity = nodeactivity.getOrElse(src, 1) +1
    val dstActivity = nodeactivity.getOrElse(dst, 1) +1
    var srcNormalizedActivity: Double = srcActivity / (srcActivity + dstActivity)
    var dstNormaizedActivity: Double = 1 - srcNormalizedActivity

    if (partitioncount != numParts) {
      throw new IllegalArgumentException("Global Stats and numParts does not match: " + partitioncount)
    }
    var scoreReplication = 0.0
    var scoreBal = 0
    var candidates = collection.mutable.Set[Int]()
    var MAX_SCORE = 0.0
    for (i <- 0 to numParts - 1) {
      var SCORE_p = 0.0
      if (partitions.get(i).contains(src)) {
        scoreReplication = 1.0 + ( srcNormalizedActivity).toDouble
      }
      if (partitions.get(i).contains(dst)) {
        scoreReplication = 1.0 + ( dstNormaizedActivity).toDouble
      }
      val load = partitions.get(i).size;
      findLoad()
      scoreBal = (maxload - load);
      scoreBal /= (epsilon + maxload - minload);
      if (scoreBal < 0) { scoreBal = 0; }

      SCORE_p = scoreReplication + lambda * scoreBal
      if (SCORE_p > MAX_SCORE) {
        MAX_SCORE = SCORE_p
        candidates.clear()
        candidates.add(i)
      } else if (SCORE_p == MAX_SCORE) {
        candidates.add(i)
      }
    }
    if (candidates.size == 0) {
      println("ERROR: candidates.isEmpty()");
      println("MAX_SCORE: " + MAX_SCORE);
      System.exit(-1);
    }

    //*** PICK A RANDOM ELEMENT FROM CANDIDATES
    val r = new Random();
    val choice = r.nextInt(candidates.size);
    val part = candidates.toList(choice)
    updatestats(part, (src, dst))
    edgepartitionsummary.put((src, dst), part)

  }
}