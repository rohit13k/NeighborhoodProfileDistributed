package com.ulb.code.wit.main

import org.apache.spark.Partitioner
import java.util.Random

class VertexPartitioner(numParts: Int) extends Serializable {
  var indegree = collection.mutable.Map[Long, Int]()
  val partitionLookup = collection.mutable.Map[Long, Int]()
  val partitionLoad = collection.mutable.Map[Int, collection.mutable.Set[Long]]()

  def partitionVertex(v: Long) {
    if (!partitionLookup.contains(v)) {
      partition(v)
    }
  }
  def partition(out: Long) {

    var MIN_COST = Int.MaxValue
    var SCORE_p = 0
    var candidates = collection.mutable.Set[Int]()
    for (i <- 0 to numParts - 1) {
      SCORE_p = getCost(i)
      if (SCORE_p < MIN_COST) {
        MIN_COST = SCORE_p
        candidates.clear()
        candidates.add(i)
      } else if (SCORE_p == MIN_COST) {
        candidates.add(i)
      }
    }
    if (candidates.size == 0) {
      println("ERROR: candidates.isEmpty()");
      println("MAX_SCORE: " + MIN_COST);
      System.exit(-1);
    }

    //*** PICK A RANDOM ELEMENT FROM CANDIDATES
    val r = new Random();
    val choice = r.nextInt(candidates.size);
    val part = candidates.toList(choice)
    val temp = partitionLoad.getOrElse(part, collection.mutable.Set[Long]())
    temp.add(out)

    partitionLoad.update(part, temp)
    partitionLookup.+=((out, part))
  }
  def getCost(part: Int): Int = {
    if (partitionLoad.contains(part)) {
      var cost = 0
      for (u <- partitionLoad.get(part).get) {
        cost = cost + indegree.getOrElse(u, 0)+1
      }
      cost
    } else
      0
  }
  case class DegreePartitioner(numParts: Int) extends Partitioner {
    override def numPartitions: Int = numParts

    override def getPartition(key: Any): Int =
      {
   
        val out = toLong(key.toString)

        val part = partitionLookup.get(out)
        if (part.isEmpty)
          out.hashCode() % numParts
        else
          part.get

      }

    override def equals(other: Any): Boolean = other match {
      case dnp: DegreePartitioner =>
        dnp.numPartitions == numPartitions
      case _ =>
        false
    }
  }
  def toLong(s: String): Long =
    {
      try {

        s.toLong

      } catch {
        case e: Exception => {
          println("error in vertex partioner")
          1l
        }
      }
    }
  def fromString(s: String, numberPart: Int): Partitioner = s match {

    case "degree" => DegreePartitioner(numberPart)
    case _        => throw new IllegalArgumentException("Invalid PartitionStrategy: " + s)
  }
}