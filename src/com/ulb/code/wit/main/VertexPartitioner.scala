package com.ulb.code.wit.main

import org.apache.spark.Partitioner

class VertexPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int =
    {
      val out = toLong(key.toString)
      //out.hashCode() % numPartitions
      0
    }

  override def equals(other: Any): Boolean = other match {
    case dnp: VertexPartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }

  def toLong(s: String): Long =
    {
      try {
        s.toLong

      } catch {
        case e: Exception => 1l
      }
    }
}