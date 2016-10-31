package com.ulb.code.wit.main

import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx._
import com.ulb.code.wit.util.NodeApprox
import java.util.Random

class MyPartitionStrategy(val partitionlookup: collection.mutable.Map[(Long, Long), Int], val nodedegree: collection.mutable.Map[Long, Int], val nodeProfile: scala.collection.immutable.Map[Long, Long]) extends Serializable {
  def this() {
    this(null, null, null)
  }

  def this(nodedegree: collection.mutable.Map[Long, Int]) {
    this(null, nodedegree, null)
  }
  def this(partitionlookup: collection.mutable.Map[(Long, Long), Int], nodedegree: collection.mutable.Map[Long, Int]) {
    this(partitionlookup, nodedegree, null)
  }
  def this(nodeProfile: scala.collection.immutable.Map[Long, Long]) {
    this(null, null, nodeProfile)
  }
  case object Hashing extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      (math.abs(Math.round(Math.random() * 10000) % numParts)).toInt
    }
  }
  case object DBH extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val srcDegree = nodedegree.getOrElse(src, 0)
      val dstDegree = nodedegree.getOrElse(dst, 0)
      if (srcDegree < dstDegree) {
        math.abs(src.hashCode()) % numParts
      } else {
        math.abs(dst.hashCode()) % numParts
      }
    }
  }
  case object ABH extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val srcDegree = nodedegree.getOrElse(src, 0)
      val dstDegree = nodedegree.getOrElse(dst, 0)
      if (srcDegree < dstDegree) {
        math.abs(src.hashCode()) % numParts
      } else {
        math.abs(dst.hashCode()) % numParts
      }
    }
  }
  case object NPH extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val srcDegree = nodeProfile.getOrElse(src, 0l)
      val dstDegree = nodeProfile.getOrElse(dst, 0l)
      if (srcDegree < dstDegree) {
        math.abs(src.hashCode()) % numParts
      } else {
        math.abs(dst.hashCode()) % numParts
      }
    }
  }
  case object HDRF extends PartitionStrategy {

    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {

      partitionlookup.getOrElse((src, dst), (math.abs(Math.round(Math.random() * 10000) % numParts)).toInt)

    }
  }
  def fromString(s: String): PartitionStrategy = s match {
    case "RandomVertexCut"          => PartitionStrategy.RandomVertexCut
    case "EdgePartition1D"          => PartitionStrategy.EdgePartition1D
    case "EdgePartition2D"          => PartitionStrategy.EdgePartition2D
    case "CanonicalRandomVertexCut" => PartitionStrategy.CanonicalRandomVertexCut
    case "Hashing"                  => Hashing
    case "HDRF"                     => HDRF
    case "DBH"                      => DBH
    case "ABH"                      => ABH
    case "NPH"                      => NPH
    case _                          => throw new IllegalArgumentException("Invalid PartitionStrategy: " + s)
  }

}