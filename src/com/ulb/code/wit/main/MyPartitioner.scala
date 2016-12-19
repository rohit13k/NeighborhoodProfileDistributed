package com.ulb.code.wit.main

import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx._
import com.ulb.code.wit.util.NodeApprox
import java.util.Random

class MyPartitionStrategy(val partitionlookup: collection.mutable.Map[(Long, Long), Int], val nodedegree: collection.mutable.Map[Long, Int], val nodeProfile: scala.collection.immutable.Map[Long, Long], val nodeUpdate: scala.collection.immutable.Map[Long, Int], val nodeReplication: scala.collection.mutable.Map[Long, Int]) extends Serializable {
  def this() {
    this(null, null, null, null, null)
  }

  def this(nodedegree: collection.mutable.Map[Long, Int]) {
    this(null, nodedegree, null, null, null)
  }
  def this(partitionlookup: collection.mutable.Map[(Long, Long), Int], nodedegree: collection.mutable.Map[Long, Int]) {
    this(partitionlookup, nodedegree, null, null, null)
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
  case object UBH extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val srcUpdateCount = nodeUpdate.getOrElse(src, 0)
      val dstUpdateCount = nodeUpdate.getOrElse(dst, 0)
      if (srcUpdateCount > dstUpdateCount) {
        math.abs(src.hashCode()) % numParts
      } else if (srcUpdateCount < dstUpdateCount) {
        math.abs(dst.hashCode()) % numParts
      } else {
        //if update count is same follow degree based approach
        val srcDegree = nodedegree.getOrElse(src, 0)
        val dstDegree = nodedegree.getOrElse(dst, 0)
        if (srcDegree < dstDegree) {
          math.abs(src.hashCode()) % numParts
        } else {
          math.abs(dst.hashCode()) % numParts
        }
      }
    }
  }
  case object UBHAdvanced extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val srcUpdateCount = nodeUpdate.getOrElse(src, 0)
      val dstUpdateCount = nodeUpdate.getOrElse(dst, 0)
      val srcReplicationCount=nodeReplication.getOrElse(src, 0)
      val dstReplicationCount=nodeReplication.getOrElse(dst, 0)
      if ((srcUpdateCount*srcReplicationCount) > (dstUpdateCount*dstReplicationCount)) {
        math.abs(src.hashCode()) % numParts
      } else if (((srcUpdateCount*srcReplicationCount) < (dstUpdateCount*dstReplicationCount))) {
        math.abs(dst.hashCode()) % numParts
      } else {
        //if update count is same follow degree based approach
        val srcDegree = nodedegree.getOrElse(src, 0)
        val dstDegree = nodedegree.getOrElse(dst, 0)
        if (srcDegree < dstDegree) {
          math.abs(src.hashCode()) % numParts
        } else {
          math.abs(dst.hashCode()) % numParts
        }
      }
    }
  }
  case object ReverseABH extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val srcDegree = nodedegree.getOrElse(src, 0)
      val dstDegree = nodedegree.getOrElse(dst, 0)
      if (srcDegree > dstDegree) {
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
  case object ONLYONE extends PartitionStrategy {

    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {

      1

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
    case "ReverseABH"               => ReverseABH
    case "NPH"                      => NPH
    case "UBH"                      => UBH
    case "UBHAdvanced"              => UBHAdvanced
    case "ONLYONE"                  => ONLYONE
    case _                          => throw new IllegalArgumentException("Invalid PartitionStrategy: " + s)
  }

}