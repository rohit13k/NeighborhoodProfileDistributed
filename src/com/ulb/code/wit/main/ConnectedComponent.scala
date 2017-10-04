package com.ulb.code.wit.main

import org.apache.spark.graphx.MyAccumulator
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.util.Date
import com.ulb.code.wit.util.Wait
class ConnectedComponent(val vertexProgramCounter: MyAccumulator, val sendMsgProgramCounter: MyAccumulator, val mergeMsgProgramCounter: MyAccumulator, val vertexProgDelay: Int, val sendProgDelay: Int, val mergeProgDelay: Int) extends Serializable{
  def connectedComponent(graph: Graph[(Long, String), Long], PR_Itteration:Int): Graph[(Long, String), Long] = {
    val stime = new Date().getTime
    val initialMessage = Long.MaxValue
    val ccGraph = graph.mapVertices { case (vid, x) => (vid, x._2) }.cache

    ccGraph.vertices.count
    ccGraph.triplets.count

    val ptime = new Date().getTime
    val pregelGraph = PregelMon(ccGraph, initialMessage,
       PR_Itteration, EdgeDirection.Either)(
        vertexProgram,
        sendMessage,
        messageCombiner)(vertexProgramCounter, sendMsgProgramCounter, mergeMsgProgramCounter)
    ccGraph.unpersist()

    println("CC time: " + (new Date().getTime - stime))
    println("PR Pregel time:" + (new Date().getTime - ptime))
    pregelGraph
  }
  def vertexProgram(id: VertexId, attr: (Long, String), msg: Long): (Long, String) = {
    if (vertexProgDelay != -1) {

      if (vertexProgDelay == 0) {
        vertexProgramCounter.add(1)
      } else {
        vertexProgramCounter.add(vertexProgDelay)
       // Thread.sleep(vertexProgDelay)
        Wait.waitfor(vertexProgDelay)
      }
    }
    (math.min(attr._1, msg), attr._2)
  }
  def sendMessage(edge: EdgeTriplet[(VertexId, String), Long]): Iterator[(VertexId, VertexId)] = {
    if (sendProgDelay != -1) {
      if (sendProgDelay == 0) {
        sendMsgProgramCounter.add(1)
      } else {
        sendMsgProgramCounter.add(sendProgDelay)
//        Thread.sleep(sendProgDelay)
        Wait.waitfor(sendProgDelay)
      }
    }
    if (edge.srcAttr._1 < edge.dstAttr._1) {
      Iterator((edge.dstId, edge.srcAttr._1))
    } else if (edge.srcAttr._1 > edge.dstAttr._1) {
      Iterator((edge.srcId, edge.dstAttr._1))
    } else {
      Iterator.empty
    }
  }
  def messageCombiner(a: Long, b: Long): Long = {
    if (mergeProgDelay != -1) {
      if (mergeProgDelay == 0) {
        mergeMsgProgramCounter.add(1)
      } else {
        mergeMsgProgramCounter.add(mergeProgDelay)
//        Thread.sleep(mergeProgDelay)
         Wait.waitfor(mergeProgDelay)
      }
    }
    math.min(a, b)
  }

}