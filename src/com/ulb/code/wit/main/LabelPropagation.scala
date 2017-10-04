package com.ulb.code.wit.main

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import com.ulb.code.wit.util.Wait
import java.util.Date

/** Label Propagation algorithm. */
class LabelPropagation(val vertexProgramCounter: MyAccumulator, val sendMsgProgramCounter: MyAccumulator, val mergeMsgProgramCounter: MyAccumulator, val vertexProgDelay: Int, val sendProgDelay: Int, val mergeProgDelay: Int) extends Serializable {
  /**
   * Run static Label Propagation for detecting communities in networks.
   *
   * Each node in the network is initially assigned to its own community. At every superstep, nodes
   * send their community affiliation to all neighbors and update their state to the mode community
   * affiliation of incoming messages.
   *
   * LPA is a standard community detection algorithm for graphs. It is very inexpensive
   * computationally, although (1) convergence is not guaranteed and (2) one can end up with
   * trivial solutions (all nodes are identified into a single community).
   *
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param graph the graph for which to compute the community affiliation
   * @param maxSteps the number of supersteps of LPA to be performed. Because this is a static
   * implementation, the algorithm will run for exactly this many supersteps.
   *
   * @return a graph with vertex attributes containing the label of community affiliation
   */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")
 val stime = new Date().getTime

    val lpaGraph = graph.mapVertices { case (vid, _) => vid }
    lpaGraph.cache()
    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = {
      if (sendProgDelay != -1) {
      if (sendProgDelay == 0) {
        sendMsgProgramCounter.add(1)
      } else {
        sendMsgProgramCounter.add(sendProgDelay)
        //        Thread.sleep(sendProgDelay)
        Wait.waitfor(sendProgDelay)
      }
    }
      Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
    }
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
      : Map[VertexId, Long] = {
      if (mergeProgDelay != -1) {
      if (mergeProgDelay == 0) {
        mergeMsgProgramCounter.add(1)
      } else {
        mergeMsgProgramCounter.add(mergeProgDelay)
        //        Thread.sleep(mergeProgDelay)
        Wait.waitfor(mergeProgDelay)

      }
    }
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        i -> (count1Val + count2Val)
      }.toMap
    }
    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
      if (vertexProgDelay != -1) {

      if (vertexProgDelay == 0) {
        vertexProgramCounter.add(1)
      } else {
        vertexProgramCounter.add(vertexProgDelay)
        //        Thread.sleep(vertexProgDelay)
        Wait.waitfor(vertexProgDelay)
      }
    }
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }
    val initialMessage = Map[VertexId, Long]()
    lpaGraph.vertices.count
    lpaGraph.triplets.count

    val ptime = new Date().getTime
    val result = PregelMon(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)(vertexProgramCounter, sendMsgProgramCounter, mergeMsgProgramCounter)
       println("LP time: " + (new Date().getTime - stime))
    println("PR Pregel time:" + (new Date().getTime - ptime))
    result
  }
}