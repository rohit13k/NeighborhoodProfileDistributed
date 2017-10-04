package com.ulb.code.wit.main

import scala.reflect.ClassTag
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
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerTaskEnd
import com.ulb.code.wit.util.SparkAppStats.taskDetail
import org.apache.spark.graphx._
import com.ulb.code.wit.util.Wait

/**
 * Computes shortest paths to the given set of landmark vertices, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
 */
class ShortestPaths(val vertexProgramCounter: MyAccumulator, val sendMsgProgramCounter: MyAccumulator, val mergeMsgProgramCounter: MyAccumulator, val vertexProgDelay: Int, val sendProgDelay: Int, val mergeProgDelay: Int) extends Serializable {

  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, Int]

  private def makeMap(x: (VertexId, Int)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

  /**
   * Computes shortest paths to the given set of landmark vertices. if its less then max ititeration needed
   *
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
   * landmark.
   *
   * @return a graph where each vertex attribute is a map containing the shortest-path distance to
   * each reachable landmark vertex.
   */
  def shortestPath(graph: Graph[(Long, String), Long], landmarks: Seq[VertexId], PR_Itteration: Int): Graph[SPMap, Long] = {
    val stime = new Date().getTime
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> 0) else makeMap()
    }.cache()

    val initialMessage = makeMap()
    spGraph.vertices.count
    spGraph.triplets.count

    val ptime = new Date().getTime
    val result = PregelMon(spGraph, initialMessage, PR_Itteration, EdgeDirection.Either)(vertexProgram, sendMessage, mergeMsg)(vertexProgramCounter, sendMsgProgramCounter, mergeMsgProgramCounter)

    println("SP time: " + (new Date().getTime - stime))
    println("PR Pregel time:" + (new Date().getTime - ptime))
    result
  }
  def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
    if (vertexProgDelay != -1) {

      if (vertexProgDelay == 0) {
        vertexProgramCounter.add(1)
      } else {
        vertexProgramCounter.add(vertexProgDelay)
        //        Thread.sleep(vertexProgDelay)
        Wait.waitfor(vertexProgDelay)
      }
    }
    addMaps(attr, msg)
  }

  def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
    if (sendProgDelay != -1) {
      if (sendProgDelay == 0) {
        sendMsgProgramCounter.add(1)
      } else {
        sendMsgProgramCounter.add(sendProgDelay)
        //        Thread.sleep(sendProgDelay)
        Wait.waitfor(sendProgDelay)
      }
    }
    val newAttr = incrementMap(edge.dstAttr)
    if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
    else Iterator.empty
  }
  def mergeMsg(spmap1: SPMap, spmap2: SPMap): SPMap = {
    if (mergeProgDelay != -1) {
      if (mergeProgDelay == 0) {
        mergeMsgProgramCounter.add(1)
      } else {
        mergeMsgProgramCounter.add(mergeProgDelay)
        //        Thread.sleep(mergeProgDelay)
        Wait.waitfor(mergeProgDelay)

      }
    }
    addMaps(spmap1, spmap2)
  }

}