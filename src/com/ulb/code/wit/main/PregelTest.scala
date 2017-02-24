package com.ulb.code.wit.main

import org.apache.spark.graphx._
import com.alibaba.fastjson.JSONObject
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.immutable.HashMap


object PregelTest {
  val logger = Logger.getLogger(getClass().getName());
  def run(graph: Graph[HashMap[String, Int], HashMap[String, Int]]): Graph[HashMap[String, Int], HashMap[String, Int]] = {

    def vProg(v: VertexId, attr: HashMap[String, Int], msg: Integer): HashMap[String, Int] = {
      var updatedAttr = attr
      
      if (msg < 0) {
        // init message received 
        if (v.equals(0.asInstanceOf[VertexId])) updatedAttr = attr.+("LENGTH" -> 0)
        else updatedAttr = attr.+("LENGTH" -> Integer.MAX_VALUE)
      } else {
        updatedAttr = attr.+("LENGTH" -> (msg + 1))
      }
      updatedAttr
    }

    def sendMsg(triplet: EdgeTriplet[HashMap[String, Int], HashMap[String, Int]]): Iterator[(VertexId, Integer)] = {
      val len = triplet.srcAttr.get("LENGTH").get
      // send a msg if last hub is reachable 
      if (len < Integer.MAX_VALUE) Iterator((triplet.dstId, len))
      else Iterator.empty
    }

    def mergeMsg(msg1: Integer, msg2: Integer): Integer = {
      if (msg1 < msg2) msg1 else msg2
    }

    Pregel(graph, new Integer(-1), 3, EdgeDirection.Either)(vProg, sendMsg, mergeMsg)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Pregel Test")
    conf.set("spark.master", "local")
    val sc = new SparkContext(conf)
    val test = new HashMap[String, Int]

    // create a simplest test graph with 3 nodes and 2 edges 
    val vertexList = Array(
      (0.asInstanceOf[VertexId], new HashMap[String, Int]),
      (1.asInstanceOf[VertexId], new HashMap[String, Int]),
      (2.asInstanceOf[VertexId], new HashMap[String, Int]),
      (3.asInstanceOf[VertexId], new HashMap[String, Int]))
    val edgeList = Array(
      Edge(0.asInstanceOf[VertexId], 1.asInstanceOf[VertexId], new HashMap[String, Int]),
      Edge(1.asInstanceOf[VertexId], 2.asInstanceOf[VertexId], new HashMap[String, Int]),
      Edge(0.asInstanceOf[VertexId], 3.asInstanceOf[VertexId], new HashMap[String, Int]))

    val vertexRdd = sc.parallelize(vertexList)
    val edgeRdd = sc.parallelize(edgeList)
    val g = Graph[HashMap[String, Int], HashMap[String, Int]](vertexRdd, edgeRdd)

    // run test code 
    val lpa = run(g)
    lpa.vertices.collect().map(println)
  }
} 