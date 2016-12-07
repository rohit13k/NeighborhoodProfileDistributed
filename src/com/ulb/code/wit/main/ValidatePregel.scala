package com.ulb.code.wit.main

import java.util.logging.Logger
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object ValidatePregel {
  val logger = Logger.getLogger(getClass().getName());
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NeighbourHoodProfileApprox").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
  }
}
class vertex {

}