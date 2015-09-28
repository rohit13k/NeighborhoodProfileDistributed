package com.ulb.code.wit.main

import com.ulb.code.wit.util.NodeApprox

/**
 * @author Rohit
 */
object Testing {
  def main(args: Array[String]) {

    print("a".to)
  }
  def slidingHyperAnf(vertexList: Array[(Long, NodeApprox)], edgeList: Array[(Long, Long, Long)], distance: Int, numberOfBucket: Int): Array[(Long, NodeApprox)] = {
    var i = 0
    var j = 0
    //initialize the 0th distance summary with the node itself
    vertexList.map { x =>
      {
        val log = new SlidingHLL(numberOfBucket)
        log.add(x._1)
        x._2.getDistanceWiseSummaries.add(log)
      }
    }
    for (i <- 1 to distance - 1) {
      vertexList.map { x =>
        {
          x._2.getDistanceWiseSummaries.add(new SlidingHLL(numberOfBucket))
          x._2.getDistanceWiseSummaries.get(i).union(x._2.getDistanceWiseSummaries.get(i - 1))
        }
      }

      for (j <- 0 to edgeList.length - 1) {

      }
    }

    return vertexList
  }
}