package com.ulb.code.wit.util

/**
 * @author Rohit
 */
class NodeExact(nodeId: Long, summ: Array[collection.mutable.Map[Long, Long]]) extends Serializable {
  var node: Long = nodeId
  var currentsuperstep = 0
  var summary: Array[collection.mutable.Map[Long, Long]] = summ
  var ischanged = false
  def getsummary(window: Long): Int = {
    var i = 0
    var sum = summary.clone()
    sum=sum.filter({ p => p != null })
    sum.foreach(f => f.filter { case (value, time) => time > window })
    var temp: scala.collection.Set[Long] = null
    for (i <- 0 to sum.length - 1) {
      if (temp == null)
        temp = sum(i).keySet
      else
        temp=temp ++ sum(i).keySet
    }
    return temp.size
  }
}