package com.ulb.code.wit.util

import it.unimi.dsi.fastutil.objects.ObjectArrays
import it.unimi.dsi.fastutil.longs.Long2LongArrayMap
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet

/**
 * @author Rohit
 */
class NewNodeExact(nodeId: Long, summ: Array[HashMap[Long,Long]]) extends Serializable {
  val node: Long = nodeId
  val currentsuperstep = 0
  val updateCount = 0
  val summary: Array[HashMap[Long,Long]] = summ
  val ischanged = false
  def getsummary(window: Long): Int = {

    var sum: HashSet[Long] =new HashSet
    for (i <- 0 until summary.length) {

      val x = summary(i)
      if (x != null) {
        val iterator = x.keySet().iterator()
        while (iterator.hasNext()) {
          val value = iterator.nextLong()
          if (x.get(value) > window) {
            sum.add(value)

          }
        }
      }
    }

    return sum.size
  }
}