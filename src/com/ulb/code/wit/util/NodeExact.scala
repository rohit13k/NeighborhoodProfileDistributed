package com.ulb.code.wit.util

import it.unimi.dsi.fastutil.objects.ObjectArrays
import it.unimi.dsi.fastutil.longs.Long2LongArrayMap

import it.unimi.dsi.fastutil.longs.LongOpenHashSet

/**
 * @author Rohit
 */
class NodeExact(nodeId: Long, summ: Array[Long2LongArrayMap]) extends Serializable {
  var node: Long = nodeId
  var currentsuperstep = 0

  var summary: Array[Long2LongArrayMap] = summ
  var ischanged = false
  def getsummary(window: Long): Int = {

    var sum: LongOpenHashSet = new LongOpenHashSet()
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