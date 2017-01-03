package com.ulb.code.wit.util

import it.unimi.dsi.fastutil.objects.ObjectArrays
import it.unimi.dsi.fastutil.longs.Long2LongArrayMap
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet

/**
 * @author Rohit
 */
class NewNodeExact(nodeId: Long, summ: Array[HashMap[Long, Long]], css: Int, cnt: Int) extends Serializable {
  def this(nodeId: Long, summ: Array[HashMap[Long, Long]]) {
    this(nodeId, summ, 0, 0)
  }
  val objectid = java.util.UUID.randomUUID.toString
  val node: Long = nodeId
  val currentsuperstep = css
  val updateCount = cnt
  val summary: Array[HashMap[Long, Long]] = summ
  //  val ischanged = changed
  def getsummary(window: Long): Int = {

    var sum: HashSet[Long] = new HashSet
    for (i <- 0 until summary.length) {

      val x = summary(i)
      if (x != null) {
        val iterator = x.keysIterator
        while (iterator.hasNext) {
          val value = iterator.next()
          if (x.get(value).get > window) {
            sum = sum.+(value)

          }
        }
      }
    }

    return sum.size
  }
}