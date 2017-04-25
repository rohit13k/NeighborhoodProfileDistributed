package com.ulb.code.wit.util

import org.apache.spark.util.AccumulatorV2
import java.{ lang => jl }
class MyAccumulator extends AccumulatorV2[jl.Long, jl.Long] {
  private var _sum = 0L
  private var _count = 0L
  private var _max = 0l
  def count: Long = _count

  def sum: Long = _sum
  def max: Long = _max
  override def merge(other: AccumulatorV2[jl.Long, jl.Long]): Unit = other match {
    case o: MyAccumulator =>
      _sum += o.sum
      _count += o.count
      if (o.max > _max) _max = o.max

    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }
  override def add(v: jl.Long): Unit = {
    _sum += v
    _count += 1
    _max = _sum
  }
  def add(v: Long): Unit = {
    _sum += v
    _count += 1
    _max = _sum
  }
  override def isZero: Boolean = _sum == 0L && _count == 0
  override def copy(): MyAccumulator = {
    val newAcc = new MyAccumulator
    newAcc._count = this._count
    newAcc._sum = this._sum
    newAcc._max = this._max
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0L
    _count = 0L
    _max = 0l
  }
  override def value: jl.Long = _sum
}