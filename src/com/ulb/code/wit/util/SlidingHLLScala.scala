package com.ulb.code.wit.util

/**
 * @author Rohit
 */
import com.ulb.code.wit.util.HyperLogLogUtil.computeHash;
import java.util.Date
import scala.util.control.Breaks._
class SlidingHLLScala(numOfBucket: Int) extends Serializable {

  private var buckets: Array[ElementListScala] = null

  var nonZeroBucket = 0

  // The current sum of 1 / (1L << buckets[i]). Updated as new items are added
  // and used for
  // estimation
  private var currentSum = 0.0
  private var isempty = true

  def getBuckets(): Array[ElementListScala] = {
    this.buckets;
  }

  def setBuckets(buckets: Array[ElementListScala]): Unit = {
    this.buckets = buckets;
  }

  def apply() {
    this.buckets = new Array[ElementListScala](numOfBucket)

    initializeBucket(numOfBucket);
  }

  def isEmpty(): Boolean = isempty

  def add(value: Long, timestamp: Long): Int = {
    if (isempty) {
      isempty = false
    }
    var bucketAndHash = BucketAndHash.fromHash(
      computeHash(value), this.buckets.length);
    var bucket = bucketAndHash.getBucket();
    var lowestBitPosition = java.lang.Long.numberOfTrailingZeros(bucketAndHash.getHash()) + 1;
    var preElementList: ElementListScala = null
    if (null == this.buckets(bucket)) {
      preElementList = new ElementListScala
    } else {
      preElementList = buckets(bucket);
    }
    var changed = preElementList.addNewElement(lowestBitPosition, timestamp)
    this.buckets.update(bucket, preElementList)
    if (!changed) {
      -1
    } else {
      bucket
    }

  }

  def merge(bucketno: Int, lowestbitposition: Int, timestamp: Long): Boolean = {
    if (isempty) {
      isempty = false
    }
    var preElementList: ElementListScala = null
    if (null == buckets(bucketno)) {

      preElementList = new ElementListScala
    } else {
      preElementList = buckets(bucketno);

    }
    var ischanged = preElementList.addNewElement(lowestbitposition, timestamp)
    this.buckets.update(bucketno, preElementList);
    ischanged
  }

  def merge(bucketno: Int, el: (Int, Long)): Boolean = {
    if (isempty) {
      isempty = false
    }
    var preElementList: ElementListScala = null
    if (null == this.buckets(bucketno)) {

      preElementList = new ElementListScala
    } else {
      preElementList = this.buckets(bucketno)

    }
    var ischanged = preElementList.addNewElement(el._1, el._2)
    buckets.update(bucketno, preElementList)
    return ischanged
  }

  def add(value: Long): Int = {
    add(value, new Date().getTime());
  }

  def estimate(): Long = {
    calculate(-1);
  }

  def estimate(window: Long): Long = {
    calculate(window);
  }

  def calculate(window: Long): Long = {
    var alpha = computeAlpha(this.buckets.length);
    if (window == -1)
      currentSum = getCurrentSum();
    else
      currentSum = getCurrentSum(window);
    var result: Double = alpha * this.buckets.length * this.buckets.length / currentSum;

    if (result <= 2.5 * this.buckets.length) {
      // adjust for small cardinalities
      var zeroBuckets = this.buckets.length - nonZeroBucket;
      if (zeroBuckets > 0) {
        result = this.buckets.length * java.lang.Math.log(this.buckets.length * 1.0 / zeroBuckets);
      }
    }

    return Math.round(result);
  }

  def getCurrentSum(): Double = {
    // TODO Auto-generated method stub
    var sum = 0.0;

    for (i <- 0 until this.buckets.length - 1) {
      if (this.buckets(i) != null) {
        sum += 1.0 / (1L << buckets(i).getTopElementValue());
        nonZeroBucket = nonZeroBucket + 1
      } else {
        sum += 1.0
      }

    }
    sum
  }

  def getCurrentSum(window: Long): Double = {
    // TODO Auto-generated method stub
    var sum = 0.0
    var value = 0
    for (i <- 0 until this.buckets.length - 1) {
      if (this.buckets(i) != null) {
        value = this.buckets(i).getElementValue(window)
        if (value != -1) {
          sum += 1.0 / (1L << value)

          nonZeroBucket = nonZeroBucket + 1
        } else
          sum += 1.0
      } else {
        sum += 1.0
      }

    }
    return sum;
  }

  def initializeBucket(numberOfBuckets: Int): Unit = {
    for (i <- 0 until numberOfBuckets) {
      this.buckets.update(i, null)
    }

  }

  def union(b: SlidingHLLScala): Unit = {

    var bdata = b.getBuckets();

    for (i <- 0 until this.buckets.length - 1) {
      var blist = b.getBuckets()(i)
      if (blist != null) {

        for (j <- 0 until blist.length - 1) {
          if (!merge(i, blist.getElement(j))) {
            break
          }
        }
      }

    }

  }

  // private int getNonZeroBucket() {
  // int nonZeroBucket = 0;
  // for (int i = 0; i < buckets.size(); i++) {
  // if (null != buckets.get(i)) {
  // nonZeroBucket++;
  //
  // }
  // }
  // return nonZeroBucket;
  // }

  def computeAlpha(numberOfBuckets: Int): Double = numberOfBuckets match {
    case 16 => 0.673
    case 32 => 0.697
    case 64 => 0.709;
    case _  => (0.7213 / (1 + 1.079 / numberOfBuckets));

  }

}