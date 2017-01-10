package com.ulb.code.wit.util

object LoadStatsCalculator {

  def getLoadRelativeStandardDeviation(data: List[Int]): Double = {
    val mean = getMean(data)
    val variance = getVariance(data, mean)
    val sd = Math.sqrt(variance)
    sd * 100 / mean
  }
  def getMean(data: List[Int]): Double = {
    data.foldRight(0.0)(_ + _) / (data.length + 0.0)
  }
  def getVariance(data: List[Int], mean: Double): Double = {
    data.map(x => (x - mean) * (x - mean)).foldRight(0.0)(_ + _) / (data.length + 0.0)
  }

  def main(args: Array[String]) {
    val data = List(3655,4686,3243,4571,5685)
    val mean = getMean(data)
    println(mean)
    println(Math.sqrt(getVariance(data, mean)))
    println(getLoadRelativeStandardDeviation(data))
  }
}