package com.ulb.code.wit.main

import scala.io.Source
import scala.collection.mutable.ArrayBuilder
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import java.util.Random
import java.util.Date
import java.text.SimpleDateFormat

object GenerateData {
  val folder = "D:\\dataset\\"
  val inputfile = "twitter_uselection_mentionincluded_3"
  val outputfile = inputfile + "repeated"

  def main(args: Array[String]) {
    var nodedegree = collection.mutable.Map[Long, collection.mutable.Set[Long]]()
    for (line <- Source.fromFile(folder + inputfile + ".csv").getLines()) {
      val temp = line.split(",")
      val src = temp(0).toLong
      val dst = temp(1).toLong
      //update degree
      var tempdegree = nodedegree.getOrElse(src, collection.mutable.Set[Long]())
      tempdegree.add(dst)
      nodedegree.put(src, tempdegree)
      tempdegree = nodedegree.getOrElse(dst, collection.mutable.Set[Long]())
      tempdegree.add(src)
      nodedegree.put(dst, tempdegree)
    }
    var nodesummary = collection.mutable.Map[Long, Int]()
    var nodepartialdegree = collection.mutable.Map[Long, collection.mutable.Set[Long]]()
    var activityCorrect = 0
    var degreeCorrect = 0
    var activityWrong = 0
    var degreeWrong = 0
    var count = 0
    for (line <- Source.fromFile(folder + inputfile + ".csv").getLines()) {
      val temp = line.split(",")
      val src = temp(0).toLong
      val dst = temp(1).toLong
      count = count + 1
      //update activity
      nodesummary.put(src, nodesummary.getOrElse(src, 0) + 1)
      nodesummary.put(dst, nodesummary.getOrElse(dst, 0) + 1)
      //update degree
      var tempdegree = nodepartialdegree.getOrElse(src, collection.mutable.Set[Long]())
      tempdegree.add(dst)
      nodepartialdegree.put(src, tempdegree)
      tempdegree = nodepartialdegree.getOrElse(dst, collection.mutable.Set[Long]())
      tempdegree.add(src)
      nodepartialdegree.put(dst, tempdegree)
      val srcPartialDegree = nodepartialdegree.get(src).get.size
      val dstPartialDegree = nodepartialdegree.get(dst).get.size
      val srcactivity = nodesummary.get(src).get
      val dstactivity = nodesummary.get(dst).get
      val srcDegree = nodedegree.get(src).get.size
      val dstDegree = nodedegree.get(dst).get.size

      if (srcDegree < dstDegree) {
        if (srcPartialDegree < dstPartialDegree) {
          degreeCorrect = degreeCorrect + 1
        } else {
          degreeWrong = degreeWrong + 1
        }
        if (srcactivity < dstactivity) {
          activityCorrect = activityCorrect + 1
        } else {
          activityWrong = activityWrong + 1
        }
      } else if (srcDegree > dstDegree) {
        if (srcPartialDegree > dstPartialDegree) {
          degreeCorrect = degreeCorrect + 1
        } else {
          degreeWrong = degreeWrong + 1
        }
        if (srcactivity > dstactivity) {
          activityCorrect = activityCorrect + 1
        } else {
          activityWrong = activityWrong + 1
        }
      } else {
        if (srcPartialDegree == dstPartialDegree) {
          degreeCorrect = degreeCorrect + 1
        } else {
          degreeWrong = degreeWrong + 1
        }
        if (srcactivity == dstactivity) {
          activityCorrect = activityCorrect + 1
        } else {
          activityWrong = activityWrong + 1
        }
      }

    }

    println("Activity correct= " + activityCorrect)
    println("Activity wrong= " + activityWrong)
    println("Degree correct= " + degreeCorrect)
    println("Degree wrong= " + degreeWrong)
    println("total= " + count)
  }
  def generate() {
    val f = new File(folder + outputfile + ".csv")
    val bw = new BufferedWriter(new FileWriter(f))
    val data: ArrayBuilder[String] = ArrayBuilder.make()
    var count = 0;
    for (line <- Source.fromFile(folder + inputfile + ".csv").getLines()) {
      data.+=(line)

      bw.write(line + "\n")
      count = count + 1
      if (count % 10 == 0) {
        for (i <- 0 to count % 10) {
          val r = new Random();
          val choice = r.nextInt(data.result().length);
          val repeat = data.result()(choice)
          bw.write(repeat + "\n")
        }

        bw.flush()
      }
      if (count % 10000 == 0) {
        println(count)
      }
    }
    bw.close()
  }
  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", " ").replaceAll("GMT", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss.SSS")
    val date: Date = formatter.parse(updateDate)
    return date
  }
}