package com.ulb.code.wit.util

object Wait extends Serializable{
    def waitfor(INTERVAL:Long = 100) {
     
    val start = System.nanoTime() + INTERVAL
    var end = 0l
    do {
      end = System.nanoTime()
    } while (start >= end)

    
  }
}